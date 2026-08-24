// Copyright (c) 2026 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gnet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	errorx "github.com/panjf2000/gnet/v2/pkg/errors"
)

type halfCloseProxyEvents struct {
	BuiltinEventEngine

	listener      string
	backend       net.Addr
	ready         chan struct{}
	paired        chan struct{}
	highWatermark int
	frontPauses   atomic.Int32
	backPauses    atomic.Int32
	frontEOFs     atomic.Int32
	backEOFs      atomic.Int32
	frontTraffic  atomic.Int64
	backTraffic   atomic.Int64
	frontEmpties  atomic.Int32
	backEmpties   atomic.Int32
	closed        atomic.Int32
	errs          chan error
	frontend      Conn
	backendConn   Conn
}

func (p *halfCloseProxyEvents) OnBoot(Engine) Action {
	close(p.ready)
	return None
}

func (p *halfCloseProxyEvents) isFrontend(c Conn) bool {
	return c.LocalAddr() != nil && c.LocalAddr().String() == p.listener
}

func (p *halfCloseProxyEvents) OnOpen(c Conn) ([]byte, Action) {
	if err := c.SetWriteBuffer(4 << 10); err != nil {
		return nil, Close
	}
	if p.isFrontend(c) {
		p.frontend = c
		if err := c.(ReadController).PauseRead(nil); err != nil {
			return nil, Close
		}
		resCh, err := c.EventLoop().Register(NewContext(context.Background(), c), p.backend)
		if err != nil {
			return nil, Close
		}
		go func() {
			if res := <-resCh; res.Err != nil {
				p.errs <- res.Err
			}
		}()
		return nil, None
	}

	frontend, ok := c.Context().(Conn)
	if !ok {
		return nil, Close
	}
	frontend.SetContext(c)
	p.backendConn = c
	if err := frontend.(ReadController).ResumeRead(nil); err != nil {
		return nil, Close
	}
	close(p.paired)
	return nil, None
}

func (p *halfCloseProxyEvents) OnTraffic(src Conn) Action {
	dst, ok := src.Context().(Conn)
	if !ok {
		return Close
	}
	n, err := src.WriteTo(dst)
	if err != nil {
		p.errs <- err
		return Close
	}
	if p.isFrontend(src) {
		p.frontTraffic.Add(n)
	} else {
		p.backTraffic.Add(n)
	}
	if dst.OutboundBuffered() < p.highWatermark {
		return None
	}
	if p.isFrontend(src) {
		p.frontPauses.Add(1)
	} else {
		p.backPauses.Add(1)
	}
	if err := src.(ReadController).PauseRead(nil); err != nil {
		p.errs <- err
		return Close
	}
	return None
}

func (p *halfCloseProxyEvents) OnWriteBufferEmpty(dst Conn) Action {
	if p.isFrontend(dst) {
		p.frontEmpties.Add(1)
	} else {
		p.backEmpties.Add(1)
	}
	if src, ok := dst.Context().(Conn); ok {
		if err := src.(ReadController).ResumeRead(nil); err != nil {
			p.errs <- err
			return Close
		}
	}
	return None
}

type halfCloseConnSnapshot struct {
	opened         bool
	eofPending     bool
	readEOF        bool
	writeClosing   bool
	writeClosed    bool
	readPaused     bool
	pollRegistered bool
	outbound       int
}

func (p *halfCloseProxyEvents) snapshot() [2]halfCloseConnSnapshot {
	result := make(chan [2]halfCloseConnSnapshot, 1)
	_ = p.frontend.EventLoop().Execute(context.Background(), RunnableFunc(func(context.Context) error {
		var snapshots [2]halfCloseConnSnapshot
		for index, connection := range []Conn{p.frontend, p.backendConn} {
			c := connection.(*conn)
			snapshots[index] = halfCloseConnSnapshot{
				opened:         c.opened,
				eofPending:     c.eofPending,
				readEOF:        c.readEOF,
				writeClosing:   c.writeClosing,
				writeClosed:    c.writeClosed,
				readPaused:     c.readPaused.Load(),
				pollRegistered: c.pollRegistered,
				outbound:       c.OutboundBuffered(),
			}
		}
		result <- snapshots
		return nil
	}))
	select {
	case snapshots := <-result:
		return snapshots
	case <-time.After(time.Second):
		return [2]halfCloseConnSnapshot{}
	}
}

func (p *halfCloseProxyEvents) OnEOF(src Conn) Action {
	if p.isFrontend(src) {
		p.frontEOFs.Add(1)
	} else {
		p.backEOFs.Add(1)
	}
	peer, ok := src.Context().(Conn)
	if !ok {
		return Close
	}
	if err := peer.(WriteHalfCloser).CloseWrite(func(_ Conn, err error) error {
		if err != nil {
			p.errs <- err
			_ = src.Close()
		}
		return nil
	}); err != nil {
		p.errs <- err
		return Close
	}
	return None
}

func (p *halfCloseProxyEvents) OnClose(c Conn, err error) Action {
	if err != nil && !errorsIsEOF(err) {
		if peer, ok := c.Context().(Conn); ok {
			_ = peer.Close()
		}
	}
	if p.closed.Add(1) == 2 {
		return Shutdown
	}
	return None
}

func errorsIsEOF(err error) bool {
	return err == io.EOF || errors.Is(err, io.EOF)
}

type halfCloseReplyEvents struct {
	BuiltinEventEngine

	ready       chan string
	opened      chan Conn
	closeWrite  chan error
	writeAfter  chan error
	closed      chan error
	request     bytes.Buffer
	response    []byte
	pauseOnOpen bool
	eofCount    atomic.Int32
}

func (h *halfCloseReplyEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		h.ready <- ln.addr.String()
		break
	}
	return None
}

func (h *halfCloseReplyEvents) OnOpen(c Conn) ([]byte, Action) {
	if h.pauseOnOpen {
		if err := c.(ReadController).PauseRead(func(_ Conn, err error) error {
			if err == nil {
				h.opened <- c
			}
			return nil
		}); err != nil {
			return nil, Close
		}
	} else {
		h.opened <- c
	}
	return nil, None
}

func (h *halfCloseReplyEvents) OnTraffic(c Conn) Action {
	if _, err := c.WriteTo(&h.request); err != nil {
		return Close
	}
	return None
}

func (h *halfCloseReplyEvents) OnEOF(c Conn) Action {
	h.eofCount.Add(1)
	if _, err := c.Write(h.response); err != nil {
		return Close
	}
	if err := c.(WriteHalfCloser).CloseWrite(func(c Conn, err error) error {
		h.closeWrite <- err
		// Once SHUT_WR has completed, every later write must be rejected.
		_, writeErr := c.Write([]byte("late"))
		h.writeAfter <- writeErr
		return nil
	}); err != nil {
		return Close
	}
	return None
}

func (h *halfCloseReplyEvents) OnClose(_ Conn, err error) Action {
	h.closed <- err
	return Shutdown
}

func testHalfCloseReply(t *testing.T, network string, et, paused bool) {
	t.Helper()
	listenAddr := reserveTCPAddr(t)
	if network == "unix" {
		listenAddr = testUnixAddr(t)
	}
	request := []byte("request that is complete only after EOF")
	response := []byte("response written after request EOF")
	events := &halfCloseReplyEvents{
		ready:       make(chan string, 1),
		opened:      make(chan Conn, 1),
		closeWrite:  make(chan error, 2),
		writeAfter:  make(chan error, 1),
		closed:      make(chan error, 1),
		response:    response,
		pauseOnOpen: paused,
	}
	done := make(chan error, 1)
	go func() {
		done <- Run(events, network+"://"+listenAddr, WithEdgeTriggeredIO(et))
	}()

	client, err := net.Dial(network, <-events.ready)
	require.NoError(t, err)
	defer client.Close() //nolint:errcheck
	serverConn := <-events.opened

	_, err = client.Write(request)
	require.NoError(t, err)
	require.NoError(t, client.(interface{ CloseWrite() error }).CloseWrite())

	if paused {
		select {
		case <-events.closeWrite:
			t.Fatal("CloseWrite completed while reading was paused")
		case <-time.After(20 * time.Millisecond):
		}
		assert.Zero(t, events.eofCount.Load())
		assert.Empty(t, events.request.Bytes())
		resumed := make(chan error, 1)
		require.NoError(t, serverConn.(ReadController).ResumeRead(func(_ Conn, err error) error {
			resumed <- err
			return nil
		}))
		require.NoError(t, <-resumed)
	}

	got, err := io.ReadAll(client)
	require.NoError(t, err)
	assert.Equal(t, response, got)
	require.NoError(t, <-events.closeWrite)
	require.ErrorIs(t, <-events.writeAfter, net.ErrClosed)
	require.ErrorIs(t, <-events.closed, io.EOF)
	require.NoError(t, <-done)
	assert.Equal(t, request, events.request.Bytes())
	assert.EqualValues(t, 1, events.eofCount.Load())
}

func TestStreamHalfCloseReplyAfterEOF(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		for _, et := range []bool{false, true} {
			for _, paused := range []bool{false, true} {
				name := network + map[bool]string{false: "-LT", true: "-ET"}[et]
				if paused {
					name += "-paused"
				}
				t.Run(name, func(t *testing.T) {
					testHalfCloseReply(t, network, et, paused)
				})
			}
		}
	}
}

func testBidirectionalProxyHalfClose(t *testing.T, frontendNetwork, backendNetwork string, et bool) {
	t.Helper()
	proxyAddr := reserveTCPAddr(t)
	if frontendNetwork == "unix" {
		proxyAddr = testUnixAddr(t)
	}
	backendAddr := "127.0.0.1:0"
	if backendNetwork == "unix" {
		backendAddr = testUnixAddr(t)
	}
	backend, err := net.Listen(backendNetwork, backendAddr)
	require.NoError(t, err)
	defer backend.Close() //nolint:errcheck

	request := bytes.Repeat([]byte("request-through-paused-proxy"), 32<<10)
	response := bytes.Repeat([]byte("response-through-paused-proxy"), 32<<10)
	releaseBackendRead := make(chan struct{})
	backendRequest := make(chan []byte, 1)
	backendDone := make(chan error, 1)
	go func() {
		conn, err := backend.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close() //nolint:errcheck
		if setter, ok := conn.(interface{ SetWriteBuffer(int) error }); ok {
			if err = setter.SetWriteBuffer(4 << 10); err != nil {
				backendDone <- err
				return
			}
		}
		<-releaseBackendRead
		got, err := io.ReadAll(conn)
		if err != nil {
			backendDone <- err
			return
		}
		backendRequest <- got
		if _, err = io.Copy(conn, bytes.NewReader(response)); err != nil {
			backendDone <- err
			return
		}
		err = conn.(interface{ CloseWrite() error }).CloseWrite()
		backendDone <- err
	}()

	events := &halfCloseProxyEvents{
		listener:      proxyAddr,
		backend:       backend.Addr(),
		ready:         make(chan struct{}),
		paired:        make(chan struct{}),
		highWatermark: 64 << 10,
		errs:          make(chan error, 16),
	}
	proxyDone := make(chan error, 1)
	go func() {
		proxyDone <- Run(events, frontendNetwork+"://"+proxyAddr, WithEdgeTriggeredIO(et))
	}()
	<-events.ready

	client, err := net.Dial(frontendNetwork, proxyAddr)
	require.NoError(t, err)
	defer client.Close() //nolint:errcheck
	require.NoError(t, client.SetReadDeadline(time.Now().Add(30*time.Second)))
	<-events.paired

	requestDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(client, bytes.NewReader(request))
		if err == nil {
			err = client.(interface{ CloseWrite() error }).CloseWrite()
		}
		requestDone <- err
	}()

	require.Eventually(t, func() bool {
		return events.frontPauses.Load() > 0
	}, 5*time.Second, time.Millisecond, "frontend-to-backend direction never applied backpressure")
	close(releaseBackendRead)
	require.NoError(t, <-requestDone)

	// The backend response is deliberately not read by the frontend until the
	// reverse direction has independently crossed the same high watermark.
	require.Eventually(t, func() bool {
		return events.backPauses.Load() > 0
	}, 5*time.Second, time.Millisecond, "backend-to-frontend direction never applied backpressure")
	gotResponse, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("reading proxy response: %v, states=%+v, frontTraffic=%d, backTraffic=%d, frontEOF=%d, backEOF=%d, frontEmpty=%d, backEmpty=%d, response=%d/%d",
			err, events.snapshot(), events.frontTraffic.Load(), events.backTraffic.Load(), events.frontEOFs.Load(),
			events.backEOFs.Load(), events.frontEmpties.Load(), events.backEmpties.Load(), len(gotResponse), len(response))
	}
	assert.Equal(t, response, gotResponse)
	assert.Equal(t, request, <-backendRequest)
	require.NoError(t, <-backendDone)
	require.NoError(t, <-proxyDone)
	assert.EqualValues(t, 1, events.frontEOFs.Load())
	assert.EqualValues(t, 1, events.backEOFs.Load())
	assert.EqualValues(t, 2, events.closed.Load())
	select {
	case err = <-events.errs:
		require.NoError(t, err)
	default:
	}
}

func TestBidirectionalProxyHalfCloseWithBackpressure(t *testing.T) {
	for _, frontendNetwork := range []string{"tcp", "unix"} {
		for _, backendNetwork := range []string{"tcp", "unix"} {
			for _, et := range []bool{false, true} {
				name := frontendNetwork + "-to-" + backendNetwork + map[bool]string{false: "-LT", true: "-ET"}[et]
				t.Run(name, func(t *testing.T) {
					testBidirectionalProxyHalfClose(t, frontendNetwork, backendNetwork, et)
				})
			}
		}
	}
}

type delayedHalfCloseReplyEvents struct {
	BuiltinEventEngine
	ready      chan string
	eof        chan Conn
	registered chan halfCloseConnSnapshot
	closeWrite chan error
	closed     chan error
}

func (d *delayedHalfCloseReplyEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		d.ready <- ln.addr.String()
		break
	}
	return None
}

func (*delayedHalfCloseReplyEvents) OnOpen(c Conn) ([]byte, Action) {
	if err := c.SetWriteBuffer(4 << 10); err != nil {
		return nil, Close
	}
	return nil, None
}

func (*delayedHalfCloseReplyEvents) OnTraffic(c Conn) Action {
	_, _ = c.WriteTo(io.Discard)
	return None
}

func (d *delayedHalfCloseReplyEvents) OnEOF(c Conn) Action {
	d.eof <- c
	return None
}

func (d *delayedHalfCloseReplyEvents) OnClose(_ Conn, err error) Action {
	d.closed <- err
	return Shutdown
}

func TestDelayedResponseReregistersWriteOnlyAfterEOF(t *testing.T) {
	for _, et := range []bool{false, true} {
		t.Run(map[bool]string{false: "LT", true: "ET"}[et], func(t *testing.T) {
			response := bytes.Repeat([]byte("delayed-response-after-eof"), 64<<10)
			events := &delayedHalfCloseReplyEvents{
				ready:      make(chan string, 1),
				eof:        make(chan Conn, 1),
				registered: make(chan halfCloseConnSnapshot, 1),
				closeWrite: make(chan error, 1),
				closed:     make(chan error, 1),
			}
			done := make(chan error, 1)
			addr := reserveTCPAddr(t)
			go func() { done <- Run(events, "tcp://"+addr, WithEdgeTriggeredIO(et)) }()

			client, err := net.Dial("tcp", <-events.ready)
			require.NoError(t, err)
			defer client.Close() //nolint:errcheck
			require.NoError(t, client.(interface{ CloseWrite() error }).CloseWrite())
			serverConn := <-events.eof

			// Execute runs after OnEOF returns and its final interest reconciliation.
			idleState := make(chan halfCloseConnSnapshot, 1)
			require.NoError(t, serverConn.EventLoop().Execute(context.Background(), RunnableFunc(func(context.Context) error {
				c := serverConn.(*conn)
				idleState <- halfCloseConnSnapshot{readEOF: c.readEOF, pollRegistered: c.pollRegistered}
				return nil
			})))
			state := <-idleState
			require.True(t, state.readEOF)
			require.False(t, state.pollRegistered, "idle EOF connection remained in epoll")

			require.NoError(t, serverConn.AsyncWrite(response, func(c Conn, err error) error {
				if err != nil {
					events.registered <- halfCloseConnSnapshot{}
					return err
				}
				gc := c.(*conn)
				events.registered <- halfCloseConnSnapshot{
					pollRegistered: gc.pollRegistered,
					outbound:       gc.OutboundBuffered(),
				}
				return c.(WriteHalfCloser).CloseWrite(func(_ Conn, err error) error {
					events.closeWrite <- err
					return nil
				})
			}))
			state = <-events.registered
			require.True(t, state.pollRegistered, "buffered delayed response was not re-registered")
			require.Positive(t, state.outbound, "test did not create outbound backpressure")

			got, err := io.ReadAll(client)
			require.NoError(t, err)
			require.True(t, bytes.Equal(response, got), "response mismatch: got=%d want=%d", len(got), len(response))
			require.NoError(t, <-events.closeWrite)
			require.ErrorIs(t, <-events.closed, io.EOF)
			require.NoError(t, <-done)
		})
	}
}

type resetAfterEOFEvents struct {
	BuiltinEventEngine
	ready      chan string
	queued     chan responseAfterEOFState
	closeWrite chan error
	closed     chan error
	response   []byte
}

type responseAfterEOFState struct {
	conn     Conn
	buffered int
}

func (r *resetAfterEOFEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		r.ready <- ln.addr.String()
		break
	}
	return None
}

func (*resetAfterEOFEvents) OnOpen(c Conn) ([]byte, Action) {
	if err := c.SetWriteBuffer(4 << 10); err != nil {
		return nil, Close
	}
	return nil, None
}

func (*resetAfterEOFEvents) OnTraffic(c Conn) Action {
	_, _ = c.WriteTo(io.Discard)
	return None
}

func (r *resetAfterEOFEvents) OnEOF(c Conn) Action {
	if _, err := c.Write(r.response); err != nil {
		r.queued <- responseAfterEOFState{}
		return Close
	}
	buffered := c.OutboundBuffered()
	if err := c.(WriteHalfCloser).CloseWrite(func(_ Conn, err error) error {
		r.closeWrite <- err
		return nil
	}); err != nil {
		r.closeWrite <- err
		return Close
	}
	r.queued <- responseAfterEOFState{conn: c, buffered: buffered}
	return None
}

func (r *resetAfterEOFEvents) OnClose(_ Conn, err error) Action {
	r.closed <- err
	return Shutdown
}

func TestResetAfterEOFHardClosesPendingResponse(t *testing.T) {
	for _, et := range []bool{false, true} {
		t.Run(map[bool]string{false: "LT", true: "ET"}[et], func(t *testing.T) {
			events := &resetAfterEOFEvents{
				ready:      make(chan string, 1),
				queued:     make(chan responseAfterEOFState, 1),
				closeWrite: make(chan error, 1),
				closed:     make(chan error, 1),
				response:   bytes.Repeat([]byte("response-buffered-after-eof"), 128<<10),
			}
			done := make(chan error, 1)
			addr := reserveTCPAddr(t)
			go func() { done <- Run(events, "tcp://"+addr, WithEdgeTriggeredIO(et)) }()

			client, err := net.Dial("tcp", <-events.ready)
			require.NoError(t, err)
			defer client.Close() //nolint:errcheck
			tcp := client.(*net.TCPConn)
			require.NoError(t, tcp.SetReadBuffer(4<<10))
			require.NoError(t, tcp.CloseWrite())
			state := <-events.queued
			require.NotNil(t, state.conn)
			require.Positive(t, state.buffered, "test did not leave a response pending after EOF")

			// Deliver the event produced by a reset after graceful EOF while the
			// server is still writing. Injecting EPOLLERR on the event-loop avoids
			// relying on timing-sensitive TCP linger behavior while exercising the
			// same processIO path as the kernel notification.
			processed := make(chan error, 1)
			require.NoError(t, state.conn.EventLoop().Execute(context.Background(), RunnableFunc(func(context.Context) error {
				err := state.conn.(*conn).processIO(0, unix.EPOLLERR, 0)
				processed <- err
				return err
			})))
			require.ErrorIs(t, <-processed, errorx.ErrEngineShutdown)

			select {
			case callbackErr := <-events.closeWrite:
				require.Error(t, callbackErr)
				assert.NotErrorIs(t, callbackErr, io.EOF)
			case <-time.After(time.Second):
				t.Fatal("pending CloseWrite callback was not failed after reset")
			}
			select {
			case closeErr := <-events.closed:
				require.Error(t, closeErr)
				assert.NotErrorIs(t, closeErr, io.EOF)
			case <-time.After(time.Second):
				t.Fatal("OnClose was not called after reset")
			}
			require.NoError(t, <-done)
		})
	}
}

type bufferedCloseWriteEvents struct {
	BuiltinEventEngine

	ready          chan string
	queued         chan int
	closeWriteDone chan error
	closed         chan error
	payload        []byte
	callbacks      atomic.Int32
	writeAfter     chan error
}

func (b *bufferedCloseWriteEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		b.ready <- ln.addr.String()
		break
	}
	return None
}

func (b *bufferedCloseWriteEvents) OnOpen(c Conn) ([]byte, Action) {
	if err := c.SetWriteBuffer(4 << 10); err != nil {
		return nil, Close
	}
	if _, err := c.Write(b.payload); err != nil {
		return nil, Close
	}
	b.queued <- c.OutboundBuffered()
	callback := func(c Conn, err error) error {
		b.callbacks.Add(1)
		b.closeWriteDone <- err
		_, writeErr := c.Write([]byte("must fail"))
		b.writeAfter <- writeErr
		return nil
	}
	if err := c.(WriteHalfCloser).CloseWrite(callback); err != nil {
		return nil, Close
	}
	if err := c.(WriteHalfCloser).CloseWrite(callback); err != nil {
		return nil, Close
	}
	return nil, None
}

func (*bufferedCloseWriteEvents) OnTraffic(c Conn) Action {
	_, _ = c.WriteTo(io.Discard)
	return None
}

func (*bufferedCloseWriteEvents) OnEOF(Conn) Action { return None }

func (b *bufferedCloseWriteEvents) OnClose(_ Conn, err error) Action {
	b.closed <- err
	return Shutdown
}

func TestCloseWriteWaitsForOutboundBuffer(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		for _, et := range []bool{false, true} {
			t.Run(network+map[bool]string{false: "-LT", true: "-ET"}[et], func(t *testing.T) {
				listenAddr := reserveTCPAddr(t)
				if network == "unix" {
					listenAddr = testUnixAddr(t)
				}
				payload := bytes.Repeat([]byte("half-close-order"), 64<<10)
				events := &bufferedCloseWriteEvents{
					ready:          make(chan string, 1),
					queued:         make(chan int, 1),
					closeWriteDone: make(chan error, 2),
					closed:         make(chan error, 1),
					payload:        payload,
					writeAfter:     make(chan error, 2),
				}
				done := make(chan error, 1)
				go func() {
					done <- Run(events, network+"://"+listenAddr, WithEdgeTriggeredIO(et))
				}()

				client, err := net.Dial(network, <-events.ready)
				require.NoError(t, err)
				defer client.Close() //nolint:errcheck
				require.Positive(t, <-events.queued, "test did not queue outbound data")
				select {
				case err = <-events.closeWriteDone:
					require.NoError(t, err)
					t.Fatal("CloseWrite completed before the peer drained outbound data")
				case <-time.After(20 * time.Millisecond):
				}

				got, err := io.ReadAll(client)
				require.NoError(t, err)
				assert.Equal(t, payload, got)
				for i := 0; i < 2; i++ {
					require.NoError(t, <-events.closeWriteDone)
					require.ErrorIs(t, <-events.writeAfter, net.ErrClosed)
				}
				assert.EqualValues(t, 2, events.callbacks.Load())

				require.NoError(t, client.(interface{ CloseWrite() error }).CloseWrite())
				require.ErrorIs(t, <-events.closed, io.EOF)
				require.NoError(t, <-done)
			})
		}
	}
}

func TestCloseWriteUnsupportedForUDP(t *testing.T) {
	c := &conn{isDatagram: true}
	called := false
	err := c.CloseWrite(func(Conn, error) error {
		called = true
		return nil
	})
	assert.ErrorIs(t, err, errorx.ErrUnsupportedOp)
	assert.False(t, called)
}

type interruptedCloseWriteEvents struct {
	BuiltinEventEngine
	ready    chan string
	conn     chan Conn
	callback chan error
	closed   chan error
	payload  []byte
	buffered atomic.Int64
}

func (i *interruptedCloseWriteEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		i.ready <- ln.addr.String()
		break
	}
	return None
}

func (i *interruptedCloseWriteEvents) OnOpen(c Conn) ([]byte, Action) {
	if err := c.SetWriteBuffer(4 << 10); err != nil {
		return nil, Close
	}
	if _, err := c.Write(i.payload); err != nil {
		return nil, Close
	}
	i.buffered.Store(int64(c.OutboundBuffered()))
	if err := c.(WriteHalfCloser).CloseWrite(func(_ Conn, err error) error {
		i.callback <- err
		return nil
	}); err != nil {
		return nil, Close
	}
	i.conn <- c
	return nil, None
}

func (i *interruptedCloseWriteEvents) OnClose(_ Conn, err error) Action {
	i.closed <- err
	return Shutdown
}

func TestPendingCloseWriteCallbackFailsOnHardClose(t *testing.T) {
	addr := testUnixAddr(t)
	events := &interruptedCloseWriteEvents{
		ready:    make(chan string, 1),
		conn:     make(chan Conn, 1),
		callback: make(chan error, 1),
		closed:   make(chan error, 1),
		payload:  bytes.Repeat([]byte("pending-close-write"), 64<<10),
	}
	done := make(chan error, 1)
	go func() { done <- Run(events, "unix://"+addr) }()
	client, err := net.Dial("unix", <-events.ready)
	require.NoError(t, err)
	defer client.Close() //nolint:errcheck
	serverConn := <-events.conn
	require.Positive(t, events.buffered.Load())
	require.NoError(t, serverConn.Close())
	require.ErrorIs(t, <-events.callback, net.ErrClosed)
	require.NoError(t, <-events.closed)
	require.NoError(t, <-done)
}

type pausedRSTEvents struct {
	BuiltinEventEngine
	ready  chan string
	paused chan struct{}
	eof    chan struct{}
	closed chan error
}

type dataThenRSTEvents struct {
	BuiltinEventEngine
	ready    chan string
	release  chan struct{}
	received []byte
	closed   chan error
}

func waitTCPWritesAcknowledged(t *testing.T, tcp *net.TCPConn) {
	t.Helper()
	raw, err := tcp.SyscallConn()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var (
			infoErr      error
			unacked      uint32
			notsentBytes uint32
		)
		if err := raw.Control(func(fd uintptr) {
			var info *unix.TCPInfo
			info, infoErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
			if infoErr == nil {
				unacked = info.Unacked
				notsentBytes = info.Notsent_bytes
			}
		}); err != nil || infoErr != nil {
			return false
		}
		return unacked == 0 && notsentBytes == 0
	}, time.Second, time.Millisecond, "TCP payload was not acknowledged before reset")
}

func (d *dataThenRSTEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		d.ready <- ln.addr.String()
		break
	}
	return None
}

func (d *dataThenRSTEvents) OnOpen(Conn) ([]byte, Action) {
	// Keep the event-loop in OnOpen until the client has queued both data and RST.
	// The next epoll event then contains EPOLLIN|EPOLLERR deterministically.
	<-d.release
	return nil, None
}

func (d *dataThenRSTEvents) OnTraffic(c Conn) Action {
	buf, err := c.Next(-1)
	if err != nil {
		return Close
	}
	d.received = append(d.received, buf...)
	return None
}

func (d *dataThenRSTEvents) OnClose(_ Conn, err error) Action {
	d.closed <- err
	return Shutdown
}

func TestDataIsDrainedBeforeRSTClose(t *testing.T) {
	for _, et := range []bool{false, true} {
		t.Run(map[bool]string{false: "LT", true: "ET"}[et], func(t *testing.T) {
			events := &dataThenRSTEvents{
				ready:   make(chan string, 1),
				release: make(chan struct{}),
				closed:  make(chan error, 1),
			}
			done := make(chan error, 1)
			addr := reserveTCPAddr(t)
			go func() { done <- Run(events, "tcp://"+addr, WithEdgeTriggeredIO(et)) }()

			client, err := net.Dial("tcp", <-events.ready)
			require.NoError(t, err)
			tcp := client.(*net.TCPConn)
			require.NoError(t, tcp.SetWriteDeadline(time.Now().Add(time.Second)))
			payload := bytes.Repeat([]byte("data-before-rst"), 8<<10)
			n, err := io.Copy(tcp, bytes.NewReader(payload))
			require.NoError(t, err)
			require.EqualValues(t, len(payload), n)
			waitTCPWritesAcknowledged(t, tcp)
			require.NoError(t, tcp.SetLinger(0))
			require.NoError(t, tcp.Close())
			close(events.release)

			closeErr := <-events.closed
			assert.NotErrorIs(t, closeErr, io.EOF)
			assert.Equal(t, payload, events.received)
			require.NoError(t, <-done)
		})
	}
}

type pausedDataThenRSTEvents struct {
	BuiltinEventEngine
	ready    chan string
	paused   chan Conn
	received []byte
	closed   chan error
}

func (p *pausedDataThenRSTEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		p.ready <- ln.addr.String()
		break
	}
	return None
}

func (p *pausedDataThenRSTEvents) OnOpen(c Conn) ([]byte, Action) {
	if err := c.(ReadController).PauseRead(func(_ Conn, err error) error {
		if err == nil {
			p.paused <- c
		}
		return nil
	}); err != nil {
		return nil, Close
	}
	return nil, None
}

func (p *pausedDataThenRSTEvents) OnTraffic(c Conn) Action {
	buf, err := c.Next(-1)
	if err != nil {
		return Close
	}
	p.received = append(p.received, buf...)
	return None
}

func (p *pausedDataThenRSTEvents) OnEOF(Conn) Action {
	panic("TCP reset must not be reported as EOF")
}

func (p *pausedDataThenRSTEvents) OnClose(_ Conn, err error) Action {
	p.closed <- err
	return Shutdown
}

func TestPausedReadDrainsDataBeforeRSTClose(t *testing.T) {
	for _, et := range []bool{false, true} {
		t.Run(map[bool]string{false: "LT", true: "ET"}[et], func(t *testing.T) {
			events := &pausedDataThenRSTEvents{
				ready:  make(chan string, 1),
				paused: make(chan Conn, 1),
				closed: make(chan error, 1),
			}
			done := make(chan error, 1)
			addr := reserveTCPAddr(t)
			go func() { done <- Run(events, "tcp://"+addr, WithEdgeTriggeredIO(et)) }()

			client, err := net.Dial("tcp", <-events.ready)
			require.NoError(t, err)
			serverConn := <-events.paused
			tcp := client.(*net.TCPConn)
			require.NoError(t, tcp.SetWriteDeadline(time.Now().Add(time.Second)))
			payload := bytes.Repeat([]byte("paused-data-before-rst"), 4<<10)
			n, err := io.Copy(tcp, bytes.NewReader(payload))
			require.NoError(t, err)
			require.EqualValues(t, len(payload), n)
			waitTCPWritesAcknowledged(t, tcp)
			require.NoError(t, tcp.SetLinger(0))
			require.NoError(t, tcp.Close())

			select {
			case err = <-events.closed:
				t.Fatalf("connection closed before ResumeRead: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			assert.Empty(t, events.received)
			resumed := make(chan error, 1)
			require.NoError(t, serverConn.(ReadController).ResumeRead(func(_ Conn, err error) error {
				resumed <- err
				return nil
			}))
			require.NoError(t, <-resumed)

			closeErr := <-events.closed
			assert.NotErrorIs(t, closeErr, io.EOF)
			require.True(t, bytes.Equal(payload, events.received), "payload mismatch: got=%d want=%d", len(events.received), len(payload))
			require.NoError(t, <-done)
		})
	}
}

type trafficPauseThenRSTEvents struct {
	dataThenRSTEvents
	paused chan Conn
	count  atomic.Int32
}

func (p *trafficPauseThenRSTEvents) OnTraffic(c Conn) Action {
	buf, err := c.Next(-1)
	if err != nil {
		return Close
	}
	p.received = append(p.received, buf...)
	if p.count.Add(1) == 1 {
		if err := c.(ReadController).PauseRead(func(_ Conn, err error) error {
			if err == nil {
				p.paused <- c
			}
			return nil
		}); err != nil {
			return Close
		}
	}
	return None
}

func TestTrafficPauseResumesDrainingBeforeRSTClose(t *testing.T) {
	for _, et := range []bool{false, true} {
		t.Run(map[bool]string{false: "LT", true: "ET"}[et], func(t *testing.T) {
			events := &trafficPauseThenRSTEvents{
				dataThenRSTEvents: dataThenRSTEvents{
					ready:   make(chan string, 1),
					release: make(chan struct{}),
					closed:  make(chan error, 1),
				},
				paused: make(chan Conn, 1),
			}
			done := make(chan error, 1)
			addr := reserveTCPAddr(t)
			go func() { done <- Run(events, "tcp://"+addr, WithEdgeTriggeredIO(et)) }()

			client, err := net.Dial("tcp", <-events.ready)
			require.NoError(t, err)
			tcp := client.(*net.TCPConn)
			require.NoError(t, tcp.SetWriteDeadline(time.Now().Add(time.Second)))
			payload := bytes.Repeat([]byte("pause-during-rst-drain"), 4<<10)
			n, err := io.Copy(tcp, bytes.NewReader(payload))
			require.NoError(t, err)
			require.EqualValues(t, len(payload), n)
			waitTCPWritesAcknowledged(t, tcp)
			require.NoError(t, tcp.SetLinger(0))
			require.NoError(t, tcp.Close())
			close(events.release)

			serverConn := <-events.paused
			require.Less(t, len(events.received), len(payload), "first read unexpectedly drained the entire payload")
			select {
			case err = <-events.closed:
				t.Fatalf("connection closed while traffic was paused: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			resumed := make(chan error, 1)
			require.NoError(t, serverConn.(ReadController).ResumeRead(func(_ Conn, err error) error {
				resumed <- err
				return nil
			}))
			require.NoError(t, <-resumed)

			closeErr := <-events.closed
			assert.NotErrorIs(t, closeErr, io.EOF)
			require.True(t, bytes.Equal(payload, events.received), "payload mismatch: got=%d want=%d", len(events.received), len(payload))
			require.GreaterOrEqual(t, events.count.Load(), int32(2))
			require.NoError(t, <-done)
		})
	}
}

func (p *pausedRSTEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		p.ready <- ln.addr.String()
		break
	}
	return None
}

func (p *pausedRSTEvents) OnOpen(c Conn) ([]byte, Action) {
	if err := c.(ReadController).PauseRead(func(_ Conn, err error) error {
		if err == nil {
			close(p.paused)
		}
		return nil
	}); err != nil {
		return nil, Close
	}
	return nil, None
}

func (p *pausedRSTEvents) OnEOF(Conn) Action {
	close(p.eof)
	return None
}

func (p *pausedRSTEvents) OnClose(_ Conn, err error) Action {
	p.closed <- err
	return Shutdown
}

func TestPausedRSTIsNotReportedAsEOF(t *testing.T) {
	events := &pausedRSTEvents{
		ready:  make(chan string, 1),
		paused: make(chan struct{}),
		eof:    make(chan struct{}),
		closed: make(chan error, 1),
	}
	done := make(chan error, 1)
	addr := reserveTCPAddr(t)
	go func() { done <- Run(events, "tcp://"+addr) }()
	client, err := net.Dial("tcp", <-events.ready)
	require.NoError(t, err)
	<-events.paused
	require.NoError(t, client.(*net.TCPConn).SetLinger(0))
	require.NoError(t, client.Close())
	select {
	case <-events.eof:
		t.Fatal("TCP reset was reported as a graceful EOF")
	case closeErr := <-events.closed:
		assert.NotErrorIs(t, closeErr, io.EOF)
	}
	require.NoError(t, <-done)
}

type legacyEOFEvents struct {
	BuiltinEventEngine
	ready  chan string
	closed chan error
}

func (l *legacyEOFEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		l.ready <- ln.addr.String()
		break
	}
	return None
}

func (l *legacyEOFEvents) OnClose(_ Conn, err error) Action {
	l.closed <- err
	return Shutdown
}

func TestEOFHandlerIsOptIn(t *testing.T) {
	assert.NotImplements(t, (*EOFEventHandler)(nil), &BuiltinEventEngine{})
	events := &legacyEOFEvents{ready: make(chan string, 1), closed: make(chan error, 1)}
	done := make(chan error, 1)
	addr := testUnixAddr(t)
	go func() { done <- Run(events, "unix://"+addr) }()
	client, err := net.Dial("unix", <-events.ready)
	require.NoError(t, err)
	require.NoError(t, client.(interface{ CloseWrite() error }).CloseWrite())
	require.ErrorIs(t, <-events.closed, io.EOF)
	require.NoError(t, client.Close())
	require.NoError(t, <-done)
}
