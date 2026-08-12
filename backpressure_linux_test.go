// Copyright (c) 2026 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gnet

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	errorx "github.com/panjf2000/gnet/v2/pkg/errors"
)

type pauseResumeEvents struct {
	BuiltinEventEngine

	ready    chan struct{}
	paused   chan Conn
	want     int64
	received atomic.Int64
	once     sync.Once
}

type pausedCloseEvents struct {
	BuiltinEventEngine

	ready  chan string
	paused chan struct{}
	closed chan error
}

type pausedHalfCloseEvents struct {
	BuiltinEventEngine

	ready          chan string
	paused         chan Conn
	closed         chan error
	outboundBytes  int
	outboundQueued atomic.Bool
	received       atomic.Int64
}

type concurrentReadControlEvents struct {
	BuiltinEventEngine

	ready chan struct{}
	conn  chan Conn
}

func (p *concurrentReadControlEvents) OnBoot(Engine) Action {
	close(p.ready)
	return None
}

func (p *concurrentReadControlEvents) OnOpen(c Conn) ([]byte, Action) {
	p.conn <- c
	return nil, None
}

func (*concurrentReadControlEvents) OnTraffic(c Conn) Action {
	_, _ = c.WriteTo(io.Discard)
	return Shutdown
}

func (p *pausedCloseEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		p.ready <- ln.addr.String()
		break
	}
	return None
}

func (p *pausedCloseEvents) OnOpen(c Conn) ([]byte, Action) {
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

func (p *pausedCloseEvents) OnClose(_ Conn, err error) Action {
	p.closed <- err
	return Shutdown
}

func (p *pausedHalfCloseEvents) OnBoot(eng Engine) Action {
	for _, ln := range eng.eng.listeners {
		p.ready <- ln.addr.String()
		break
	}
	return None
}

func (p *pausedHalfCloseEvents) OnOpen(c Conn) ([]byte, Action) {
	if err := c.(ReadController).PauseRead(func(_ Conn, err error) error {
		if err == nil {
			p.paused <- c
		}
		return nil
	}); err != nil {
		return nil, Close
	}
	if p.outboundBytes > 0 {
		if err := c.SetWriteBuffer(4 << 10); err != nil {
			return nil, Close
		}
		if _, err := c.Write(make([]byte, p.outboundBytes)); err != nil {
			return nil, Close
		}
		p.outboundQueued.Store(c.OutboundBuffered() > 0)
	}
	return nil, None
}

func (p *pausedHalfCloseEvents) OnTraffic(c Conn) Action {
	n, err := c.WriteTo(io.Discard)
	if err != nil {
		return Close
	}
	p.received.Add(n)
	return None
}

func (p *pausedHalfCloseEvents) OnClose(_ Conn, err error) Action {
	p.closed <- err
	return Shutdown
}

func (p *pauseResumeEvents) OnBoot(Engine) Action {
	close(p.ready)
	return None
}

func (p *pauseResumeEvents) OnTraffic(c Conn) Action {
	n, err := c.WriteTo(io.Discard)
	if err != nil {
		return Close
	}
	p.received.Add(n)
	p.once.Do(func() {
		_ = c.(ReadController).PauseRead(nil)
		p.paused <- c
	})
	if p.received.Load() == p.want {
		return Shutdown
	}
	return None
}

func TestPauseResumeRead(t *testing.T) {
	for _, et := range []bool{false, true} {
		t.Run(map[bool]string{false: "LT", true: "ET"}[et], func(t *testing.T) {
			addr := testUnixAddr(t)
			events := &pauseResumeEvents{
				ready:  make(chan struct{}),
				paused: make(chan Conn, 1),
				want:   6,
			}
			done := make(chan error, 1)
			go func() {
				done <- Run(events, "unix://"+addr, WithEdgeTriggeredIO(et))
			}()
			<-events.ready

			client, err := net.Dial("unix", addr)
			require.NoError(t, err)
			defer client.Close() //nolint:errcheck
			_, err = client.Write([]byte("one"))
			require.NoError(t, err)
			conn := <-events.paused
			_, err = client.Write([]byte("two"))
			require.NoError(t, err)

			time.Sleep(50 * time.Millisecond)
			assert.EqualValues(t, 3, events.received.Load())
			resumed := make(chan error, 1)
			require.NoError(t, conn.(ReadController).ResumeRead(func(_ Conn, err error) error {
				resumed <- err
				return nil
			}))
			require.NoError(t, <-resumed)
			require.NoError(t, <-done)
			assert.EqualValues(t, events.want, events.received.Load())
		})
	}
}

func TestPauseReadUnsupportedForUDP(t *testing.T) {
	c := &conn{isDatagram: true}
	assert.ErrorIs(t, c.PauseRead(nil), errorx.ErrUnsupportedOp)
	assert.ErrorIs(t, c.ResumeRead(nil), errorx.ErrUnsupportedOp)
}

func TestPausedConnectionObservesPeerClose(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		for _, et := range []bool{false, true} {
			t.Run(network+map[bool]string{false: "-LT", true: "-ET"}[et], func(t *testing.T) {
				listenAddr := reserveTCPAddr(t)
				if network == "unix" {
					listenAddr = testUnixAddr(t)
				}
				events := &pausedCloseEvents{
					ready:  make(chan string, 1),
					paused: make(chan struct{}),
					closed: make(chan error, 1),
				}
				done := make(chan error, 1)
				go func() {
					done <- Run(events, network+"://"+listenAddr, WithEdgeTriggeredIO(et))
				}()
				addr := <-events.ready
				client, err := net.Dial(network, addr)
				require.NoError(t, err)
				<-events.paused
				if tcp, ok := client.(*net.TCPConn); ok {
					require.NoError(t, tcp.SetLinger(0))
				}
				require.NoError(t, client.Close())
				select {
				case <-events.closed:
				case <-time.After(time.Second):
					t.Fatal("paused connection did not observe peer close")
				}
				require.NoError(t, <-done)
			})
		}
	}
}

func TestPausedConnectionDrainsInputAfterPeerHalfClose(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		for _, et := range []bool{false, true} {
			t.Run(network+map[bool]string{false: "-LT", true: "-ET"}[et], func(t *testing.T) {
				listenAddr := reserveTCPAddr(t)
				if network == "unix" {
					listenAddr = testUnixAddr(t)
				}
				events := &pausedHalfCloseEvents{
					ready:  make(chan string, 1),
					paused: make(chan Conn, 1),
					closed: make(chan error, 1),
				}
				done := make(chan error, 1)
				go func() {
					done <- Run(events, network+"://"+listenAddr, WithEdgeTriggeredIO(et))
				}()
				addr := <-events.ready
				client, err := net.Dial(network, addr)
				require.NoError(t, err)
				defer client.Close() //nolint:errcheck
				serverConn := <-events.paused

				payload := []byte("final request bytes")
				_, err = client.Write(payload)
				require.NoError(t, err)
				require.NoError(t, client.(interface{ CloseWrite() error }).CloseWrite())

				select {
				case <-events.closed:
					t.Fatal("paused connection closed before draining pending input")
				case <-time.After(50 * time.Millisecond):
				}
				assert.Zero(t, events.received.Load())

				resumed := make(chan error, 1)
				require.NoError(t, serverConn.(ReadController).ResumeRead(func(_ Conn, err error) error {
					resumed <- err
					return nil
				}))
				require.NoError(t, <-resumed)
				<-events.closed
				require.NoError(t, <-done)
				assert.EqualValues(t, len(payload), events.received.Load())
			})
		}
	}
}

func TestPausedConnectionWithPendingOutputDrainsInputAfterPeerHalfClose(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		for _, et := range []bool{false, true} {
			t.Run(network+map[bool]string{false: "-LT", true: "-ET"}[et], func(t *testing.T) {
				listenAddr := reserveTCPAddr(t)
				if network == "unix" {
					listenAddr = testUnixAddr(t)
				}
				events := &pausedHalfCloseEvents{
					ready:         make(chan string, 1),
					paused:        make(chan Conn, 1),
					closed:        make(chan error, 1),
					outboundBytes: 4 << 20,
				}
				done := make(chan error, 1)
				go func() {
					done <- Run(events, network+"://"+listenAddr, WithEdgeTriggeredIO(et))
				}()
				addr := <-events.ready
				client, err := net.Dial(network, addr)
				require.NoError(t, err)
				defer client.Close() //nolint:errcheck
				if conn, ok := client.(interface{ SetReadBuffer(int) error }); ok {
					require.NoError(t, conn.SetReadBuffer(4<<10))
				}
				serverConn := <-events.paused
				require.True(t, events.outboundQueued.Load(), "test did not queue outbound data")

				payload := []byte("final request bytes")
				_, err = client.Write(payload)
				require.NoError(t, err)
				require.NoError(t, client.(interface{ CloseWrite() error }).CloseWrite())

				select {
				case <-events.closed:
					t.Fatal("paused connection closed before draining pending input")
				case <-time.After(50 * time.Millisecond):
				}
				assert.Zero(t, events.received.Load())

				resumed := make(chan error, 1)
				require.NoError(t, serverConn.(ReadController).ResumeRead(func(_ Conn, err error) error {
					resumed <- err
					return nil
				}))
				require.NoError(t, <-resumed)
				<-events.closed
				require.NoError(t, <-done)
				assert.EqualValues(t, len(payload), events.received.Load())
			})
		}
	}
}

func TestPausedConnectionDrainsInputAfterPeerClose(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		for _, et := range []bool{false, true} {
			t.Run(network+map[bool]string{false: "-LT", true: "-ET"}[et], func(t *testing.T) {
				listenAddr := reserveTCPAddr(t)
				if network == "unix" {
					listenAddr = testUnixAddr(t)
				}
				events := &pausedHalfCloseEvents{
					ready:  make(chan string, 1),
					paused: make(chan Conn, 1),
					closed: make(chan error, 1),
				}
				done := make(chan error, 1)
				go func() {
					done <- Run(events, network+"://"+listenAddr, WithEdgeTriggeredIO(et))
				}()
				addr := <-events.ready
				client, err := net.Dial(network, addr)
				require.NoError(t, err)
				serverConn := <-events.paused

				payload := []byte("final request bytes")
				_, err = client.Write(payload)
				require.NoError(t, err)
				require.NoError(t, client.Close())

				select {
				case <-events.closed:
					t.Fatal("paused connection closed before draining pending input")
				case <-time.After(50 * time.Millisecond):
				}
				assert.Zero(t, events.received.Load())

				resumed := make(chan error, 1)
				require.NoError(t, serverConn.(ReadController).ResumeRead(func(_ Conn, err error) error {
					resumed <- err
					return nil
				}))
				require.NoError(t, <-resumed)
				<-events.closed
				require.NoError(t, <-done)
				assert.EqualValues(t, len(payload), events.received.Load())
			})
		}
	}
}

func TestConcurrentPauseResumeRead(t *testing.T) {
	addr := testUnixAddr(t)
	events := &concurrentReadControlEvents{
		ready: make(chan struct{}),
		conn:  make(chan Conn, 1),
	}
	done := make(chan error, 1)
	go func() { done <- Run(events, "unix://"+addr, WithEdgeTriggeredIO(true)) }()
	<-events.ready
	client, err := net.Dial("unix", addr)
	require.NoError(t, err)
	defer client.Close() //nolint:errcheck
	controller := (<-events.conn).(ReadController)

	var wg sync.WaitGroup
	errs := make(chan error, 64*32)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 32; j++ {
				if (i+j)%2 == 0 {
					if err := controller.PauseRead(nil); err != nil {
						errs <- err
					}
				} else {
					if err := controller.ResumeRead(nil); err != nil {
						errs <- err
					}
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	resumed := make(chan error, 1)
	require.NoError(t, controller.ResumeRead(func(_ Conn, err error) error {
		resumed <- err
		return nil
	}))
	require.NoError(t, <-resumed)
	_, err = client.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, <-done)
}

type slowProxyEvents struct {
	BuiltinEventEngine

	listener      string
	backend       net.Addr
	ready         chan struct{}
	highWatermark int
	maxBuffered   atomic.Int64
	pauses        atomic.Int64
}

func (p *slowProxyEvents) OnBoot(Engine) Action {
	close(p.ready)
	return None
}

func (p *slowProxyEvents) OnOpen(c Conn) ([]byte, Action) {
	if c.LocalAddr().String() == p.listener {
		if err := c.(ReadController).PauseRead(nil); err != nil {
			return nil, Close
		}
		resCh, err := c.EventLoop().Register(NewContext(context.Background(), c), p.backend)
		if err != nil {
			return nil, Close
		}
		go func() { <-resCh }()
		return nil, None
	}

	client, ok := c.Context().(Conn)
	if !ok {
		return nil, Close
	}
	client.SetContext(c)
	if err := client.(ReadController).ResumeRead(nil); err != nil {
		return nil, Close
	}
	return nil, None
}

func (p *slowProxyEvents) OnTraffic(src Conn) Action {
	dst, ok := src.Context().(Conn)
	if !ok {
		_ = src.(ReadController).PauseRead(nil)
		return None
	}
	if _, err := src.WriteTo(dst); err != nil {
		return Close
	}
	buffered := int64(dst.OutboundBuffered())
	for old := p.maxBuffered.Load(); buffered > old && !p.maxBuffered.CompareAndSwap(old, buffered); old = p.maxBuffered.Load() {
	}
	if buffered >= int64(p.highWatermark) {
		p.pauses.Add(1)
		if err := src.(ReadController).PauseRead(nil); err != nil {
			return Close
		}
	}
	return None
}

func (p *slowProxyEvents) OnWriteBufferEmpty(c Conn) Action {
	if src, ok := c.Context().(Conn); ok {
		_ = src.(ReadController).ResumeRead(nil)
	}
	return None
}

func (p *slowProxyEvents) OnClose(c Conn, _ error) Action {
	if peer, ok := c.Context().(Conn); ok {
		_ = peer.Close()
	}
	if c.LocalAddr() != nil && c.LocalAddr().String() == p.listener {
		return Shutdown
	}
	return None
}

func reserveTCPAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func testStreamProxyBackpressure(t *testing.T, proxyNetwork, backendNetwork string, et bool) {
	proxyAddr := reserveTCPAddr(t)
	if proxyNetwork == "unix" {
		proxyAddr = testUnixAddr(t)
	}
	backendListenAddr := "127.0.0.1:0"
	if backendNetwork == "unix" {
		backendListenAddr = testUnixAddr(t)
	}
	backend, err := net.Listen(backendNetwork, backendListenAddr)
	require.NoError(t, err)
	defer backend.Close() //nolint:errcheck

	payloadSize := 8 << 20
	if proxyNetwork == "tcp" {
		// A successful TCP Write only means the bytes fit in the local kernel
		// send buffer. Exceed autotuned buffers so the assertion below verifies
		// that transport-level backpressure reaches the writer.
		payloadSize = 64 << 20
	}
	releaseBackend := make(chan struct{})
	backendDone := make(chan error, 1)
	go func() {
		conn, err := backend.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close() //nolint:errcheck
		<-releaseBackend
		_, err = io.CopyN(io.Discard, conn, int64(payloadSize))
		backendDone <- err
	}()

	events := &slowProxyEvents{
		listener:      proxyAddr,
		backend:       backend.Addr(),
		ready:         make(chan struct{}),
		highWatermark: MaxStreamBufferCap,
	}
	proxyDone := make(chan error, 1)
	go func() {
		proxyDone <- Run(events, proxyNetwork+"://"+proxyAddr, WithEdgeTriggeredIO(et))
	}()
	<-events.ready

	client, err := net.Dial(proxyNetwork, proxyAddr)
	require.NoError(t, err)
	payload := make([]byte, payloadSize)
	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		writeDone <- err
	}()

	require.Eventually(t, func() bool { return events.pauses.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	assert.LessOrEqual(t, events.maxBuffered.Load(), int64(events.highWatermark+MaxStreamBufferCap))
	select {
	case err = <-writeDone:
		require.NoError(t, err)
		close(releaseBackend)
		t.Fatal("client write completed while the backend was stalled")
	default:
	}
	close(releaseBackend)
	require.NoError(t, <-writeDone)
	require.NoError(t, <-backendDone)
	require.NoError(t, client.Close())
	require.NoError(t, <-proxyDone)
}

func TestStreamProxyBackpressure(t *testing.T) {
	for _, proxyNetwork := range []string{"tcp", "unix"} {
		for _, backendNetwork := range []string{"tcp", "unix"} {
			for _, et := range []bool{false, true} {
				name := proxyNetwork + "-to-" + backendNetwork + map[bool]string{false: "-LT", true: "-ET"}[et]
				t.Run(name, func(t *testing.T) {
					testStreamProxyBackpressure(t, proxyNetwork, backendNetwork, et)
				})
			}
		}
	}
}
