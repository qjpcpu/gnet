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
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// halfCloseIdleTimer is application-level state, not part of gnet. The
// generation check makes Stop/Reset safe even if an old time.AfterFunc callback
// has already started on another goroutine.
type halfCloseIdleTimer struct {
	mu         sync.Mutex
	duration   time.Duration
	timer      *time.Timer
	generation uint64
	active     bool
	done       bool
	onExpire   func()
}

func (t *halfCloseIdleTimer) startOrRefresh() {
	if t.duration <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return
	}
	t.active = true
	t.resetLocked()
}

func (t *halfCloseIdleTimer) refresh() {
	if t.duration <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done || !t.active {
		return
	}
	t.resetLocked()
}

func (t *halfCloseIdleTimer) resetLocked() {
	t.generation++
	generation := t.generation
	if t.timer != nil {
		t.timer.Stop()
	}
	t.timer = time.AfterFunc(t.duration, func() {
		t.expire(generation)
	})
}

func (t *halfCloseIdleTimer) expire(generation uint64) {
	t.mu.Lock()
	if t.done || generation != t.generation {
		t.mu.Unlock()
		return
	}
	t.done = true
	onExpire := t.onExpire
	t.mu.Unlock()
	if onExpire != nil {
		onExpire()
	}
}

func (t *halfCloseIdleTimer) finish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done = true
	t.generation++
	if t.timer != nil {
		t.timer.Stop()
	}
}

type exampleProxyPair struct {
	endpoints [2]Conn
	closed    atomic.Int32
	abortOnce sync.Once
	idle      halfCloseIdleTimer
}

func newExampleProxyPair(a, b Conn, timeout time.Duration) *exampleProxyPair {
	pair := &exampleProxyPair{endpoints: [2]Conn{a, b}}
	pair.idle.duration = timeout
	pair.idle.onExpire = func() {
		// Conn.Close is concurrency-safe, so it can be called by the timer
		// goroutine. Closing both endpoints is the hard timeout path.
		pair.abort()
	}
	return pair
}

func (p *exampleProxyPair) abort() {
	p.abortOnce.Do(func() {
		_ = p.endpoints[0].Close()
		_ = p.endpoints[1].Close()
	})
}

type exampleProxySide struct {
	pair *exampleProxyPair
	peer Conn
}

type exampleBidirectionalProxy struct {
	BuiltinEventEngine
}

func (*exampleBidirectionalProxy) OnTraffic(src Conn) Action {
	side, ok := src.Context().(*exampleProxySide)
	if !ok {
		return Close
	}
	if _, err := src.WriteTo(side.peer); err != nil {
		side.pair.abort()
		return Close
	}
	// This is a no-op before the first EOF. Afterwards, traffic in the still-open
	// direction extends the application-level idle timeout.
	side.pair.idle.refresh()
	return None
}

func (*exampleBidirectionalProxy) OnEOF(src Conn) Action {
	side, ok := src.Context().(*exampleProxySide)
	if !ok {
		return Close
	}
	side.pair.idle.startOrRefresh()
	closer, ok := side.peer.(WriteHalfCloser)
	if !ok {
		side.pair.abort()
		return Close
	}
	if err := closer.CloseWrite(func(_ Conn, err error) error {
		if err != nil {
			side.pair.abort()
		}
		return nil
	}); err != nil {
		side.pair.abort()
		return Close
	}
	return None
}

func (*exampleBidirectionalProxy) OnClose(c Conn, err error) Action {
	side, ok := c.Context().(*exampleProxySide)
	if !ok {
		return None
	}
	// io.EOF is the normal convergence of both half-closes. A nil error means
	// the application requested a full close and must still tear down the pair.
	if !errors.Is(err, io.EOF) {
		side.pair.abort()
	}
	if side.pair.closed.Add(1) == 2 {
		side.pair.idle.finish()
	}
	return None
}

// ExampleEOFEventHandler shows the direction-independent part of a stream
// proxy. Pair creation and assigning an exampleProxySide to each Conn.Context
// happen when the outbound connection is established.
func ExampleEOFEventHandler() {
	var handler EventHandler = &exampleBidirectionalProxy{}
	_, supportsHalfClose := handler.(EOFEventHandler)
	_ = supportsHalfClose
}

func TestHalfCloseIdleTimerGeneration(t *testing.T) {
	var expired atomic.Int32
	timer := &halfCloseIdleTimer{
		duration: time.Hour,
		onExpire: func() {
			expired.Add(1)
		},
	}
	timer.startOrRefresh()
	firstGeneration := timer.generation
	timer.refresh()
	secondGeneration := timer.generation

	timer.expire(firstGeneration)
	assert.Zero(t, expired.Load(), "a stale timer generation expired the pair")
	timer.expire(secondGeneration)
	assert.EqualValues(t, 1, expired.Load())
	timer.expire(secondGeneration)
	assert.EqualValues(t, 1, expired.Load(), "the same timer expired more than once")
	timer.finish()
}

func TestHalfCloseIdleTimerCanBeDisabledAndCancelled(t *testing.T) {
	var expired atomic.Int32
	disabled := &halfCloseIdleTimer{onExpire: func() { expired.Add(1) }}
	disabled.startOrRefresh()
	assert.False(t, disabled.active)

	timer := &halfCloseIdleTimer{duration: time.Hour, onExpire: func() { expired.Add(1) }}
	timer.startOrRefresh()
	generation := timer.generation
	timer.finish()
	timer.expire(generation)
	assert.Zero(t, expired.Load())
}
