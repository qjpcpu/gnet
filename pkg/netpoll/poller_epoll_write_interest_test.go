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

//go:build linux

package netpoll

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	errorx "github.com/panjf2000/gnet/v2/pkg/errors"
	"github.com/panjf2000/gnet/v2/pkg/queue"
)

func TestAddWriteDoesNotSubscribeToRDHUP(t *testing.T) {
	for _, et := range []bool{false, true} {
		t.Run(map[bool]string{false: "LT", true: "ET"}[et], func(t *testing.T) {
			fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			defer unix.Close(fds[0]) //nolint:errcheck
			defer unix.Close(fds[1]) //nolint:errcheck

			payload := make([]byte, 64<<10)
			for {
				if _, err = unix.Write(fds[0], payload); err == unix.EAGAIN {
					break
				}
				require.NoError(t, err)
			}
			require.NoError(t, unix.Shutdown(fds[1], unix.SHUT_WR))

			poller, err := OpenPoller()
			require.NoError(t, err)
			defer poller.Close() //nolint:errcheck

			var callbacks atomic.Int64
			callback := func(int, IOEvent, IOFlags) error {
				callbacks.Add(1)
				return nil
			}
			attachment := &PollAttachment{FD: fds[0], Callback: callback}
			require.NoError(t, poller.AddWrite(attachment, et))

			done := make(chan error, 1)
			go func() { done <- pollForWriteInterestTest(poller, callback) }()
			time.AfterFunc(20*time.Millisecond, func() {
				_ = poller.Trigger(queue.HighPriority, func(any) error {
					return errorx.ErrEngineShutdown
				}, nil)
			})
			err = <-done
			assert.True(t, errors.Is(err, errorx.ErrEngineShutdown), "unexpected polling error: %v", err)
			assert.Zero(t, callbacks.Load(), "write-only interest observed an already-consumed RDHUP")
		})
	}
}
