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
	"net"

	errorx "github.com/panjf2000/gnet/v2/pkg/errors"
	"github.com/panjf2000/gnet/v2/pkg/queue"
)

type readControlHook struct {
	callback AsyncCallback
}

func (c *conn) PauseRead(callback AsyncCallback) error {
	return c.setReadPaused(true, callback)
}

func (c *conn) ResumeRead(callback AsyncCallback) error {
	return c.setReadPaused(false, callback)
}

func (c *conn) setReadPaused(paused bool, callback AsyncCallback) error {
	if c.isDatagram {
		return errorx.ErrUnsupportedOp
	}
	c.readControlMu.Lock()
	defer c.readControlMu.Unlock()
	previous := c.readPaused.Load()
	c.readPaused.Store(paused)
	err := c.loop.poller.Trigger(queue.HighPriority, c.applyReadControl, &readControlHook{
		callback: callback,
	})
	if err != nil {
		c.readPaused.Store(previous)
	}
	return err
}

func (c *conn) applyReadControl(a any) (err error) {
	hook := a.(*readControlHook)
	defer func() {
		if hook.callback != nil {
			_ = hook.callback(c, err)
		}
	}()
	if !c.opened {
		return net.ErrClosed
	}
	if err = c.loop.updatePollInterest(c); err != nil {
		// Invoke the callback before close releases the stream connection.
		if hook.callback != nil {
			_ = hook.callback(c, err)
			hook.callback = nil
		}
		_ = c.loop.close(c, err)
		return err
	}
	if c.readPaused.Load() {
		return nil
	}
	return c.loop.poller.Trigger(queue.LowPriority, c.loop.resumeRead0, c)
}
