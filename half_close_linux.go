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
	"io"
	"net"
	"os"

	"golang.org/x/sys/unix"

	errorx "github.com/panjf2000/gnet/v2/pkg/errors"
	"github.com/panjf2000/gnet/v2/pkg/queue"
)

type closeWriteHook struct {
	callback AsyncCallback
}

// CloseWrite gracefully shuts down the writing half of a Linux stream
// connection. The operation is serialized on the connection's event-loop, so
// all writes accepted before it are sent before SHUT_WR. The callback runs when
// SHUT_WR has completed, not merely when the request has been queued.
func (c *conn) CloseWrite(callback AsyncCallback) error {
	if c.isDatagram {
		return errorx.ErrUnsupportedOp
	}
	return c.loop.poller.Trigger(queue.HighPriority, c.applyCloseWrite, &closeWriteHook{callback: callback})
}

func (c *conn) applyCloseWrite(a any) error {
	hook := a.(*closeWriteHook)
	if !c.opened {
		if hook.callback != nil {
			_ = hook.callback(c, net.ErrClosed)
		}
		return nil
	}
	if c.writeClosed {
		if hook.callback != nil {
			_ = hook.callback(c, nil)
		}
		return nil
	}
	if hook.callback != nil {
		c.writeCloseCBs = append(c.writeCloseCBs, hook.callback)
	}
	if c.writeClosing {
		return nil
	}

	c.writeClosing = true
	if !c.outboundBuffer.IsEmpty() {
		return c.loop.updatePollInterest(c)
	}
	if err := c.loop.finishCloseWrite(c); err != nil || !c.opened {
		return err
	}
	if err := c.loop.updatePollInterest(c); err != nil {
		return err
	}
	return c.loop.maybeFinalizeHalfClose(c)
}

// finishCloseWrite is called only on the owning event-loop. Delaying SHUT_WR
// until the user-space buffer is empty is essential: the kernel can order its
// own send buffer before FIN, but it cannot see bytes still buffered by gnet.
func (el *eventloop) finishCloseWrite(c *conn) error {
	if !c.writeClosing || c.writeClosed || !c.outboundBuffer.IsEmpty() {
		return nil
	}
	if err := unix.Shutdown(c.fd, unix.SHUT_WR); err != nil {
		err = os.NewSyscallError("shutdown", err)
		el.completeCloseWrite(c, err)
		return el.close(c, err)
	}
	c.writeClosing = false
	c.writeClosed = true
	el.completeCloseWrite(c, nil)
	return nil
}

func (el *eventloop) completeCloseWrite(c *conn, err error) {
	callbacks := c.writeCloseCBs
	c.writeCloseCBs = nil
	for _, callback := range callbacks {
		_ = callback(c, err)
	}
}

func (el *eventloop) failCloseWrite(c *conn, err error) {
	if len(c.writeCloseCBs) == 0 {
		return
	}
	if err == nil {
		err = net.ErrClosed
	}
	c.writeClosing = false
	el.completeCloseWrite(c, err)
}

func (el *eventloop) handleReadEOF(c *conn, legacyErr error) error {
	if c.readEOF {
		return nil
	}
	c.eofPending = false

	handler, ok := el.eventHandler.(EOFEventHandler)
	if !ok {
		return el.close(c, legacyErr)
	}

	c.readEOF = true
	switch handler.OnEOF(c) {
	case Close:
		return el.close(c, io.EOF)
	case Shutdown:
		return errorx.ErrEngineShutdown
	}

	// Reconcile interest after OnEOF so a response buffered synchronously by the
	// callback transitions directly to write-only polling instead of DEL+ADD.
	if !c.opened {
		return nil
	}
	if err := el.updatePollInterest(c); err != nil {
		return el.close(c, err)
	}
	return el.maybeFinalizeHalfClose(c)
}

func (el *eventloop) maybeFinalizeHalfClose(c *conn) error {
	if c.opened && c.readEOF && c.writeClosed && c.outboundBuffer.IsEmpty() {
		return el.close(c, io.EOF)
	}
	return nil
}
