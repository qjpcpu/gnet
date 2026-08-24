/*
 * Copyright (c) 2021 The Gnet Authors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package gnet

import (
	"errors"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/panjf2000/gnet/v2/pkg/netpoll"
)

func (c *conn) processIO(_ int, ev netpoll.IOEvent, _ netpoll.IOFlags) error {
	el := c.loop
	if ev&unix.EPOLLERR != 0 && c.readTerminalErr == nil {
		// SO_ERROR is cleared when queried. Keep it on the connection until every
		// byte queued before the reset has been delivered through OnTraffic.
		c.outboundBuffer.Release()
		c.readTerminalErr = c.pendingSocketError()
	}
	if c.readTerminalErr != nil {
		if c.readEOF {
			// EOF closes only the peer's writing half. A later hard error still
			// terminates the whole connection and must fail any pending CloseWrite.
			// Do not route this through read: it intentionally ignores delivered EOF.
			return el.close(c, c.readTerminalErr)
		}
		if c.readPaused.Load() {
			if !c.hasPendingInput() {
				return el.close(c, c.readTerminalErr)
			}
			return el.suspendPollInterest(c)
		}
		return el.read(c)
	}
	if c.readPaused.Load() {
		if ev&unix.EPOLLHUP != 0 {
			if _, halfClose := el.eventHandler.(EOFEventHandler); !halfClose && !c.hasPendingInput() {
				// Preserve the legacy behavior for handlers that did not opt into
				// half-close: an input-free full close is observable even while paused.
				return el.close(c, io.EOF)
			}
			// EPOLLHUP is reported even when it is not part of the interest mask.
			// Suspend polling until reads resume so that a peer's final bytes and
			// EOF are delivered in order without causing a level-triggered busy loop.
			c.eofPending = true
			return el.suspendPollInterest(c)
		}
		if ev&unix.EPOLLRDHUP != 0 {
			// A stale half-close notification may have been queued before the
			// pause took effect. Remember EOF and process only a writable event.
			c.eofPending = true
			if ev&netpoll.WriteEvents != 0 {
				return el.write(c)
			}
			return nil
		}
	}
	// EPOLLHUP is different from EPOLLERR: a Unix peer that calls CloseWrite
	// followed quickly by Close may report only HUP, so it still goes through the
	// normal drain-and-EOF path.
	if !c.readEOF && ev&(unix.EPOLLRDHUP|unix.EPOLLHUP) != 0 {
		c.eofPending = true
	}
	// Secondly, check for EPOLLOUT before EPOLLIN, the former has a higher priority
	// than the latter regardless of the aliveness of the current connection:
	//
	// 1. When the connection is alive and the system is overloaded, we want to
	// offload the incoming traffic by writing all pending data back to the remotes
	// before continuing to read and handle requests.
	// 2. When the connection is dead, we need to try writing any pending data back
	// to the remote first and then close the connection.
	//
	// We perform eventloop.write for EPOLLOUT because it can take good care of either case.
	if ev&(netpoll.WriteEvents|netpoll.ErrEvents) != 0 {
		if err := el.write(c); err != nil {
			return err
		}
	}
	// Check for EPOLLIN before EPOLLRDHUP in case that there are pending data in
	// the socket buffer.
	if ev&(netpoll.ReadEvents|netpoll.ErrEvents) != 0 {
		if err := el.read(c); err != nil {
			return err
		}
	}
	// Ultimately, check for EPOLLRDHUP, this event indicates that the remote has
	// either closed connection or shut down the writing half of the connection.
	if c.eofPending && c.opened && !c.readEOF {
		// RDHUP/HUP can arrive with or without EPOLLIN. Reading explicitly makes LT
		// and ET modes both continue until zero, after all final bytes have gone
		// through OnTraffic.
		return el.read(c)
	}
	return nil
}

func (c *conn) pendingSocketError() error {
	errno, err := unix.GetsockoptInt(c.fd, unix.SOL_SOCKET, unix.SO_ERROR)
	if err != nil {
		return os.NewSyscallError("getsockopt", err)
	}
	if errno != 0 {
		return os.NewSyscallError("socket", syscall.Errno(errno))
	}
	return errors.New("gnet: socket error")
}

func (c *conn) hasPendingInput() bool {
	if c.InboundBuffered() > 0 {
		return true
	}
	var buf [1]byte
	n, _, err := unix.Recvfrom(c.fd, buf[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
	return n > 0 && err == nil
}
