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

func (el *eventloop) updatePollInterest(c *conn) error {
	wantRead := !c.readPaused.Load()
	wantWrite := !c.outboundBuffer.IsEmpty()
	isET := el.engine.opts.EdgeTriggeredIO
	if !c.pollRegistered {
		if !wantRead {
			return nil
		}
		var err error
		if wantWrite {
			err = el.poller.AddReadWrite(&c.pollAttachment, isET)
		} else {
			err = el.poller.AddRead(&c.pollAttachment, isET)
		}
		if err == nil {
			c.pollRegistered = true
		}
		return err
	}

	switch {
	case wantRead && wantWrite:
		return el.poller.ModReadWrite(&c.pollAttachment, isET)
	case wantRead:
		return el.poller.ModRead(&c.pollAttachment, isET)
	case wantWrite:
		return el.poller.ModWrite(&c.pollAttachment, isET)
	default:
		return el.poller.ModReadDisabled(&c.pollAttachment, isET)
	}
}

func (el *eventloop) suspendPollInterest(c *conn) error {
	if !c.pollRegistered {
		return nil
	}
	if err := el.poller.Delete(c.fd); err != nil {
		return err
	}
	c.pollRegistered = false
	return nil
}
