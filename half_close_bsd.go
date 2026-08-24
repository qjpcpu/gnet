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

//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package gnet

// Non-Linux platforms retain the historical behavior where EOF closes the
// entire connection. These hooks keep the shared Unix event-loop code simple.
func (*eventloop) finishCloseWrite(*conn) error { return nil }
func (*eventloop) failCloseWrite(*conn, error)  {}

func (el *eventloop) handleReadEOF(c *conn, err error) error {
	return el.close(c, err)
}

func (*eventloop) maybeFinalizeHalfClose(*conn) error { return nil }
