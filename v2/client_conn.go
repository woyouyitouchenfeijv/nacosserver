/**
 * Tencent is pleased to support the open source community by making Polaris available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package v2

import (
	"context"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

type cientAddrKey struct{}

type clientConnHook struct {
	lock        sync.RWMutex
	clients     map[string]*Client
	connections map[string]*net.TCPConn
}

func newClientConnHook() *clientConnHook {
	return &clientConnHook{
		connections: make(map[string]*net.TCPConn),
	}
}

func (h *clientConnHook) OnAccept(conn net.Conn) {
}

func (h *clientConnHook) OnRelease(conn net.Conn) {
	clientAddr := conn.RemoteAddr().(*net.TCPAddr)
	h.lock.Lock()
	defer h.lock.Unlock()

	delete(h.connections, clientAddr.String())
}

func (h *clientConnHook) OnClose() {
}

// TagRPC can attach some information to the given context.
// The context used for the rest lifetime of the RPC will be derived from
// the returned context.
func (h *clientConnHook) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

// HandleRPC processes the RPC stats.
func (h *clientConnHook) HandleRPC(ctx context.Context, s stats.RPCStats) {
}

// TagConn can attach some information to the given context.
// The returned context will be used for stats handling.
// For conn stats handling, the context used in HandleConn for this
// connection will be derived from the context returned.
// For RPC stats handling,
//   - On server side, the context used in HandleRPC for all RPCs on this
//
// connection will be derived from the context returned.
//   - On client side, the context is not derived from the context returned.
func (h *clientConnHook) TagConn(ctx context.Context, connInfo *stats.ConnTagInfo) context.Context {
	clientAddr := connInfo.RemoteAddr.(*net.TCPAddr)
	return context.WithValue(ctx, cientAddrKey{}, clientAddr)
}

// HandleConn processes the Conn stats.
func (h *clientConnHook) HandleConn(ctx context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnBegin:
	case *stats.ConnEnd:
	}
}

func (h *clientConnHook) UnaryInterceptor(ctx context.Context, req interface{},
	info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	return handler(ctx, req)
}

func (h *clientConnHook) StreamInterceptor(srv interface{}, ss grpc.ServerStream,
	info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	return handler(srv, ss)
}

type Client struct {
	ID          string
	Addr        *net.TCPAddr
	RefreshTime time.Duration
}
