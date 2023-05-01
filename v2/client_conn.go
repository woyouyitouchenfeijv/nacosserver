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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/polarismesh/polaris/common/eventhub"
	commontime "github.com/polarismesh/polaris/common/time"
	"google.golang.org/grpc/stats"
)

type (
	EventType int32

	ConnIDKey struct{}

	ConnectionEvent struct {
		EventType EventType
		ConnID    string
		Client    *Client
	}
)

const (
	ClientConnectionEvent = "ClientConnectionEvent"

	_ EventType = iota
	EventClientConnected
	EventClientDisConnected
)

type clientConnHook struct {
	lock        sync.RWMutex
	clients     map[string]*Client // conn_id => Client
	connections map[string]*Client // TCPAddr => Client
}

func newClientConnHook() *clientConnHook {
	return &clientConnHook{
		connections: map[string]*Client{},
		clients:     map[string]*Client{},
	}
}

// TagRPC can attach some information to the given context.
// The context used for the rest lifetime of the RPC will be derived from
// the returned context.
func (h *clientConnHook) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	// do nothing
	return ctx
}

// HandleRPC processes the RPC stats.
func (h *clientConnHook) HandleRPC(ctx context.Context, s stats.RPCStats) {
	// do nothing
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
	h.lock.Lock()
	defer h.lock.Unlock()

	clientAddr := connInfo.RemoteAddr.(*net.TCPAddr)
	client, ok := h.connections[clientAddr.String()]
	if !ok {
		connId := fmt.Sprintf("%d_%s_%d", commontime.CurrentMillisecond(), clientAddr.IP, clientAddr.Port)
		client := &Client{
			ID:          connId,
			Addr:        clientAddr,
			RefreshTime: time.Now(),
		}

		h.clients[connId] = client
		h.connections[clientAddr.String()] = client
	}

	return context.WithValue(ctx, ConnIDKey{}, client.ID)
}

// HandleConn processes the Conn stats.
func (h *clientConnHook) HandleConn(ctx context.Context, s stats.ConnStats) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	switch s.(type) {
	case *stats.ConnBegin:
		connID, _ := ctx.Value(ConnIDKey{}).(string)
		eventhub.Publish(ClientConnectionEvent, ConnectionEvent{
			EventType: EventClientConnected,
			ConnID:    connID,
			Client:    h.clients[connID],
		})
	case *stats.ConnEnd:
		connID, _ := ctx.Value(ConnIDKey{}).(string)
		eventhub.Publish(ClientConnectionEvent, ConnectionEvent{
			EventType: EventClientDisConnected,
			ConnID:    connID,
			Client:    h.clients[connID],
		})
		client, ok := h.clients[connID]
		if ok {
			delete(h.clients, connID)
			delete(h.connections, client.Addr.String())
		}
	}
}

type Client struct {
	ID          string
	Addr        *net.TCPAddr
	RefreshTime time.Time
}
