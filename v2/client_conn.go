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
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/polarismesh/polaris/common/eventhub"
	"github.com/polarismesh/polaris/common/model"
	commontime "github.com/polarismesh/polaris/common/time"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"

	nacosmodel "github.com/pole-group/nacosserver/model"
	nacospb "github.com/pole-group/nacosserver/v2/pb"
)

type (
	EventType int32

	ConnIDKey         struct{}
	ClientIPKey       struct{}
	ConnectionInfoKey struct{}

	Client struct {
		ID          string
		Addr        *net.TCPAddr
		RefreshTime time.Time
		ConnMeta    ConnectionMeta
		Stream      *SyncServerStream
	}

	ConnectionEvent struct {
		EventType EventType
		ConnID    string
		Client    *Client
	}

	ConnectionMeta struct {
		ConnectType      string
		ClientIp         string
		RemoteIp         string
		RemotePort       int
		LocalPort        int
		Version          string
		ConnectionId     string
		CreateTime       time.Time
		AppName          string
		Tenant           string
		Labels           map[string]string
		ClientAttributes nacospb.ClientAbilities
	}

	SyncServerStream struct {
		lock   sync.Mutex
		stream grpc.ServerStream
	}

	InFlights struct {
		lock      sync.RWMutex
		inFlights map[string]*ClientInFlights
	}

	ClientInFlights struct {
		lock      sync.RWMutex
		inFlights map[string]*InFlight
	}

	InFlight struct {
		ConnID    string
		RequestID string
		Callback  func(nacospb.BaseResponse, error)
	}
)

func (i *InFlights) NotifyInFlight(connID string, resp nacospb.BaseResponse) {
	i.lock.RLock()
	clientInflight, ok := i.inFlights[connID]
	i.lock.RUnlock()

	if !ok {
		// TODO log
		return
	}

	clientInflight.lock.Lock()
	defer clientInflight.lock.Unlock()

	inflight, ok := clientInflight.inFlights[resp.GetRequestId()]
	if !ok {
		// TODO log
		return
	}

	if resp.GetResultCode() != int(nacosmodel.Response_Success.Code) {
		inflight.Callback(resp, &nacosmodel.NacosError{
			ErrCode: int32(resp.GetErrorCode()),
			ErrMsg:  resp.GetMessage(),
		})
		return
	}
	inflight.Callback(resp, nil)
}

func (i *InFlights) AddInFlight(inflight *InFlight) error {
	i.lock.Lock()
	connID := inflight.ConnID
	if _, ok := i.inFlights[connID]; !ok {
		i.inFlights[connID] = &ClientInFlights{
			inFlights: map[string]*InFlight{},
		}
	}
	clientInFlights := i.inFlights[connID]
	i.lock.Unlock()

	clientInFlights.lock.Lock()
	defer clientInFlights.lock.Unlock()

	if _, ok := clientInFlights.inFlights[inflight.RequestID]; ok {
		return &nacosmodel.NacosError{
			ErrCode: int32(nacosmodel.ExceptionCode_ClientInvalidParam),
			ErrMsg:  "InFlight request id conflict",
		}
	}

	clientInFlights.inFlights[inflight.RequestID] = inflight
	return nil
}

// Context returns the context for this stream.
func (s *SyncServerStream) Context() context.Context {
	return s.stream.Context()
}

func (s *SyncServerStream) SendMsg(m interface{}) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.stream.SendMsg(m)
}

const (
	ClientConnectionEvent = "ClientConnectionEvent"

	_ EventType = iota
	EventClientConnected
	EventClientDisConnected
)

type ConnectionManager struct {
	lock        sync.RWMutex
	clients     map[string]*Client // conn_id => Client
	connections map[string]*Client // TCPAddr => Client
}

func newConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: map[string]*Client{},
		clients:     map[string]*Client{},
	}
}

func (h *ConnectionManager) RegisterConnection(ctx context.Context, payload *nacospb.Payload,
	req *nacospb.ConnectionSetupRequest) error {

	connID := ValueConnID(ctx)

	connMeta := ConnectionMeta{
		ClientIp:         payload.GetMetadata().GetClientIp(),
		Version:          "",
		ConnectionId:     connID,
		CreateTime:       time.Now(),
		AppName:          "-",
		Tenant:           req.Tenant,
		Labels:           req.Labels,
		ClientAttributes: req.ClientAbilities,
	}
	if val, ok := req.Labels["AppName"]; ok {
		connMeta.AppName = val
	}

	h.lock.Lock()
	defer h.lock.Unlock()

	client, ok := h.clients[connID]
	if !ok {
		return errors.New("Connection register fail, Not Found target client")
	}

	client.ConnMeta = connMeta
	return nil
}

func (h *ConnectionManager) GetClient(id string) (*Client, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	client, ok := h.clients[id]
	return client, ok
}

func (h *ConnectionManager) GetClientByAddr(addr string) (*Client, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	client, ok := h.connections[addr]
	return client, ok
}

// TagRPC can attach some information to the given context.
// The context used for the rest lifetime of the RPC will be derived from
// the returned context.
func (h *ConnectionManager) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	// do nothing
	return ctx
}

// HandleRPC processes the RPC stats.
func (h *ConnectionManager) HandleRPC(ctx context.Context, s stats.RPCStats) {
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
func (h *ConnectionManager) TagConn(ctx context.Context, connInfo *stats.ConnTagInfo) context.Context {
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

	client = h.connections[clientAddr.String()]
	return context.WithValue(ctx, ConnIDKey{}, client.ID)
}

// HandleConn processes the Conn stats.
func (h *ConnectionManager) HandleConn(ctx context.Context, s stats.ConnStats) {
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

func (h *ConnectionManager) RefreshClient(ctx context.Context) {
	connID := ValueConnID(ctx)
	h.lock.RLock()
	defer h.lock.RUnlock()

	client, ok := h.clients[connID]
	if !ok {
		client.RefreshTime = time.Now()
	}
}

func (h *ConnectionManager) GetStream(connID string) *SyncServerStream {
	h.lock.RLock()
	defer h.lock.RUnlock()

	if _, ok := h.clients[connID]; !ok {
		return nil
	}

	client := h.clients[connID]
	return client.Stream
}

type (
	ConnectionClientManager struct {
		lock        sync.RWMutex
		clients     map[string]*ConnectionClient
		inteceptors []ClientConnectionInterceptor
	}

	ClientConnectionInterceptor interface {
		HandleClientConnect(ctx context.Context, client *ConnectionClient)
		HandleClientDisConnect(ctx context.Context, client *ConnectionClient)
	}
)

// PreProcess do preprocess logic for event
func (cm *ConnectionClientManager) PreProcess(_ context.Context, a any) any {
	return a
}

// OnEvent event process logic
func (cm *ConnectionClientManager) OnEvent(ctx context.Context, a any) error {
	event, ok := a.(ConnectionEvent)
	if !ok {
		return nil
	}
	switch event.EventType {
	case EventClientConnected:
		cm.lock.Lock()
		defer cm.lock.Unlock()
		client := &ConnectionClient{
			ConnID:           event.ConnID,
			PublishInstances: map[model.ServiceKey]map[string]struct{}{},
		}
		for i := range cm.inteceptors {
			cm.inteceptors[i].HandleClientConnect(ctx, client)
		}
		cm.clients[event.ConnID] = client
	case EventClientDisConnected:
		cm.lock.Lock()
		defer cm.lock.Unlock()

		client, ok := cm.clients[event.ConnID]
		if ok {
			for i := range cm.inteceptors {
				cm.inteceptors[i].HandleClientDisConnect(ctx, client)
			}
			delete(cm.clients, event.ConnID)
		}
	}

	return nil
}

func (c *ConnectionClientManager) addServiceInstance(connID string, svc model.ServiceKey, instanceIDS ...string) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if _, ok := c.clients[connID]; !ok {
		c.clients[connID] = &ConnectionClient{
			ConnID:           connID,
			PublishInstances: make(map[model.ServiceKey]map[string]struct{}),
		}
	}

	client := c.clients[connID]
	client.addServiceInstance(svc, instanceIDS...)
}

func (c *ConnectionClientManager) delServiceInstance(connID string, svc model.ServiceKey, instanceIDS ...string) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if _, ok := c.clients[connID]; !ok {
		c.clients[connID] = &ConnectionClient{
			ConnID:           connID,
			PublishInstances: make(map[model.ServiceKey]map[string]struct{}),
		}
	}

	client := c.clients[connID]
	client.delServiceInstance(svc, instanceIDS...)
}

type ConnectionClient struct {
	ConnID           string
	lock             sync.RWMutex
	PublishInstances map[model.ServiceKey]map[string]struct{}
}

func (c *ConnectionClient) RangePublishInstance(f func(svc model.ServiceKey, ids []string)) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for svc, ids := range c.PublishInstances {
		ret := make([]string, 0, 16)
		for i := range ids {
			ret = append(ret, i)
		}
		f(svc, ret)
	}
}

func (c *ConnectionClient) addServiceInstance(svc model.ServiceKey, instanceIDS ...string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.PublishInstances[svc]; !ok {
		c.PublishInstances[svc] = map[string]struct{}{}
	}
	publishInfos := c.PublishInstances[svc]

	for i := range instanceIDS {
		publishInfos[instanceIDS[i]] = struct{}{}
	}
	c.PublishInstances[svc] = publishInfos
}

func (c *ConnectionClient) delServiceInstance(svc model.ServiceKey, instanceIDS ...string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.PublishInstances[svc]; !ok {
		c.PublishInstances[svc] = map[string]struct{}{}
	}
	publishInfos := c.PublishInstances[svc]

	for i := range instanceIDS {
		delete(publishInfos, instanceIDS[i])
	}
	c.PublishInstances[svc] = publishInfos
}

// renewPublishInstances 定期上报实例的心跳信息维护实例的健康状态
// TODO 待设计
func (c *ConnectionClient) renewPublishInstances() {

}
