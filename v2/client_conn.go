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
	"sync/atomic"
	"time"

	"github.com/polarismesh/polaris/common/eventhub"
	"github.com/polarismesh/polaris/common/model"
	commontime "github.com/polarismesh/polaris/common/time"
	"github.com/polarismesh/polaris/common/timewheel"
	"github.com/polarismesh/polaris/common/utils"
	"github.com/polarismesh/polaris/service/healthcheck"
	apimodel "github.com/polarismesh/specification/source/go/api/v1/model"
	"github.com/polarismesh/specification/source/go/api/v1/service_manage"
	"go.uber.org/zap"
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

	// Client
	Client struct {
		ID          string
		Addr        *net.TCPAddr
		RefreshTime atomic.Value
		ConnMeta    ConnectionMeta
		Stream      *SyncServerStream
	}

	// ConnectionEvent
	ConnectionEvent struct {
		EventType EventType
		ConnID    string
		Client    *Client
	}

	// ConnectionMeta
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

	// SyncServerStream
	SyncServerStream struct {
		lock   sync.Mutex
		stream grpc.ServerStream
	}

	// InFlights
	InFlights struct {
		lock      sync.RWMutex
		inFlights map[string]*ClientInFlights
	}

	// ClientInFlights
	ClientInFlights struct {
		lock      sync.RWMutex
		inFlights map[string]*InFlight
	}

	// InFlight
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
		connId := fmt.Sprintf("%d_%s_%d_%s", commontime.CurrentMillisecond(), clientAddr.IP, clientAddr.Port,
			utils.LocalHost)
		client := &Client{
			ID:          connId,
			Addr:        clientAddr,
			RefreshTime: atomic.Value{},
		}
		client.RefreshTime.Store(time.Now())
		h.clients[connId] = client
		h.connections[clientAddr.String()] = client

		nacoslog.Info("[NACOS-V2][ConnMgr] tag new conn", zap.String("conn-id", connId))
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
		nacoslog.Info("[NACOS-V2][ConnMgr] grpc conn begin", zap.String("conn-id", connID))
		eventhub.Publish(ClientConnectionEvent, ConnectionEvent{
			EventType: EventClientConnected,
			ConnID:    connID,
			Client:    h.clients[connID],
		})
	case *stats.ConnEnd:
		connID, _ := ctx.Value(ConnIDKey{}).(string)
		nacoslog.Info("[NACOS-V2][ConnMgr] grpc conn end", zap.String("conn-id", connID))
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
		client.RefreshTime.Store(time.Now())
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
	// ConnectionClientManager
	ConnectionClientManager struct {
		lock        sync.RWMutex
		clients     map[string]*ConnectionClient // ConnID => ConnectionClient
		inteceptors []ClientConnectionInterceptor
		checker     *healthcheck.Server
		wheel       *timewheel.TimeWheel
	}

	// ClientConnectionInterceptor
	ClientConnectionInterceptor interface {
		// HandleClientConnect .
		HandleClientConnect(ctx context.Context, client *ConnectionClient)
		// HandleClientDisConnect .
		HandleClientDisConnect(ctx context.Context, client *ConnectionClient)
	}

	// ConnectionClient .
	ConnectionClient struct {
		// ConnID 物理连接唯一ID标识
		ConnID string
		lock   sync.RWMutex
		// PublishInstances 这个连接上发布的实例信息
		PublishInstances map[model.ServiceKey]map[string]struct{}
		checker          *healthcheck.Server
		wheel            *timewheel.TimeWheel
		destroy          int32
	}
)

func newConnectionClientManager(checker *healthcheck.Server,
	inteceptors []ClientConnectionInterceptor) *ConnectionClientManager {
	return &ConnectionClientManager{
		checker: checker,
		clients: map[string]*ConnectionClient{},
		wheel:   timewheel.New(time.Second, 128, "nacosv2-beat"),
	}
}

// PreProcess do preprocess logic for event
func (cm *ConnectionClientManager) PreProcess(_ context.Context, a any) any {
	return a
}

// OnEvent event process logic
func (c *ConnectionClientManager) OnEvent(ctx context.Context, a any) error {
	event, ok := a.(ConnectionEvent)
	if !ok {
		return nil
	}
	switch event.EventType {
	case EventClientConnected:
		c.addConnectionClientIfAbsent(event.ConnID)
		c.lock.RLock()
		defer c.lock.RUnlock()
		client := c.clients[event.ConnID]
		for i := range c.inteceptors {
			c.inteceptors[i].HandleClientConnect(ctx, client)
		}
	case EventClientDisConnected:
		c.lock.Lock()
		defer c.lock.Unlock()

		client, ok := c.clients[event.ConnID]
		if ok {
			for i := range c.inteceptors {
				c.inteceptors[i].HandleClientDisConnect(ctx, client)
			}
			client.Destroy()
			delete(c.clients, event.ConnID)
		}
	}

	return nil
}

func (c *ConnectionClientManager) addServiceInstance(connID string, svc model.ServiceKey, instanceIDS ...string) {
	c.addConnectionClientIfAbsent(connID)
	c.lock.RLock()
	defer c.lock.RUnlock()
	client := c.clients[connID]
	client.addServiceInstance(svc, instanceIDS...)
}

func (c *ConnectionClientManager) delServiceInstance(connID string, svc model.ServiceKey, instanceIDS ...string) {
	c.addConnectionClientIfAbsent(connID)
	c.lock.RLock()
	defer c.lock.RUnlock()
	client := c.clients[connID]
	client.delServiceInstance(svc, instanceIDS...)
}

func (c *ConnectionClientManager) addConnectionClientIfAbsent(connID string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.clients[connID]; !ok {
		client := &ConnectionClient{
			ConnID:           connID,
			PublishInstances: make(map[model.ServiceKey]map[string]struct{}),
			checker:          c.checker,
			wheel:            c.wheel,
		}
		c.clients[connID] = client
		c.wheel.AddTask(1000, nil, client.renewPublishInstances)
	}
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

// renewPublishInstances 定期上报实例的心跳信息维护实例的健康状态，1s上报一次
func (c *ConnectionClient) renewPublishInstances(_ interface{}) {
	defer func() {
		if c.isDestroy() {
			return
		}
		c.wheel.AddTask(1000, nil, c.renewPublishInstances)
	}()

	if nacoslog.DebugEnabled() {
		nacoslog.Debug("[NACOS-V2] renew publish instance life", zap.String("conn-id", c.ConnID))
	}

	for _, ids := range c.PublishInstances {
		records := make([]*service_manage.InstanceHeartbeat, 0, 32)
		for instanceID := range ids {
			records = append(records, &service_manage.InstanceHeartbeat{
				InstanceId: instanceID,
			})
		}
		resp := c.checker.Reports(context.Background(), records)
		if resp.GetCode().GetValue() != uint32(apimodel.Code_ExecuteSuccess) {
			// TODO print log
		}
	}
}

func (c *ConnectionClient) Destroy() {
	atomic.StoreInt32(&c.destroy, 1)
}

func (c *ConnectionClient) isDestroy() bool {
	return atomic.LoadInt32(&c.destroy) == 1
}
