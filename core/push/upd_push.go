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

package push

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/polaris-contrib/nacosserver/core"
	commontime "github.com/polarismesh/polaris/common/time"
)

type UdpPushCenter struct {
	lock sync.RWMutex

	subscribers map[string]core.Subscriber
	// connectors namespace -> service -> Connectors
	connectors map[string]map[string]map[string]*Connector
}

func (p *UdpPushCenter) AddSubscriber(s core.Subscriber) {
	p.lock.Lock()
	defer p.lock.Unlock()

	id := fmt.Sprintf("%s:%d", s.AddrStr, s.Port)
	if _, ok := p.subscribers[id]; ok {
		return
	}

	p.subscribers[id] = s

	if _, ok := p.connectors[s.NamespaceId]; !ok {
		p.connectors[s.NamespaceId] = map[string]map[string]*Connector{}
	}
	if _, ok := p.connectors[s.NamespaceId][s.ServiceName]; !ok {
		p.connectors[s.NamespaceId][s.ServiceName] = map[string]*Connector{}
	}
	if _, ok := p.connectors[s.NamespaceId][s.ServiceName][id]; !ok {
		conn := newConnector(s)
		p.connectors[s.NamespaceId][s.ServiceName][id] = conn
	}
}

func (p *UdpPushCenter) RemoveSubscriber(s core.Subscriber) {
	p.lock.Lock()
	defer p.lock.Unlock()

	id := fmt.Sprintf("%s:%d", s.AddrStr, s.Port)
	if _, ok := p.subscribers[id]; !ok {
		return
	}

	if _, ok := p.connectors[s.NamespaceId]; ok {
		if _, ok = p.connectors[s.NamespaceId][s.ServiceName]; ok {
			if _, ok = p.connectors[s.NamespaceId][s.ServiceName][id]; ok {
				connections := p.connectors[s.NamespaceId][s.ServiceName]
				delete(connections, id)
				p.connectors[s.NamespaceId][s.ServiceName] = connections
			}
		}
	}

	delete(p.subscribers, id)
}

func (p *UdpPushCenter) enablePush(s core.Subscriber) bool {
	return core.UDPCPush == s.Type
}

func (p *UdpPushCenter) Push(d *core.PushData) {
	namespace := d.Service.Namespace
	service := d.Service.Name

	connectors := func() map[string]*Connector {
		p.lock.RLock()
		defer p.lock.RUnlock()

		if _, ok := p.connectors[namespace]; !ok {
			return nil
		}
		if _, ok := p.connectors[namespace][service]; !ok {
			return nil
		}
		return p.connectors[namespace][service]
	}()

	data := warpPushData(d)
	body, err := json.Marshal(data)
	if err != nil {
		return
	}

	body = compressIfNecessary(body)

	p.lock.Lock()
	defer p.lock.Unlock()

	for id, conn := range connectors {
		if conn.isZombie() {
			// 移除自己
			conn.close()
			delete(p.connectors[namespace][service], id)
			continue
		}
		conn.send(body)
	}
}

func newConnector(s core.Subscriber) *Connector {
	connector := &Connector{
		subscriber: s,
	}

	return connector
}

type Connector struct {
	lock        sync.Mutex
	subscriber  core.Subscriber
	conn        *net.UDPConn
	lastRefTime int64
}

func (c *Connector) doConnect() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.conn != nil {
		return nil
	}

	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{
		IP:   net.IP(c.subscriber.AddrStr),
		Port: c.subscriber.Port,
	})

	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Connector) send(data []byte) {
	if err := c.doConnect(); err != nil {
		return
	}

	if _, err := c.conn.Write(data); err != nil {
		return
	}
}

func (c *Connector) isZombie() bool {
	return commontime.CurrentMillisecond()-c.lastRefTime > 10*1000
}

func (c *Connector) close() {
}

const (
	maxDataSizeUncompress = 1024
)

func compressIfNecessary(data []byte) []byte {
	if len(data) <= maxDataSizeUncompress {
		return data
	}

	var ret bytes.Buffer
	writer := gzip.NewWriter(&ret)
	_, err := writer.Write(data)
	if err != nil {
		return data
	}
	return ret.Bytes()
}

func warpPushData(p *core.PushData) map[string]interface{} {
	data := map[string]interface{}{
		"type": "dom",
		"data": map[string]interface{}{
			"dom":             p.Service.Name,
			"cacheMillis":     p.ServiceInfo.CacheMillis,
			"lastRefTime":     p.ServiceInfo.LastRefTime,
			"checksum":        p.ServiceInfo.Checksum,
			"useSpecifiedURL": false,
			"hosts":           p.ServiceInfo.Hosts,
			"metadata":        p.Service.Meta,
		},
		"lastRefTime": time.Now().Nanosecond(),
	}
	return data
}
