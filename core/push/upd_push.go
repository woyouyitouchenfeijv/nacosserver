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
	"encoding/json"
	"fmt"
	"net"
	"sync"

	commontime "github.com/polarismesh/polaris/common/time"

	"github.com/pole-group/nacosserver/core"
)

func init() {
	core.RegisterCreatePushCenterFunc(core.UDPCPush, NewUDPPushCenter)
}

func NewUDPPushCenter(store *core.NacosDataStorage) (core.PushCenter, error) {
	ln, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 0})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return &UdpPushCenter{
		BasePushCenter: newBasePushCenter(store),
		udpLn:          ln,
		srcAddr:        ln.LocalAddr().(*net.UDPAddr),
	}, nil
}

type UdpPushCenter struct {
	*BasePushCenter
	lock    sync.RWMutex
	udpLn   *net.UDPConn
	srcAddr *net.UDPAddr
}

func (p *UdpPushCenter) AddSubscriber(s core.Subscriber) {
	notifier := newUDPNotifier(s, p.srcAddr)
	if ok := p.addSubscriber(s, notifier); !ok {
		_ = notifier.Close()
		return
	}
	client := p.getSubscriber(s)
	if client != nil {
		client.lastRefreshTime = commontime.CurrentMillisecond()
	}
}

func (p *UdpPushCenter) RemoveSubscriber(s core.Subscriber) {
	p.removeSubscriber(s)
}

func (p *UdpPushCenter) EnablePush(s core.Subscriber) bool {
	return core.UDPCPush == s.Type
}

func newUDPNotifier(s core.Subscriber, srcAddr *net.UDPAddr) *UDPNotifier {
	connector := &UDPNotifier{
		subscriber: s,
		srcAddr:    srcAddr,
	}
	return connector
}

type UDPNotifier struct {
	lock        sync.Mutex
	subscriber  core.Subscriber
	conn        *net.UDPConn
	lastRefTime int64
	srcAddr     *net.UDPAddr
}

func (c *UDPNotifier) doConnect() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.conn != nil {
		return nil
	}

	conn, err := net.DialUDP("udp", c.srcAddr, &net.UDPAddr{
		IP:   net.IP(c.subscriber.AddrStr),
		Port: c.subscriber.Port,
	})
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *UDPNotifier) Notify(d *core.PushData) {
	data := d.CompressUDPData
	if len(data) == 0 {
		body, err := json.Marshal(d.UDPData)
		if err != nil {
			return
		}
		data = body
	}
	if len(data) == 0 {
		return
	}
	c.send(data)
}

func (c *UDPNotifier) send(data []byte) {
	if err := c.doConnect(); err != nil {
		return
	}

	if _, err := c.conn.Write(data); err != nil {
		return
	}
}

func (c *UDPNotifier) IsZombie() bool {
	return commontime.CurrentMillisecond()-c.lastRefTime > 10*1000
}

func (c *UDPNotifier) Close() error {
	return nil
}

type UDPAckPacket struct {
	Type        string
	LastRefTime int64
	Data        string
}
