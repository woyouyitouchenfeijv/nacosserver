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

package core

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"time"

	"github.com/polarismesh/polaris/common/model"

	nacosmodel "github.com/pole-group/nacosserver/model"
)

type PushType string

const (
	UDPCPush PushType = "udp"
	GRPCPush PushType = "grpc"
)

type PushCenter interface {
	AddSubscriber(s Subscriber)
	RemoveSubscriber(s Subscriber)
	EnablePush(s Subscriber) bool
}

type PushData struct {
	Service          *model.Service
	ServiceInfo      *nacosmodel.ServiceInfo
	UDPData          interface{}
	CompressUDPData  []byte
	GRPCData         interface{}
	CompressGRPCData []byte
}

func WarpGRPCPushData(p *PushData) {
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
	p.GRPCData = data
	body, _ := json.Marshal(data)
	p.CompressGRPCData = CompressIfNecessary(body)
}

func WarpUDPPushData(p *PushData) {
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
	p.UDPData = data
	body, _ := json.Marshal(data)
	p.CompressUDPData = CompressIfNecessary(body)
}

const (
	maxDataSizeUncompress = 1024
)

func CompressIfNecessary(data []byte) []byte {
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

type Subscriber struct {
	AddrStr     string
	Agent       string
	App         string
	Ip          string
	Port        int
	NamespaceId string
	ServiceName string
	Cluster     string
	Type        PushType
}

type CreatePushCenterFunc func(store *NacosDataStorage) (PushCenter, error)

var (
	createPushCenterFunc map[PushType]CreatePushCenterFunc = map[PushType]CreatePushCenterFunc{}
)

func RegisterCreatePushCenterFunc(t PushType, f CreatePushCenterFunc) {
	createPushCenterFunc[t] = f
}

func NewPushCenter(store *NacosDataStorage) (PushCenter, error) {
	var (
		err error
		up  PushCenter = &noopPushCenter{}
		gp  PushCenter = &noopPushCenter{}
	)
	upFunc, ok := createPushCenterFunc[UDPCPush]
	if ok {
		up, err = upFunc(store)
		if err != nil {
			return nil, err
		}
	}
	gpFunc, ok := createPushCenterFunc[GRPCPush]
	if ok {
		gp, err = gpFunc(store)
		if err != nil {
			return nil, err
		}
	}
	return &PushCenterProxy{
		store:    store,
		udpPush:  up,
		grpcPush: gp,
	}, nil
}

type PushCenterProxy struct {
	store    *NacosDataStorage
	udpPush  PushCenter
	grpcPush PushCenter
}

func (p *PushCenterProxy) AddSubscriber(s Subscriber) {
	if !p.EnablePush(s) {
		return
	}
	switch s.Type {
	case UDPCPush:
		p.udpPush.AddSubscriber(s)
	case GRPCPush:
		p.grpcPush.AddSubscriber(s)
	default:
	}
}

func (p *PushCenterProxy) RemoveSubscriber(s Subscriber) {
	if !p.EnablePush(s) {
		return
	}
	switch s.Type {
	case UDPCPush:
		p.udpPush.RemoveSubscriber(s)
	case GRPCPush:
		p.grpcPush.RemoveSubscriber(s)
	default:
	}
}

func (p *PushCenterProxy) EnablePush(s Subscriber) bool {
	switch s.Type {
	case UDPCPush:
		return p.udpPush.EnablePush(s)
	case GRPCPush:
		return p.grpcPush.EnablePush(s)
	default:
		return false
	}
}

type noopPushCenter struct {
}

func (p *noopPushCenter) AddSubscriber(s Subscriber) {
}

func (p *noopPushCenter) RemoveSubscriber(s Subscriber) {
}

func (p *noopPushCenter) EnablePush(s Subscriber) bool {
	return true
}
