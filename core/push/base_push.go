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
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/polarismesh/polaris/common/model"
	"go.uber.org/zap"

	"github.com/pole-group/nacosserver/core"
	nacosmodel "github.com/pole-group/nacosserver/model"
)

type (
	Notifier interface {
		io.Closer
		Notify(d *core.PushData)
		IsZombie() bool
	}

	WatchClient struct {
		subscriber      core.Subscriber
		notifier        Notifier
		lastRefreshTime int64
		lastCheclksum   string
	}
)

type BasePushCenter struct {
	lock sync.RWMutex

	store *core.NacosDataStorage

	clients map[string]*WatchClient
	// notifiers namespace -> service -> notifiers
	notifiers map[string]map[string]map[string]*WatchClient
}

func newBasePushCenter(store *core.NacosDataStorage) *BasePushCenter {
	pc := &BasePushCenter{
		store:     store,
		clients:   map[string]*WatchClient{},
		notifiers: map[string]map[string]map[string]*WatchClient{},
	}
	go pc.cleanZombieClient()
	return pc
}

// PreProcess do preprocess logic for event
func (pc *BasePushCenter) PreProcess(_ context.Context, any any) any {
	return any
}

// OnEvent event process logic
func (pc *BasePushCenter) OnEvent(ctx context.Context, any2 any) error {
	event, ok := any2.(nacosmodel.NacosServicesChangeEvent)
	if !ok {
		return nil
	}
	if log.DebugEnabled() {
		log.Debug("receive nacos services change", zap.Any("event", event))
	}
	for i := range event.Services {
		svc := event.Services[i]
		svcInfo := pc.store.ListInstances(context.Background(), model.ServiceKey{
			Namespace: svc.Namespace,
			Name:      svc.Name,
		}, []string{},
			func(ctx context.Context, svcInfo *nacosmodel.ServiceInfo, ins []*nacosmodel.Instance,
				healthyCount int32) *nacosmodel.ServiceInfo {
				return svcInfo
			})
		pushData := &core.PushData{
			Service: &model.Service{
				ID:        svc.ID,
				Name:      svc.Name,
				Namespace: svc.Namespace,
				Meta:      svc.Meta,
			},
			ServiceInfo: svcInfo,
		}
		core.WarpGRPCPushData(pushData)
		core.WarpUDPPushData(pushData)
		pc.notifyClients(svc.Name, svc.Namespace, func(client *WatchClient) {
			client.notifier.Notify(pushData)
		})
	}
	return nil
}

func (pc *BasePushCenter) getSubscriber(s core.Subscriber) *WatchClient {
	pc.lock.RLock()
	defer pc.lock.RUnlock()

	id := fmt.Sprintf("%s:%d", s.AddrStr, s.Port)
	val := pc.clients[id]
	return val
}

func (pc *BasePushCenter) addSubscriber(s core.Subscriber, notifier Notifier) bool {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	id := fmt.Sprintf("%s:%d", s.AddrStr, s.Port)
	if _, ok := pc.clients[id]; ok {
		return false
	}

	client := &WatchClient{
		subscriber: s,
		notifier:   notifier,
	}
	pc.clients[id] = client

	if _, ok := pc.notifiers[s.NamespaceId]; !ok {
		pc.notifiers[s.NamespaceId] = map[string]map[string]*WatchClient{}
	}
	if _, ok := pc.notifiers[s.NamespaceId][s.ServiceName]; !ok {
		pc.notifiers[s.NamespaceId][s.ServiceName] = map[string]*WatchClient{}
	}
	_, ok := pc.notifiers[s.NamespaceId][s.ServiceName][id]
	if !ok {
		pc.notifiers[s.NamespaceId][s.ServiceName][id] = client
	}
	return true
}

func (pc *BasePushCenter) removeSubscriber(s core.Subscriber) {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	pc.removeSubscriber0(s)
}

func (pc *BasePushCenter) removeSubscriber0(s core.Subscriber) {
	id := fmt.Sprintf("%s:%d", s.AddrStr, s.Port)
	if _, ok := pc.clients[id]; !ok {
		return
	}

	if _, ok := pc.notifiers[s.NamespaceId]; ok {
		if _, ok = pc.notifiers[s.NamespaceId][s.ServiceName]; ok {
			if _, ok = pc.notifiers[s.NamespaceId][s.ServiceName][id]; ok {
				notifiers := pc.notifiers[s.NamespaceId][s.ServiceName]
				delete(notifiers, id)
				pc.notifiers[s.NamespaceId][s.ServiceName] = notifiers
			}
		}
	}

	if notifier, ok := pc.clients[id]; ok {
		_ = notifier.notifier.Close()
	}
	delete(pc.clients, id)
}

func (pc *BasePushCenter) notifyClients(svcName, namespace string, notify func(client *WatchClient)) {
	pc.lock.RLock()
	defer pc.lock.RUnlock()

	ns, ok := pc.notifiers[namespace]
	if !ok {
		return
	}
	clients, ok := ns[svcName]
	if !ok {
		return
	}

	for i := range clients {
		notify(clients[i])
	}
}

func (pc *BasePushCenter) cleanZombieClient() {
	cleanFunc := func() {
		pc.lock.Lock()
		defer pc.lock.Unlock()

		for i := range pc.clients {
			client := pc.clients[i]
			if !client.notifier.IsZombie() {
				continue
			}
			pc.removeSubscriber0(client.subscriber)
		}
	}

	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		cleanFunc()
	}
}
