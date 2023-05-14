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
	"strings"

	"github.com/polarismesh/polaris/common/utils"

	"github.com/pole-group/nacosserver/core"
	nacosmodel "github.com/pole-group/nacosserver/model"
	nacospb "github.com/pole-group/nacosserver/v2/pb"
)

func (h *NacosV2Server) handleSubscribeServiceReques(ctx context.Context, req nacospb.BaseRequest,
	meta nacospb.RequestMeta) (nacospb.BaseResponse, error) {
	subReq, ok := req.(*nacospb.SubscribeServiceRequest)
	if !ok {
		return nil, ErrorInvalidRequestBodyType
	}
	namespace := subReq.Namespace
	service := subReq.ServiceName
	group := subReq.GroupName

	subscriber := core.Subscriber{
		AddrStr:     meta.ClientIP,
		Agent:       meta.ClientVersion,
		App:         defaultString(req.GetHeaders()["app"], "unknown"),
		Ip:          meta.ClientIP,
		NamespaceId: namespace,
		Group:       group,
		Service:     service,
		Cluster:     subReq.Clusters,
		Type:        core.GRPCPush,
	}

	// 默认只下发 enable 的实例
	result := h.store.ListInstances(ctx, nacosmodel.ServiceKey{
		Namespace: namespace,
		Group:     group,
		Name:      service,
	}, strings.Split(subReq.Clusters, ","), core.SelectInstancesWithHealthyProtection)

	if subReq.Subscribe {
		h.pushCenter.AddSubscriber(subscriber)
	} else {
		h.pushCenter.RemoveSubscriber(subscriber)
	}

	return &nacospb.SubscribeServiceResponse{
		Response: &nacospb.Response{
			ResultCode: int(nacosmodel.Response_Success.Code),
			Message:    "success",
		},
		ServiceInfo: *result,
	}, nil
}

func (h *NacosV2Server) handleNotifySubscriber(ctx context.Context, svr *SyncServerStream,
	data *core.PushData) error {
	req := &nacospb.NotifySubscriberRequest{
		NamingRequest: nacospb.NewNamingRequest(data.ServiceInfo.Namespace,
			data.ServiceInfo.Name, data.ServiceInfo.GroupName),
		ServiceInfo: *data.ServiceInfo,
	}
	req.RequestId = utils.NewUUID()

	// add inflight first
	h.inFlights.AddInFlight(&InFlight{
		ConnID:    ValueConnID(ctx),
		RequestID: req.RequestId,
		Callback: func(resp nacospb.BaseResponse, err error) {
			// TODO push ack response handle
		},
	})

	return svr.SendMsg(req)
}
