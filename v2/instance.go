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
	"strings"

	api "github.com/polarismesh/polaris/common/api/v1"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/common/utils"
	apimodel "github.com/polarismesh/specification/source/go/api/v1/model"
	"github.com/polarismesh/specification/source/go/api/v1/service_manage"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/pole-group/nacosserver/core"
	nacosmodel "github.com/pole-group/nacosserver/model"
	nacospb "github.com/pole-group/nacosserver/v2/pb"
)

func (h *NacosV2Server) handleInstanceRequest(ctx context.Context, req nacospb.BaseRequest,
	meta nacospb.RequestMeta) (nacospb.BaseResponse, error) {
	insReq, ok := req.(*nacospb.InstanceRequest)
	if !ok {
		return nil, ErrorInvalidRequestBodyType
	}

	namespace := nacosmodel.DefaultNacosNamespace
	if len(insReq.Namespace) != 0 {
		namespace = insReq.Namespace
	}
	ins := nacosmodel.PrepareSpecInstance(namespace, insReq.ServiceName, &insReq.Instance)
	ins.EnableHealthCheck = wrapperspb.Bool(true)
	ins.HealthCheck = &service_manage.HealthCheck{
		Type: service_manage.HealthCheck_HEARTBEAT,
		Heartbeat: &service_manage.HeartbeatHealthCheck{
			Ttl: wrapperspb.UInt32(1),
		},
	}

	var (
		resp *service_manage.Response
	)

	switch insReq.Type {
	case "registerInstance":
		resp = h.discoverSvr.RegisterInstance(ctx, ins)
		insID := resp.GetInstance().GetId().GetValue()
		h.clientManager.addServiceInstance(meta.ConnectionID, model.ServiceKey{
			Namespace: ins.GetNamespace().GetValue(),
			Name:      ins.GetService().GetValue(),
		}, insID)
	case "deregisterInstance":
		resp = h.discoverSvr.DeregisterInstance(ctx, ins)
		insID := ins.GetId().GetValue()
		h.clientManager.delServiceInstance(meta.ConnectionID, model.ServiceKey{
			Namespace: ins.GetNamespace().GetValue(),
			Name:      ins.GetService().GetValue(),
		}, insID)
	default:
		return nil, &nacosmodel.NacosError{
			ErrCode: int32(nacosmodel.ExceptionCode_InvalidParam),
			ErrMsg:  fmt.Sprintf("Unsupported request type %s", insReq.Type),
		}
	}

	errCode := int(nacosmodel.ErrorCode_Success.Code)
	resultCode := int(nacosmodel.Response_Success.Code)
	success := true

	if resp.GetCode().GetValue() != uint32(apimodel.Code_ExecuteSuccess) {
		success = false
		errCode = int(nacosmodel.ErrorCode_ServerError.Code)
		resultCode = int(nacosmodel.Response_Fail.Code)
	}

	return &nacospb.InstanceResponse{
		Response: &nacospb.Response{
			ResultCode: resultCode,
			ErrorCode:  errCode,
			Success:    success,
			Message:    resp.GetInfo().GetValue(),
		},
	}, nil
}

func (h *NacosV2Server) handleBatchInstanceRequest(ctx context.Context, req nacospb.BaseRequest,
	meta nacospb.RequestMeta) (nacospb.BaseResponse, error) {
	batchInsReq, ok := req.(*nacospb.BatchInstanceRequest)
	if !ok {
		return nil, ErrorInvalidRequestBodyType
	}

	namespace := nacosmodel.DefaultNacosNamespace
	if len(batchInsReq.Namespace) != 0 {
		namespace = batchInsReq.Namespace
	}

	var (
		batchResp *service_manage.BatchWriteResponse = api.NewBatchWriteResponse(apimodel.Code_ExecuteSuccess)
	)

	ctx = context.WithValue(ctx, utils.ContextOpenAsyncRegis, true)
	switch batchInsReq.Type {
	case "batchRegisterInstance":
		for i := range batchInsReq.Instances {
			insReq := batchInsReq.Instances[i]
			ins := nacosmodel.PrepareSpecInstance(namespace, insReq.ServiceName, &insReq)
			ins.EnableHealthCheck = wrapperspb.Bool(true)
			ins.HealthCheck = &service_manage.HealthCheck{
				Type: service_manage.HealthCheck_HEARTBEAT,
				Heartbeat: &service_manage.HeartbeatHealthCheck{
					Ttl: wrapperspb.UInt32(1),
				},
			}
			resp := h.discoverSvr.RegisterInstance(ctx, ins)
			api.Collect(batchResp, resp)
			if resp.GetCode().GetValue() == uint32(apimodel.Code_ExecuteSuccess) {
				insID := resp.GetInstance().GetId().GetValue()
				h.clientManager.addServiceInstance(meta.ConnectionID, model.ServiceKey{
					Namespace: ins.GetNamespace().GetValue(),
					Name:      ins.GetService().GetValue(),
				}, insID)
			}
		}
	default:
		return nil, &nacosmodel.NacosError{
			ErrCode: int32(nacosmodel.ExceptionCode_InvalidParam),
			ErrMsg:  fmt.Sprintf("Unsupported request type %s", batchInsReq.Type),
		}
	}

	errCode := int(nacosmodel.ErrorCode_Success.Code)
	resultCode := int(nacosmodel.Response_Success.Code)
	success := true

	if batchResp.GetCode().GetValue() != uint32(apimodel.Code_ExecuteSuccess) {
		success = false
		errCode = int(nacosmodel.ErrorCode_ServerError.Code)
		resultCode = int(nacosmodel.Response_Fail.Code)
	}

	return &nacospb.BatchInstanceResponse{
		Response: &nacospb.Response{
			ResultCode: resultCode,
			ErrorCode:  errCode,
			Success:    success,
			Message:    batchResp.GetInfo().GetValue(),
		},
	}, nil
}

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

func (h *NacosV2Server) HandleClientConnect(ctx context.Context, client *ConnectionClient) {
	// do nothing
}

func (h *NacosV2Server) HandleClientDisConnect(ctx context.Context, client *ConnectionClient) {
	client.RangePublishInstance(func(svc model.ServiceKey, ids []string) {
		req := make([]*service_manage.Instance, 0, len(ids))
		for i := range ids {
			req = append(req, &service_manage.Instance{
				Id: utils.NewStringValue(ids[i]),
			})
		}
		h.clientManager.delServiceInstance(client.ConnID, model.ServiceKey{
			Namespace: svc.Namespace,
			Name:      svc.Name,
		}, ids...)
		resp := h.originDiscoverSvr.DeleteInstances(ctx, req)
		if resp.GetCode().GetValue() != uint32(apimodel.Code_ExecuteSuccess) {
			// TODO log
		}
	})
}

func defaultString(s, d string) string {
	if len(s) == 0 {
		return d
	}
	return s
}
