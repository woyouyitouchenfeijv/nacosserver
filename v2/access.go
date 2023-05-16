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
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/polarismesh/polaris/common/utils"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	nacosmodel "github.com/pole-group/nacosserver/model"
	nacospb "github.com/pole-group/nacosserver/v2/pb"
)

var (
	ErrorNoSuchPayloadType      = errors.New("not such payload type")
	ErrorInvalidRequestBodyType = errors.New("invalid request body type")
)

type (
	RequestHandler func(context.Context, nacospb.BaseRequest, nacospb.RequestMeta) (nacospb.BaseResponse, error)

	RequestHandlerWarrper struct {
		Handler        RequestHandler
		PayloadBuilder func() nacospb.CustomerPayload
	}
)

var (
	debugLevel = map[string]struct{}{
		"HealthCheckRequest": {},
	}
)

func (h *NacosV2Server) Request(ctx context.Context, payload *nacospb.Payload) (*nacospb.Payload, error) {
	h.connectionManager.RefreshClient(ctx)
	handle, val, err := h.UnmarshalPayload(payload)
	if err != nil {
		return nil, err
	}
	msg, ok := val.(nacospb.BaseRequest)
	if !ok {
		return nil, ErrorInvalidRequestBodyType
	}

	if _, ok := debugLevel[msg.GetRequestType()]; !ok {
		nacoslog.Info("[NACOS-V2] handler client request", zap.String("conn-id", ValueConnID(ctx)),
			utils.ZapRequestID(msg.GetRequestId()),
			zap.String("type", msg.GetRequestType()),
		)
	} else {
		if nacoslog.DebugEnabled() {
			nacoslog.Debug("[NACOS-V2] handler client request", zap.String("conn-id", ValueConnID(ctx)),
				utils.ZapRequestID(msg.GetRequestId()),
				zap.String("type", msg.GetRequestType()),
			)
		}
	}
	connMeta := ValueConnMeta(ctx)

	startTime := time.Now()
	resp, err := handle(ctx, msg, nacospb.RequestMeta{
		ConnectionID:  ValueConnID(ctx),
		ClientIP:      payload.GetMetadata().GetClientIp(),
		ClientVersion: connMeta.Version,
		Labels:        connMeta.Labels,
	})
	// 接口调用统计
	diff := time.Since(startTime)
	// 打印耗时超过1s的请求
	if diff > time.Second {
		nacoslog.Info("[NACOS-V2] handler client request", zap.String("conn-id", ValueConnID(ctx)),
			utils.ZapRequestID(msg.GetRequestId()),
			zap.String("type", msg.GetRequestType()),
			zap.Duration("handling-time", diff),
		)
	}

	if err != nil {
		resp = toNacosErrorResp(err)
	}

	resp.SetRequestId(msg.GetRequestId())
	return h.MarshalPayload(resp)
}

func toNacosErrorResp(err error) nacospb.BaseResponse {
	if nacosErr, ok := err.(*nacosmodel.NacosError); ok {
		return &nacospb.ErrorResponse{
			Response: &nacospb.Response{
				ResultCode: int(nacosmodel.Response_Fail.Code),
				ErrorCode:  int(nacosErr.ErrCode),
				Success:    false,
				Message:    nacosErr.ErrMsg,
			},
		}
	} else if nacosErr, ok := err.(*nacosmodel.NacosApiError); ok {
		return &nacospb.ErrorResponse{
			Response: &nacospb.Response{
				ResultCode: int(nacosmodel.Response_Fail.Code),
				ErrorCode:  int(nacosErr.DetailErrCode),
				Success:    false,
				Message:    nacosErr.ErrAbstract,
			},
		}
	}
	return &nacospb.ErrorResponse{
		Response: &nacospb.Response{
			ResultCode: int(nacosmodel.Response_Fail.Code),
			ErrorCode:  int(nacosmodel.ErrorCode_ServerError.Code),
			Success:    false,
			Message:    err.Error(),
		},
	}
}

func (h *NacosV2Server) RequestBiStream(svr nacospb.BiRequestStream_RequestBiStreamServer) error {
	ctx := h.ConvertContext(svr.Context())
	connID := ValueConnID(ctx)
	client, ok := h.connectionManager.GetClient(connID)
	if ok {
		client.Stream = &SyncServerStream{
			stream: svr,
		}
	}

	for {
		req, err := svr.Recv()
		if err != nil {
			if io.EOF == err {
				return nil
			}
			return err
		}

		_, val, err := h.UnmarshalPayload(req)
		if err != nil {
			return err
		}
		switch msg := val.(type) {
		case *nacospb.ConnectionSetupRequest:
			nacoslog.Info("[NACOS-V2] handler client birequest", zap.String("conn-id", ValueConnID(ctx)),
				utils.ZapRequestID(msg.GetRequestId()),
				zap.String("type", msg.GetRequestType()),
			)
			if err := h.connectionManager.RegisterConnection(ctx, req, msg); err != nil {
				return err
			}
		case nacospb.BaseResponse:
			nacoslog.Info("[NACOS-V2] handler client birequest", zap.String("conn-id", ValueConnID(ctx)),
				utils.ZapRequestID(msg.GetRequestId()),
				zap.String("resp-type", msg.GetResponseType()),
			)
			// notify ack msg to callback
			h.inFlights.NotifyInFlight(connID, msg)
			h.connectionManager.RefreshClient(ctx)
		}
	}
}

// UnmarshalPayload .
func (h *NacosV2Server) UnmarshalPayload(payload *nacospb.Payload) (RequestHandler, nacospb.CustomerPayload, error) {
	t := payload.GetMetadata().GetType()
	nacoslog.Debug("[API-Server][NACOS-V2] unmarshal payload info", zap.String("type", t))
	handler, ok := h.handleRegistry[t]
	if !ok {
		return nil, nil, ErrorNoSuchPayloadType
	}
	msg := handler.PayloadBuilder()
	if err := json.Unmarshal(payload.GetBody().GetValue(), msg); err != nil {
		return nil, nil, err
	}
	return handler.Handler, msg, nil
}

// MarshalPayload .
func (h *NacosV2Server) MarshalPayload(resp nacospb.BaseResponse) (*nacospb.Payload, error) {
	data, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	payload := &nacospb.Payload{
		Metadata: &nacospb.Metadata{
			Type: resp.GetResponseType(),
		},
		Body: &anypb.Any{
			Value: data,
		},
	}

	return payload, nil
}
