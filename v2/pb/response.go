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

package nacos_grpc_service

import (
	"encoding/json"

	"github.com/pole-group/nacosserver/model"
)

type BaseResponse interface {
	GetResponseType() string
	SetRequestId(requestId string)
	GetRequestId() string
	GetBody() string
	GetErrorCode() int
	IsSuccess() bool
	GetResultCode() int
	GetMessage() string
}

type Response struct {
	ResultCode int    `json:"resultCode"`
	ErrorCode  int    `json:"errorCode"`
	Success    bool   `json:"success"`
	Message    string `json:"message"`
	RequestId  string `json:"requestId"`
}

func (r *Response) GetRequestId() string {
	return r.RequestId
}

func (r *Response) SetRequestId(requestId string) {
	r.RequestId = requestId
}

func (r *Response) GetBody() string {
	data, _ := json.Marshal(r)
	return string(data)
}

func (r *Response) IsSuccess() bool {
	return r.Success
}

func (r *Response) GetErrorCode() int {
	return r.ErrorCode
}

func (r *Response) GetResultCode() int {
	return r.ResultCode
}

func (r *Response) GetMessage() string {
	return r.Message
}

type ConnectResetResponse struct {
	*Response
}

func (c *ConnectResetResponse) GetResponseType() string {
	return "ConnectResetResponse"
}

type ClientDetectionResponse struct {
	*Response
}

func (c *ClientDetectionResponse) GetResponseType() string {
	return "ClientDetectionResponse"
}

func NewServerCheckResponse() *ServerCheckResponse {
	return &ServerCheckResponse{
		Response: &Response{
			ResultCode: 0,
			ErrorCode:  0,
			Success:    true,
			Message:    "success",
			RequestId:  "",
		},
		ConnectionId: "",
	}
}

type ServerCheckResponse struct {
	*Response
	ConnectionId string `json:"connectionId"`
}

func (c *ServerCheckResponse) GetResponseType() string {
	return "ServerCheckResponse"
}

type InstanceResponse struct {
	*Response
}

func (c *InstanceResponse) GetResponseType() string {
	return "InstanceResponse"
}

type BatchInstanceResponse struct {
	*Response
}

func (c *BatchInstanceResponse) GetResponseType() string {
	return "BatchInstanceResponse"
}

type QueryServiceResponse struct {
	*Response
	ServiceInfo model.Service `json:"serviceInfo"`
}

func (c *QueryServiceResponse) GetResponseType() string {
	return "QueryServiceResponse"
}

type SubscribeServiceResponse struct {
	*Response
	ServiceInfo model.ServiceInfo `json:"serviceInfo"`
}

func (c *SubscribeServiceResponse) GetResponseType() string {
	return "SubscribeServiceResponse"
}

type ServiceListResponse struct {
	*Response
	Count        int      `json:"count"`
	ServiceNames []string `json:"serviceNames"`
}

func (c *ServiceListResponse) GetResponseType() string {
	return "ServiceListResponse"
}

type NotifySubscriberResponse struct {
	*Response
}

func (c *NotifySubscriberResponse) GetResponseType() string {
	return "NotifySubscriberResponse"
}

type HealthCheckResponse struct {
	*Response
}

func (c *HealthCheckResponse) GetResponseType() string {
	return "HealthCheckResponse"
}

type ErrorResponse struct {
	*Response
}

func (c *ErrorResponse) GetResponseType() string {
	return "ErrorResponse"
}
