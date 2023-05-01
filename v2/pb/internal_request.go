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

type ClientAbilities struct {
}

type InternalRequest struct {
	*Request
	Module string `json:"module"`
}

func NewInternalRequest() *InternalRequest {
	request := Request{
		Headers: make(map[string]string, 8),
	}
	return &InternalRequest{
		Request: &request,
		Module:  "internal",
	}
}

type HealthCheckRequest struct {
	*InternalRequest
}

func NewHealthCheckRequest() *HealthCheckRequest {
	return &HealthCheckRequest{
		InternalRequest: NewInternalRequest(),
	}
}

func (r *HealthCheckRequest) GetRequestType() string {
	return "HealthCheckRequest"
}

type ConnectResetRequest struct {
	*InternalRequest
	ServerIp   string
	ServerPort string
}

func (r *ConnectResetRequest) GetRequestType() string {
	return "ConnectResetRequest"
}

type ClientDetectionRequest struct {
	*InternalRequest
}

func (r *ClientDetectionRequest) GetRequestType() string {
	return "ClientDetectionRequest"
}

type ServerCheckRequest struct {
	*InternalRequest
}

func NewServerCheckRequest() *ServerCheckRequest {
	return &ServerCheckRequest{
		InternalRequest: NewInternalRequest(),
	}
}

func (r *ServerCheckRequest) GetRequestType() string {
	return "ServerCheckRequest"
}

type ConnectionSetupRequest struct {
	*InternalRequest
	ClientVersion   string            `json:"clientVersion"`
	Tenant          string            `json:"tenant"`
	Labels          map[string]string `json:"labels"`
	ClientAbilities ClientAbilities   `json:"clientAbilities"`
}

func NewConnectionSetupRequest() *ConnectionSetupRequest {
	return &ConnectionSetupRequest{
		InternalRequest: NewInternalRequest(),
	}
}

func (r *ConnectionSetupRequest) GetRequestType() string {
	return "ConnectionSetupRequest"
}
