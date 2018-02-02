/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.protocol.protobuf.v1;

import java.util.function.Function;

import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.security.ResourcePermission;

public class ProtobufOperationContext<OperationRequest, OperationResponse> {
  private final ProtobufOperationHandler<OperationRequest, OperationResponse> operationHandler;
  private final Function<ClientProtocol.Request, OperationRequest> fromRequest;
  private final Function<OperationResponse, ClientProtocol.Response.Builder> toResponse;
  private final Function<ClientProtocol.ErrorResponse, ClientProtocol.Response.Builder> toErrorResponse;
  private final ResourcePermission accessPermissionRequired;

  public ProtobufOperationContext(Function<ClientProtocol.Request, OperationRequest> fromRequest,
      ProtobufOperationHandler<OperationRequest, OperationResponse> operationHandler,
      Function<OperationResponse, ClientProtocol.Response.Builder> toResponse,
      ResourcePermission permissionRequired) {
    this.operationHandler = operationHandler;
    this.fromRequest = fromRequest;
    this.toResponse = toResponse;
    this.toErrorResponse = this::makeErrorBuilder;
    accessPermissionRequired = permissionRequired;
  }

  protected ClientProtocol.Response.Builder makeErrorBuilder(
      ClientProtocol.ErrorResponse errorResponse) {
    return ClientProtocol.Response.newBuilder().setErrorResponse(errorResponse);
  }

  public ProtobufOperationHandler<OperationRequest, OperationResponse> getOperationHandler() {
    return operationHandler;
  }

  public Function<ClientProtocol.Request, OperationRequest> getFromRequest() {
    return fromRequest;
  }

  public Function<OperationResponse, ClientProtocol.Response.Builder> getToResponse() {
    return toResponse;
  }

  public Function<ClientProtocol.ErrorResponse, ClientProtocol.Response.Builder> getToErrorResponse() {
    return toErrorResponse;
  }

  public ResourcePermission getAccessPermissionRequired() {
    return accessPermissionRequired;
  }
}
