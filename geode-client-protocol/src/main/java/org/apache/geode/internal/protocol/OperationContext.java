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

package org.apache.geode.internal.protocol;

import java.util.function.Function;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.security.ResourcePermission;

@Experimental
public abstract class OperationContext<OperationRequest, OperationResponse, ErrorResponse, ProtocolRequest, ProtocolResponse> {
  private final OperationHandler<OperationRequest, OperationResponse, ErrorResponse> operationHandler;
  private final Function<ProtocolRequest, OperationRequest> fromRequest;
  private final Function<OperationResponse, ProtocolResponse> toResponse;
  private final Function<ErrorResponse, ProtocolResponse> toErrorResponse;
  private final ResourcePermission accessPermissionRequired;

  public OperationContext(Function<ProtocolRequest, OperationRequest> fromRequest,
      OperationHandler<OperationRequest, OperationResponse, ErrorResponse> operationHandler,
      Function<OperationResponse, ProtocolResponse> toResponse,
      ResourcePermission permissionRequired) {
    this.operationHandler = operationHandler;
    this.fromRequest = fromRequest;
    this.toResponse = toResponse;
    this.toErrorResponse = this::makeErrorBuilder;
    accessPermissionRequired = permissionRequired;
  }

  protected abstract ProtocolResponse makeErrorBuilder(ErrorResponse errorResponse);

  public OperationHandler<OperationRequest, OperationResponse, ErrorResponse> getOperationHandler() {
    return operationHandler;
  }

  public Function<ProtocolRequest, OperationRequest> getFromRequest() {
    return fromRequest;
  }

  public Function<OperationResponse, ProtocolResponse> getToResponse() {
    return toResponse;
  }

  public Function<ErrorResponse, ProtocolResponse> getToErrorResponse() {
    return toErrorResponse;
  }

  public ResourcePermission getAccessPermissionRequired() {
    return accessPermissionRequired;
  }
}
