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
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.security.ResourcePermission;

public class ProtobufOperationContext<OperationRequest, OperationResponse> {
  @FunctionalInterface
  public interface PermissionFunction<OperationRequest> {
    ResourcePermission apply(OperationRequest request, ProtobufSerializationService service)
        throws DecodingException;
  }

  private final ProtobufOperationHandler<OperationRequest, OperationResponse> operationHandler;
  private final Function<ClientProtocol.Message, OperationRequest> fromRequest;
  private final Function<OperationResponse, ClientProtocol.Message.Builder> toResponse;
  private final Function<ClientProtocol.ErrorResponse, ClientProtocol.Message.Builder> toErrorResponse;


  public ProtobufOperationContext(Function<ClientProtocol.Message, OperationRequest> fromRequest,
      ProtobufOperationHandler<OperationRequest, OperationResponse> operationHandler,
      Function<OperationResponse, ClientProtocol.Message.Builder> toResponse) {
    this.operationHandler = operationHandler;
    this.fromRequest = fromRequest;
    this.toResponse = toResponse;
    this.toErrorResponse = this::makeErrorBuilder;
  }


  protected ClientProtocol.Message.Builder makeErrorBuilder(
      ClientProtocol.ErrorResponse errorResponse) {
    return ClientProtocol.Message.newBuilder().setErrorResponse(errorResponse);
  }

  public ProtobufOperationHandler<OperationRequest, OperationResponse> getOperationHandler() {
    return operationHandler;
  }

  public Function<ClientProtocol.Message, OperationRequest> getFromRequest() {
    return fromRequest;
  }

  public Function<OperationResponse, ClientProtocol.Message.Builder> getToResponse() {
    return toResponse;
  }

  public Function<ClientProtocol.ErrorResponse, ClientProtocol.Message.Builder> getToErrorResponse() {
    return toErrorResponse;
  }
}
