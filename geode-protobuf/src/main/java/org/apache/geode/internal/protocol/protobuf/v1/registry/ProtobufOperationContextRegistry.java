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

package org.apache.geode.internal.protocol.protobuf.v1.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Request.RequestAPICase;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufOperationContext;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetAllRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetRegionNamesRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetRegionRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetServerOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.PutAllRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.PutRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.RemoveRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.security.AuthenticationRequestOperationHandler;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class ProtobufOperationContextRegistry {
  private Map<RequestAPICase, ProtobufOperationContext> operationContexts =
      new ConcurrentHashMap<>();

  public ProtobufOperationContextRegistry() {
    addContexts();
  }

  public ProtobufOperationContext getOperationContext(RequestAPICase apiCase) {
    return operationContexts.get(apiCase);
  }

  private void addContexts() {
    operationContexts.put(RequestAPICase.AUTHENTICATIONREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Request::getAuthenticationRequest,
            new AuthenticationRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setAuthenticationResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.READ)));

    operationContexts.put(RequestAPICase.GETREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Request::getGetRequest,
            new GetRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setGetResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.READ)));

    operationContexts.put(RequestAPICase.GETALLREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Request::getGetAllRequest,
            new GetAllRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setGetAllResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.READ)));

    operationContexts.put(RequestAPICase.PUTREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Request::getPutRequest,
            new PutRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setPutResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.WRITE)));

    operationContexts.put(RequestAPICase.PUTALLREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Request::getPutAllRequest,
            new PutAllRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setPutAllResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.WRITE)));

    operationContexts.put(RequestAPICase.REMOVEREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Request::getRemoveRequest,
            new RemoveRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setRemoveResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.WRITE)));

    operationContexts.put(RequestAPICase.GETREGIONNAMESREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Request::getGetRegionNamesRequest,
            new GetRegionNamesRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setGetRegionNamesResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.READ)));

    operationContexts.put(RequestAPICase.GETREGIONREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Request::getGetRegionRequest,
            new GetRegionRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setGetRegionResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.READ)));

    operationContexts.put(RequestAPICase.GETSERVERREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Request::getGetServerRequest,
            new GetServerOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setGetServerResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.CLUSTER,
                ResourcePermission.Operation.READ)));
  }
}
