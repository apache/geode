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
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufOperationContext;
import org.apache.geode.internal.protocol.protobuf.v1.operations.DisconnectClientRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.ExecuteFunctionOnGroupRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.ExecuteFunctionOnMemberRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.ExecuteFunctionOnRegionRequestOperationHandler;
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
  private Map<ClientProtocol.Message.MessageTypeCase, ProtobufOperationContext> operationContexts =
      new ConcurrentHashMap<>();

  public ProtobufOperationContextRegistry() {
    addContexts();
  }

  public ProtobufOperationContext getOperationContext(
      ClientProtocol.Message.MessageTypeCase apiCase) {
    return operationContexts.get(apiCase);
  }

  private void addContexts() {
    operationContexts.put(ClientProtocol.Message.MessageTypeCase.AUTHENTICATIONREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getAuthenticationRequest,
            new AuthenticationRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setAuthenticationResponse(opsResp),
            new ResourcePermission(ResourcePermission.NULL, ResourcePermission.NULL)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.DISCONNECTCLIENTREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getDisconnectClientRequest,
            new DisconnectClientRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setDisconnectClientResponse(opsResp),
            new ResourcePermission(ResourcePermission.NULL, ResourcePermission.NULL)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.GETREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetRequest,
            new GetRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.READ)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.GETALLREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetAllRequest,
            new GetAllRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetAllResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.READ)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.PUTREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getPutRequest,
            new PutRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setPutResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.WRITE)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.PUTALLREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getPutAllRequest,
            new PutAllRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setPutAllResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.WRITE)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.REMOVEREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getRemoveRequest,
            new RemoveRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setRemoveResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.WRITE)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.GETREGIONNAMESREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetRegionNamesRequest,
            new GetRegionNamesRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetRegionNamesResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.READ)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.GETREGIONREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetRegionRequest,
            new GetRegionRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetRegionResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.DATA,
                ResourcePermission.Operation.READ)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.GETSERVERREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetServerRequest,
            new GetServerOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetServerResponse(opsResp),
            new ResourcePermission(ResourcePermission.Resource.CLUSTER,
                ResourcePermission.Operation.READ)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONREGIONREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getExecuteFunctionOnRegionRequest,
            new ExecuteFunctionOnRegionRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder()
                .setExecuteFunctionOnRegionResponse(opsResp),
            // Resource permissions get handled per-function, since they have varying permission
            // requirements.
            new ResourcePermission(ResourcePermission.NULL, ResourcePermission.NULL)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONMEMBERREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getExecuteFunctionOnMemberRequest,
            new ExecuteFunctionOnMemberRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder()
                .setExecuteFunctionOnMemberResponse(opsResp),
            // Resource permissions get handled per-function, since they have varying permission
            // requirements.
            new ResourcePermission(ResourcePermission.NULL, ResourcePermission.NULL)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONGROUPREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getExecuteFunctionOnGroupRequest,
            new ExecuteFunctionOnGroupRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder()
                .setExecuteFunctionOnGroupResponse(opsResp),
            // Resource permissions get handled per-function, since they have varying permission
            // requirements.
            new ResourcePermission(ResourcePermission.NULL, ResourcePermission.NULL)));
  }
}
