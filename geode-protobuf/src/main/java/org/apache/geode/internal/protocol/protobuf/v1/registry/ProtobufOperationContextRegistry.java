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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message.MessageTypeCase;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufOperationContext;
import org.apache.geode.internal.protocol.protobuf.v1.operations.ClearRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.DisconnectClientRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.ExecuteFunctionOnGroupRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.ExecuteFunctionOnMemberRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.ExecuteFunctionOnRegionRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetAllRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetRegionNamesRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetServerOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.GetSizeRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.KeySetOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.OqlQueryRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.PutAllRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.PutIfAbsentRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.PutRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.RemoveRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.operations.security.HandshakeRequestOperationHandler;
import org.apache.geode.services.module.ModuleService;

@Experimental
public class ProtobufOperationContextRegistry {
  private final Map<MessageTypeCase, ProtobufOperationContext> operationContexts;

  public ProtobufOperationContextRegistry(ModuleService moduleService) {
    operationContexts = Collections.unmodifiableMap(generateContexts(moduleService));
  }

  public ProtobufOperationContext getOperationContext(MessageTypeCase apiCase) {
    return operationContexts.get(apiCase);
  }

  private Map<MessageTypeCase, ProtobufOperationContext> generateContexts(
      ModuleService moduleService) {
    final Map<MessageTypeCase, ProtobufOperationContext> operationContexts = new HashMap<>();

    operationContexts.put(MessageTypeCase.HANDSHAKEREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getHandshakeRequest,
            new HandshakeRequestOperationHandler(moduleService),
            opsResp -> ClientProtocol.Message.newBuilder().setHandshakeResponse(opsResp)));

    operationContexts.put(MessageTypeCase.DISCONNECTCLIENTREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getDisconnectClientRequest,
            new DisconnectClientRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setDisconnectClientResponse(opsResp)));

    operationContexts.put(MessageTypeCase.GETREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetRequest,
            new GetRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetResponse(opsResp)));

    operationContexts.put(MessageTypeCase.GETALLREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetAllRequest,
            new GetAllRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetAllResponse(opsResp)));

    operationContexts.put(MessageTypeCase.PUTREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getPutRequest,
            new PutRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setPutResponse(opsResp)));

    operationContexts.put(MessageTypeCase.PUTALLREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getPutAllRequest,
            new PutAllRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setPutAllResponse(opsResp)));

    operationContexts.put(MessageTypeCase.REMOVEREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getRemoveRequest,
            new RemoveRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setRemoveResponse(opsResp)));

    operationContexts.put(MessageTypeCase.GETREGIONNAMESREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetRegionNamesRequest,
            new GetRegionNamesRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetRegionNamesResponse(opsResp)));

    operationContexts.put(MessageTypeCase.GETSIZEREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetSizeRequest,
            new GetSizeRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetSizeResponse(opsResp)));

    operationContexts.put(MessageTypeCase.GETSERVERREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getGetServerRequest,
            new GetServerOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setGetServerResponse(opsResp)));

    operationContexts.put(MessageTypeCase.EXECUTEFUNCTIONONREGIONREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getExecuteFunctionOnRegionRequest,
            new ExecuteFunctionOnRegionRequestOperationHandler(), opsResp -> ClientProtocol.Message
                .newBuilder().setExecuteFunctionOnRegionResponse(opsResp)));

    operationContexts.put(MessageTypeCase.EXECUTEFUNCTIONONMEMBERREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getExecuteFunctionOnMemberRequest,
            new ExecuteFunctionOnMemberRequestOperationHandler(), opsResp -> ClientProtocol.Message
                .newBuilder().setExecuteFunctionOnMemberResponse(opsResp)));

    operationContexts.put(MessageTypeCase.EXECUTEFUNCTIONONGROUPREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getExecuteFunctionOnGroupRequest,
            new ExecuteFunctionOnGroupRequestOperationHandler(), opsResp -> ClientProtocol.Message
                .newBuilder().setExecuteFunctionOnGroupResponse(opsResp)));
    operationContexts.put(MessageTypeCase.OQLQUERYREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getOqlQueryRequest,
            new OqlQueryRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setOqlQueryResponse(opsResp)));

    operationContexts.put(MessageTypeCase.KEYSETREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getKeySetRequest,
            new KeySetOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setKeySetResponse(opsResp)));

    operationContexts.put(ClientProtocol.Message.MessageTypeCase.CLEARREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getClearRequest,
            new ClearRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setClearResponse(opsResp)));

    operationContexts.put(MessageTypeCase.PUTIFABSENTREQUEST,
        new ProtobufOperationContext<>(ClientProtocol.Message::getPutIfAbsentRequest,
            new PutIfAbsentRequestOperationHandler(),
            opsResp -> ClientProtocol.Message.newBuilder().setPutIfAbsentResponse(opsResp)));

    return operationContexts;
  }
}
