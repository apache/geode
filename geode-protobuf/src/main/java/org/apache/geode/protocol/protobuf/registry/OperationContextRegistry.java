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

package org.apache.geode.protocol.protobuf.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.ClientProtocol.Request.RequestAPICase;
import org.apache.geode.protocol.protobuf.OperationContext;
import org.apache.geode.protocol.protobuf.operations.GetAllRequestOperationHandler;
import org.apache.geode.protocol.protobuf.operations.GetRegionNamesRequestOperationHandler;
import org.apache.geode.protocol.protobuf.operations.GetRegionRequestOperationHandler;
import org.apache.geode.protocol.protobuf.operations.GetRequestOperationHandler;
import org.apache.geode.protocol.protobuf.operations.PutAllRequestOperationHandler;
import org.apache.geode.protocol.protobuf.operations.PutRequestOperationHandler;
import org.apache.geode.protocol.protobuf.operations.RemoveRequestOperationHandler;

public class OperationContextRegistry {
  private Map<RequestAPICase, OperationContext> operationContexts = new ConcurrentHashMap<>();

  public OperationContextRegistry() {
    addContexts();
  }

  public OperationContext getOperationContext(RequestAPICase apiCase) {
    return operationContexts.get(apiCase);
  }

  private void addContexts() {
    operationContexts.put(RequestAPICase.GETREQUEST,
        new OperationContext<>(ClientProtocol.Request::getGetRequest,
            new GetRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setGetResponse(opsResp)));

    operationContexts.put(RequestAPICase.GETALLREQUEST,
        new OperationContext<>(ClientProtocol.Request::getGetAllRequest,
            new GetAllRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setGetAllResponse(opsResp)));

    operationContexts.put(RequestAPICase.PUTREQUEST,
        new OperationContext<>(ClientProtocol.Request::getPutRequest,
            new PutRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setPutResponse(opsResp)));

    operationContexts.put(RequestAPICase.PUTALLREQUEST,
        new OperationContext<>(ClientProtocol.Request::getPutAllRequest,
            new PutAllRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setPutAllResponse(opsResp)));

    operationContexts.put(RequestAPICase.REMOVEREQUEST,
        new OperationContext<>(ClientProtocol.Request::getRemoveRequest,
            new RemoveRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setRemoveResponse(opsResp)));

    operationContexts.put(RequestAPICase.GETREGIONNAMESREQUEST,
        new OperationContext<>(ClientProtocol.Request::getGetRegionNamesRequest,
            new GetRegionNamesRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setGetRegionNamesResponse(opsResp)));

    operationContexts.put(RequestAPICase.GETREGIONREQUEST,
        new OperationContext<>(ClientProtocol.Request::getGetRegionRequest,
            new GetRegionRequestOperationHandler(),
            opsResp -> ClientProtocol.Response.newBuilder().setGetRegionResponse(opsResp)));
  }
}
