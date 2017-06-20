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
package org.apache.geode.protocol.protobuf;

import org.apache.geode.cache.Cache;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.operations.registry.OperationsHandlerRegistry;
import org.apache.geode.protocol.operations.registry.exception.OperationHandlerNotRegisteredException;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.TypeEncodingException;

/**
 * This handles protobuf requests by determining the operation type of the request and dispatching
 * it to the appropriate handler.
 */
public class ProtobufOpsProcessor {
  private final OperationsHandlerRegistry opsHandlerRegistry;
  private final SerializationService serializationService;

  public ProtobufOpsProcessor(OperationsHandlerRegistry opsHandlerRegistry,
      SerializationService serializationService) {
    this.opsHandlerRegistry = opsHandlerRegistry;
    this.serializationService = serializationService;
  }

  public ClientProtocol.Response process(ClientProtocol.Request request, Cache cache)
      throws TypeEncodingException, OperationHandlerNotRegisteredException,
      InvalidProtocolMessageException {
    OperationHandler opsHandler = opsHandlerRegistry
        .getOperationHandlerForOperationId(request.getRequestAPICase().getNumber());

    Object responseMessage =
        opsHandler.process(serializationService, getRequestForOperationTypeID(request), cache);
    return ClientProtocol.Response.newBuilder()
        .setGetResponse((RegionAPI.GetResponse) responseMessage).build();
  }

  // package visibility for testing
  static Object getRequestForOperationTypeID(ClientProtocol.Request request)
      throws InvalidProtocolMessageException {
    switch (request.getRequestAPICase()) {
      case PUTREQUEST:
        return request.getPutRequest();
      case GETREQUEST:
        return request.getGetRequest();
      case PUTALLREQUEST:
        return request.getPutAllRequest();
      default:
        throw new InvalidProtocolMessageException(
            "Unknown request type: " + request.getRequestAPICase().getNumber());
    }
  }
}
