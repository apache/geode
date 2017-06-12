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
package org.apache.geode.protocol;

import org.apache.geode.cache.Cache;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.operations.ProtobufRequestOperationParser;
import org.apache.geode.protocol.operations.registry.OperationsHandlerRegistry;
import org.apache.geode.protocol.operations.registry.exception.OperationHandlerNotRegisteredException;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

public class OpsProcessor {
  private final OperationsHandlerRegistry opsHandlerRegistry;
  private final SerializationService serializationService;

  public OpsProcessor(OperationsHandlerRegistry opsHandlerRegistry,
      SerializationService serializationService) {
    this.opsHandlerRegistry = opsHandlerRegistry;
    this.serializationService = serializationService;
  }

  public ClientProtocol.Response process(ClientProtocol.Request request, Cache cache)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    OperationHandler opsHandler = null;
    try {
      opsHandler = opsHandlerRegistry
          .getOperationHandlerForOperationId(request.getRequestAPICase().getNumber());
    } catch (OperationHandlerNotRegisteredException e) {
      e.printStackTrace();
    }

    Object responseMessage = opsHandler.process(serializationService,
        ProtobufRequestOperationParser.getRequestForOperationTypeID(request), cache);
    return ClientProtocol.Response.newBuilder()
        .setGetResponse((RegionAPI.GetResponse) responseMessage).build();
  }
}
