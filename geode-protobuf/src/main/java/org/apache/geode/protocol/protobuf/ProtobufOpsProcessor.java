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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.protocol.protobuf.registry.OperationContextRegistry;
import org.apache.geode.serialization.SerializationService;

/**
 * This handles protobuf requests by determining the operation type of the request and dispatching
 * it to the appropriate handler.
 */
@Experimental
public class ProtobufOpsProcessor {

  private final OperationContextRegistry operationContextRegistry;
  private final SerializationService serializationService;

  public ProtobufOpsProcessor(SerializationService serializationService,
      OperationContextRegistry operationContextRegistry) {
    this.serializationService = serializationService;
    this.operationContextRegistry = operationContextRegistry;
  }

  public ClientProtocol.Response process(ClientProtocol.Request request, Cache cache) {
    ClientProtocol.Request.RequestAPICase requestType = request.getRequestAPICase();
    OperationContext operationContext = operationContextRegistry.getOperationContext(requestType);
    ClientProtocol.Response.Builder builder;
    Result result = operationContext.getOperationHandler().process(serializationService,
        operationContext.getFromRequest().apply(request), cache);

    builder = (ClientProtocol.Response.Builder) result.map(operationContext.getToResponse(),
        operationContext.getToErrorResponse());
    return builder.build();
  }
}
