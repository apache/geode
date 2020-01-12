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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.security.SecureFunctionService;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnRegionRequest;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnRegionResponse;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ExecuteFunctionOnRegionRequestOperationHandler implements
    ProtobufOperationHandler<ExecuteFunctionOnRegionRequest, ExecuteFunctionOnRegionResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<ExecuteFunctionOnRegionResponse> process(
      ProtobufSerializationService serializationService, ExecuteFunctionOnRegionRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException,
      ConnectionStateException, EncodingException, DecodingException {

    final String functionID = request.getFunctionID();
    final String regionName = request.getRegion();
    Object arguments = getFunctionArguments(request, serializationService);
    Set<?> filter = parseFilter(serializationService, request);

    SecureFunctionService functionService =
        messageExecutionContext.getSecureCache().getFunctionService();

    List<Object> results =
        functionService.executeFunctionOnRegion(functionID, regionName, arguments, filter);

    final ExecuteFunctionOnRegionResponse.Builder responseMessage =
        ExecuteFunctionOnRegionResponse.newBuilder();
    for (Object result : results) {
      responseMessage.addResults(serializationService.encode(result));
    }
    return Success.of(responseMessage.build());
  }

  private Set<Object> parseFilter(ProtobufSerializationService serializationService,
      ExecuteFunctionOnRegionRequest request) throws DecodingException {
    List<BasicTypes.EncodedValue> encodedFilter = request.getKeyFilterList();
    Set<Object> filter = new HashSet<>();

    for (BasicTypes.EncodedValue filterKey : encodedFilter) {
      filter.add(serializationService.decode(filterKey));
    }
    return filter;
  }

  private Object getFunctionArguments(ExecuteFunctionOnRegionRequest request,
      ProtobufSerializationService serializationService) throws DecodingException {
    if (request.hasArguments()) {
      return serializationService.decode(request.getArguments());
    } else {
      return null;
    }
  }
}
