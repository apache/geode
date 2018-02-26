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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnRegionRequest;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnRegionResponse;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;

public class ExecuteFunctionOnRegionRequestOperationHandler extends
    AbstractFunctionRequestOperationHandler<ExecuteFunctionOnRegionRequest, ExecuteFunctionOnRegionResponse> {
  private static final Logger logger = LogService.getLogger();

  protected Set<Object> parseFilter(ProtobufSerializationService serializationService,
      ExecuteFunctionOnRegionRequest request) throws DecodingException {
    List<BasicTypes.EncodedValue> encodedFilter = request.getKeyFilterList();
    Set<Object> filter = new HashSet<>();

    for (BasicTypes.EncodedValue filterKey : encodedFilter) {
      filter.add(serializationService.decode(filterKey));
    }
    return filter;
  }

  @Override
  protected String getFunctionID(ExecuteFunctionOnRegionRequest request) {
    return request.getFunctionID();
  }

  @Override
  protected String getRegionName(ExecuteFunctionOnRegionRequest request) {
    return request.getRegion();
  }

  @Override
  protected Object getExecutionTarget(ExecuteFunctionOnRegionRequest request, String regionName,
      MessageExecutionContext executionContext) throws InvalidExecutionContextException {
    final Region<Object, Object> region = executionContext.getCache().getRegion(regionName);
    if (region == null) {
      logger.error("Received execute-function-on-region request for nonexistent region: {}",
          regionName);
      return Failure.of(BasicTypes.ErrorCode.SERVER_ERROR,
          "Region \"" + regionName + "\" not found");
    }
    return region;
  }

  @Override
  protected Object getFunctionArguments(ExecuteFunctionOnRegionRequest request,
      ProtobufSerializationService serializationService) throws DecodingException {
    if (request.hasArguments()) {
      return serializationService.decode(request.getArguments());
    } else {
      return null;
    }
  }

  @Override
  protected Execution getFunctionExecutionObject(Object executionTarget) {
    return FunctionService.onRegion((Region) executionTarget);
  }

  @Override
  protected Result buildResultMessage(ProtobufSerializationService serializationService,
      List<Object> results) throws EncodingException {
    final ExecuteFunctionOnRegionResponse.Builder responseMessage =
        ExecuteFunctionOnRegionResponse.newBuilder();
    for (Object result : results) {
      responseMessage.addResults(serializationService.encode(result));
    }
    return Success.of(responseMessage.build());
  }

  @Override
  protected Result buildResultMessage(ProtobufSerializationService serializationService) {
    return Success.of(ExecuteFunctionOnRegionResponse.newBuilder().build());
  }

}
