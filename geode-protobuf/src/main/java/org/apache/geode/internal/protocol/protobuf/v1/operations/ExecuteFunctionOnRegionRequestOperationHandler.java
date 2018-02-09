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

import com.google.protobuf.AbstractMessage;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnRegionRequest;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnRegionResponse;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;

public class ExecuteFunctionOnRegionRequestOperationHandler
    extends AbstractFunctionRequestOperationHandler implements
    ProtobufOperationHandler<ExecuteFunctionOnRegionRequest, ExecuteFunctionOnRegionResponse> {

  @Override
  public Result<ExecuteFunctionOnRegionResponse, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService, ExecuteFunctionOnRegionRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {

    return (Result<ExecuteFunctionOnRegionResponse, ClientProtocol.ErrorResponse>) super.process(
        serializationService, request, messageExecutionContext);
  }

  protected Set<Object> parseFilter(ProtobufSerializationService serializationService,
      AbstractMessage request) throws EncodingException {
    List<BasicTypes.EncodedValue> encodedFilter =
        ((ExecuteFunctionOnRegionRequest) request).getKeyFilterList();
    Set<Object> filter = new HashSet<>();

    for (BasicTypes.EncodedValue filterKey : encodedFilter) {
      filter.add(serializationService.decode(filterKey));
    }
    return filter;
  }

  @Override
  protected String getFunctionID(AbstractMessage request) {
    return ((ExecuteFunctionOnRegionRequest) request).getFunctionID();
  }

  @Override
  protected String getRegionName(AbstractMessage request) {
    return ((ExecuteFunctionOnRegionRequest) request).getRegion();
  }

  @Override
  protected Object getExecutionTarget(AbstractMessage request, String regionName,
      MessageExecutionContext executionContext) throws InvalidExecutionContextException {
    final Region<Object, Object> region = executionContext.getCache().getRegion(regionName);
    if (region == null) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder().setErrorCode(BasicTypes.ErrorCode.INVALID_REQUEST)
              .setMessage("Region \"" + regionName + "\" not found"))
          .build());
    }
    return region;
  }

  @Override
  protected Object getFunctionArguments(AbstractMessage request,
      ProtobufSerializationService serializationService) throws EncodingException {
    return serializationService.decode(((ExecuteFunctionOnRegionRequest) request).getArguments());
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
  protected Result buildResultMessage(ProtobufSerializationService serializationService)
      throws EncodingException {
    return Success.of(ExecuteFunctionOnRegionResponse.newBuilder().build());
  }

}
