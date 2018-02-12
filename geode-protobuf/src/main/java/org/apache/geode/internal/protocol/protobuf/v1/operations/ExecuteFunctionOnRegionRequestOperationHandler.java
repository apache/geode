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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;

public class ExecuteFunctionOnRegionRequestOperationHandler implements
    ProtobufOperationHandler<FunctionAPI.ExecuteFunctionOnRegionRequest, FunctionAPI.ExecuteFunctionOnRegionResponse> {
  @Override
  public Result<FunctionAPI.ExecuteFunctionOnRegionResponse, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService,
      FunctionAPI.ExecuteFunctionOnRegionRequest request,
      MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, EncodingException, DecodingException {

    final String functionID = request.getFunctionID();
    final String regionName = request.getRegion();

    final Function<?> function = FunctionService.getFunction(functionID);
    if (function == null) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder().setErrorCode(BasicTypes.ErrorCode.INVALID_REQUEST)
              .setMessage(LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
                  .toLocalizedString(functionID))
              .build())
          .build());
    }

    final Region<Object, Object> region = messageExecutionContext.getCache().getRegion(regionName);
    if (region == null) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder().setErrorCode(BasicTypes.ErrorCode.INVALID_REQUEST)
              .setMessage("Region \"" + regionName + "\" not found"))
          .build());
    }

    final SecurityService securityService = messageExecutionContext.getCache().getSecurityService();

    try {
      // check security for function.
      function.getRequiredPermissions(regionName).forEach(securityService::authorize);
    } catch (NotAuthorizedException ex) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder()
              .setMessage("Authorization failed for function \"" + functionID + "\"")
              .setErrorCode(BasicTypes.ErrorCode.AUTHORIZATION_FAILED))
          .build());
    }

    try {
      Execution execution = FunctionService.onRegion(region);

      final Object arguments = serializationService.decode(request.getArguments());

      if (arguments != null) {
        execution = execution.setArguments(arguments);
      }

      execution = execution.withFilter(parseFilter(serializationService, request));

      final ResultCollector<Object, List<Object>> resultCollector = execution.execute(functionID);

      if (function.hasResult()) {
        List<Object> results = resultCollector.getResult();

        final FunctionAPI.ExecuteFunctionOnRegionResponse.Builder responseMessage =
            FunctionAPI.ExecuteFunctionOnRegionResponse.newBuilder();
        for (Object result : results) {
          responseMessage.addResults(serializationService.encode(result));
        }
        return Success.of(responseMessage.build());
      } else {
        // This is fire and forget.
        return Success.of(FunctionAPI.ExecuteFunctionOnRegionResponse.newBuilder().build());
      }
    } catch (FunctionException ex) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder().setErrorCode(BasicTypes.ErrorCode.SERVER_ERROR)
              .setMessage("Function execution failed: " + ex.toString()))
          .build());
    }
  }

  private Set<Object> parseFilter(ProtobufSerializationService serializationService,
      FunctionAPI.ExecuteFunctionOnRegionRequest request) throws DecodingException {
    List<BasicTypes.EncodedValue> encodedFilter = request.getKeyFilterList();
    Set<Object> filter = new HashSet<>();

    for (BasicTypes.EncodedValue filterKey : encodedFilter) {
      filter.add(serializationService.decode(filterKey));
    }
    return filter;
  }
}
