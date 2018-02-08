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

import com.google.protobuf.ProtocolStringList;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
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
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;

public class ExecuteFunctionOnMemberRequestOperationHandler implements
    ProtobufOperationHandler<FunctionAPI.ExecuteFunctionOnMemberRequest, FunctionAPI.ExecuteFunctionOnMemberResponse> {
  @Override
  public Result<FunctionAPI.ExecuteFunctionOnMemberResponse, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService,
      FunctionAPI.ExecuteFunctionOnMemberRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {

    final String functionID = request.getFunctionID();

    final Function<?> function = FunctionService.getFunction(functionID);
    if (function == null) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder().setErrorCode(BasicTypes.ErrorCode.INVALID_REQUEST)
              .setMessage(LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
                  .toLocalizedString(functionID))
              .build())
          .build());
    }

    final SecurityService securityService = messageExecutionContext.getCache().getSecurityService();

    try {
      // check security for function.
      final String noRegion = null;
      function.getRequiredPermissions(noRegion).forEach(securityService::authorize);
    } catch (NotAuthorizedException ex) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder()
              .setMessage("Authorization failed for function \"" + functionID + "\"")
              .setErrorCode(BasicTypes.ErrorCode.AUTHORIZATION_FAILED))
          .build());
    }

    ProtocolStringList memberNameList = request.getMemberNameList();

    Set<DistributedMember> memberIds = new HashSet<>(memberNameList.size());
    DistributionManager distributionManager =
        messageExecutionContext.getCache().getDistributionManager();
    for (String name : memberNameList) {
      DistributedMember member = distributionManager.getMemberWithName(name);
      if (member == null) {
        return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
            .setError(BasicTypes.Error.newBuilder()
                .setMessage("Member " + name + " not found to execute \"" + functionID + "\"")
                .setErrorCode(BasicTypes.ErrorCode.NO_AVAILABLE_SERVER))
            .build());
      }
      memberIds.add(member);
    }

    if (memberIds.isEmpty()) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder()
              .setMessage("No members found to execute \"" + functionID + "\"")
              .setErrorCode(BasicTypes.ErrorCode.NO_AVAILABLE_SERVER))
          .build());
    }

    try {
      Execution execution;
      if (memberIds.size() == 1) {
        execution = FunctionService.onMember(memberIds.iterator().next());
      } else {
        execution = FunctionService.onMembers(memberIds);
      }

      final Object arguments = serializationService.decode(request.getArguments());

      if (arguments != null) {
        execution = execution.setArguments(arguments);
      }

      final ResultCollector<Object, List<Object>> resultCollector = execution.execute(functionID);

      if (function.hasResult()) {
        List<Object> results = resultCollector.getResult();

        final FunctionAPI.ExecuteFunctionOnMemberResponse.Builder responseMessage =
            FunctionAPI.ExecuteFunctionOnMemberResponse.newBuilder();
        for (Object result : results) {
          responseMessage.addResults(serializationService.encode(result));
        }
        return Success.of(responseMessage.build());
      } else {
        // This is fire and forget.
        return Success.of(FunctionAPI.ExecuteFunctionOnMemberResponse.newBuilder().build());
      }
    } catch (FunctionException ex) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder().setErrorCode(BasicTypes.ErrorCode.SERVER_ERROR)
              .setMessage("Function execution failed: " + ex.toString()))
          .build());
    } catch (EncodingException ex) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder().setErrorCode(BasicTypes.ErrorCode.SERVER_ERROR)
              .setMessage("Encoding failed: " + ex.toString()))
          .build());
    }
  }

}
