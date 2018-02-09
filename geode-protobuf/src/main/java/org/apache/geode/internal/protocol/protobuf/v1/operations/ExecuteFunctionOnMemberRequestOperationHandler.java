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
import com.google.protobuf.ProtocolStringList;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnMemberRequest;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnMemberResponse;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;

public class ExecuteFunctionOnMemberRequestOperationHandler
    extends AbstractFunctionRequestOperationHandler implements
    ProtobufOperationHandler<ExecuteFunctionOnMemberRequest, ExecuteFunctionOnMemberResponse> {


  @Override
  public Result<ExecuteFunctionOnMemberResponse, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService, ExecuteFunctionOnMemberRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {

    return (Result<ExecuteFunctionOnMemberResponse, ClientProtocol.ErrorResponse>) super.process(
        serializationService, request, messageExecutionContext);
  }

  @Override
  protected Set<?> parseFilter(ProtobufSerializationService serializationService,
      AbstractMessage request) throws EncodingException {
    // filters are not allowed on functions not associated with regions
    return null;
  }

  @Override
  protected String getFunctionID(AbstractMessage request) {
    return ((ExecuteFunctionOnMemberRequest) request).getFunctionID();
  }

  @Override
  protected String getRegionName(AbstractMessage request) {
    // region name is not allowed in onMember invocation
    return null;
  }

  @Override
  protected Object getExecutionTarget(AbstractMessage abstractRequest, String regionName,
      MessageExecutionContext executionContext) throws InvalidExecutionContextException {
    ExecuteFunctionOnMemberRequest request = (ExecuteFunctionOnMemberRequest) abstractRequest;

    ProtocolStringList memberNameList = request.getMemberNameList();

    Set<DistributedMember> memberIds = new HashSet<>(memberNameList.size());
    DistributionManager distributionManager = executionContext.getCache().getDistributionManager();
    for (String name : memberNameList) {
      DistributedMember member = distributionManager.getMemberWithName(name);
      if (member == null) {
        return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
            .setError(BasicTypes.Error.newBuilder()
                .setMessage(
                    "Member " + name + " not found to execute \"" + request.getFunctionID() + "\"")
                .setErrorCode(BasicTypes.ErrorCode.NO_AVAILABLE_SERVER))
            .build());
      }
      memberIds.add(member);
    }
    if (memberIds.isEmpty()) {
      return Failure.of(ClientProtocol.ErrorResponse.newBuilder()
          .setError(BasicTypes.Error.newBuilder()
              .setMessage("No members found to execute \"" + request.getFunctionID() + "\"")
              .setErrorCode(BasicTypes.ErrorCode.NO_AVAILABLE_SERVER))
          .build());
    }
    return memberIds;
  }

  @Override
  protected Object getFunctionArguments(AbstractMessage request,
      ProtobufSerializationService serializationService) throws EncodingException {
    return serializationService.decode(((ExecuteFunctionOnMemberRequest) request).getArguments());
  }

  @Override
  protected Execution getFunctionExecutionObject(Object executionTarget) {
    Set<DistributedMember> memberIds = (Set<DistributedMember>) executionTarget;
    if (memberIds.size() == 1) {
      return FunctionService.onMember(memberIds.iterator().next());
    } else {
      return FunctionService.onMembers(memberIds);
    }
  }

  @Override
  protected Result buildResultMessage(ProtobufSerializationService serializationService,
      List<Object> results) throws EncodingException {
    final ExecuteFunctionOnMemberResponse.Builder responseMessage =
        ExecuteFunctionOnMemberResponse.newBuilder();
    for (Object result : results) {
      responseMessage.addResults(serializationService.encode(result));
    }
    return Success.of(responseMessage.build());
  }

  @Override
  protected Result buildResultMessage(ProtobufSerializationService serializationService)
      throws EncodingException {
    return Success.of(ExecuteFunctionOnMemberResponse.newBuilder().build());
  }

}
