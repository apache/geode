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

import java.util.List;
import java.util.Set;

import com.google.protobuf.ProtocolStringList;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnGroupRequest;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnGroupResponse;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;

public class ExecuteFunctionOnGroupRequestOperationHandler extends
    AbstractFunctionRequestOperationHandler<ExecuteFunctionOnGroupRequest, ExecuteFunctionOnGroupResponse> {


  @Override
  protected Set<?> parseFilter(ProtobufSerializationService serializationService,
      ExecuteFunctionOnGroupRequest request) {
    // filters are not allowed on functions not associated with regions
    return null;
  }

  @Override
  protected String getFunctionID(ExecuteFunctionOnGroupRequest request) {
    return request.getFunctionID();
  }

  @Override
  protected String getRegionName(ExecuteFunctionOnGroupRequest request) {
    // region name is not allowed in onMember invocation
    return null;
  }

  @Override
  protected Object getExecutionTarget(ExecuteFunctionOnGroupRequest request, String regionName,
      MessageExecutionContext executionContext) throws InvalidExecutionContextException {

    ProtocolStringList groupList = request.getGroupNameList();

    // unfortunately FunctionServiceManager throws a FunctionException if there are no
    // servers matching any of the given groups. In order to distinguish between
    // function execution failure and this condition we have to preprocess the groups
    // and ensure that there is at least one server that has one of the given groups
    DistributedSystem distributedSystem =
        executionContext.getCache().getDistributionManager().getSystem();
    boolean foundMatch = false;
    for (String group : groupList) {
      if (distributedSystem.getGroupMembers(group).size() > 0) {
        foundMatch = true;
        break;
      }
    }
    if (!foundMatch) {
      return Failure.of(BasicTypes.ErrorCode.NO_AVAILABLE_SERVER, "No server  in groups "
          + groupList + " could be found to execute \"" + request.getFunctionID() + "\"");
    }
    return groupList;
  }

  @Override
  protected Object getFunctionArguments(ExecuteFunctionOnGroupRequest request,
      ProtobufSerializationService serializationService) throws DecodingException {
    if (request.hasArguments()) {
      return serializationService.decode(request.getArguments());
    } else {
      return null;
    }
  }

  @Override
  protected Execution getFunctionExecutionObject(Object executionTarget) {
    ProtocolStringList groupList = (ProtocolStringList) executionTarget;
    return FunctionService.onMember(groupList.toArray(new String[0]));
  }

  @Override
  protected Result buildResultMessage(ProtobufSerializationService serializationService,
      List<Object> results) throws EncodingException {
    final ExecuteFunctionOnGroupResponse.Builder responseMessage =
        ExecuteFunctionOnGroupResponse.newBuilder();
    for (Object result : results) {
      responseMessage.addResults(serializationService.encode(result));
    }
    return Success.of(responseMessage.build());
  }

  @Override
  protected Result buildResultMessage(ProtobufSerializationService serializationService) {
    return Success.of(ExecuteFunctionOnGroupResponse.newBuilder().build());
  }

}
