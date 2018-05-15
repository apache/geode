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

import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnMemberRequest;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnMemberResponse;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;

public class ExecuteFunctionOnMemberRequestOperationHandler implements
    ProtobufOperationHandler<ExecuteFunctionOnMemberRequest, ExecuteFunctionOnMemberResponse> {

  @Override
  public Result<ExecuteFunctionOnMemberResponse> process(
      ProtobufSerializationService serializationService, ExecuteFunctionOnMemberRequest request,
      MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException, EncodingException {

    final String functionID = request.getFunctionID();
    List<String> memberNameList = request.getMemberNameList();
    Object arguments = getFunctionArguments(request, serializationService);

    List<Object> results = messageExecutionContext.getSecureCache().getFunctionService()
        .executeFunctionOnMember(functionID, arguments, memberNameList);

    final ExecuteFunctionOnMemberResponse.Builder responseMessage =
        ExecuteFunctionOnMemberResponse.newBuilder();

    results.stream().map(serializationService::encode).forEach(responseMessage::addResults);

    return Success.of(responseMessage.build());
  }

  private Object getFunctionArguments(ExecuteFunctionOnMemberRequest request,
      ProtobufSerializationService serializationService) throws DecodingException {
    if (request.hasArguments()) {
      return serializationService.decode(request.getArguments());
    } else {
      return null;
    }
  }
}
