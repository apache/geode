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
package org.apache.geode.experimental.driver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI;

public class ProtobufFunction<T> implements Function<T> {
  private final String functionId;
  private final ProtobufChannel channel;
  private final ValueEncoder valueEncoder;

  public ProtobufFunction(String functionId, ProtobufChannel channel, ValueEncoder valueEncoder) {
    this.functionId = functionId;
    this.channel = channel;
    this.valueEncoder = valueEncoder;
  }

  @Override
  public List<T> executeOnRegion(Object arguments, String regionName, Object... keyFilters)
      throws IOException {
    List<BasicTypes.EncodedValue> encodedFilters = Arrays.stream(keyFilters)
        .map(valueEncoder::encodeValue).collect(Collectors.toList());
    ClientProtocol.Message request = ClientProtocol.Message.newBuilder()
        .setExecuteFunctionOnRegionRequest(FunctionAPI.ExecuteFunctionOnRegionRequest.newBuilder()
            .setRegion(regionName).addAllKeyFilter(encodedFilters).setFunctionID(functionId))
        .build();
    final FunctionAPI.ExecuteFunctionOnRegionResponse response = channel
        .sendRequest(request,
            ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONREGIONRESPONSE)
        .getExecuteFunctionOnRegionResponse();
    return response.getResultsList().stream().map(valueEncoder::<T>decodeValue)
        .collect(Collectors.toList());
  }

  @Override
  public List<T> executeOnMember(Object arguments, String... members) throws IOException {
    final List<String> stringMembers = Arrays.asList(members);
    ClientProtocol.Message request = ClientProtocol.Message.newBuilder()
        .setExecuteFunctionOnMemberRequest(FunctionAPI.ExecuteFunctionOnMemberRequest.newBuilder()
            .addAllMemberName(stringMembers).setFunctionID(functionId))
        .build();
    final FunctionAPI.ExecuteFunctionOnMemberResponse response = channel
        .sendRequest(request,
            ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONMEMBERRESPONSE)
        .getExecuteFunctionOnMemberResponse();
    return response.getResultsList().stream().map(valueEncoder::<T>decodeValue)
        .collect(Collectors.toList());
  }

  @Override
  public List<T> executeOnGroup(Object arguments, String... groups) throws IOException {
    final List<String> stringGroups = Arrays.asList(groups);
    ClientProtocol.Message request = ClientProtocol.Message.newBuilder()
        .setExecuteFunctionOnGroupRequest(FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder()
            .addAllGroupName(stringGroups).setFunctionID(functionId))
        .build();
    final FunctionAPI.ExecuteFunctionOnGroupResponse response = channel
        .sendRequest(request, ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONGROUPRESPONSE)
        .getExecuteFunctionOnGroupResponse();
    return response.getResultsList().stream().map(valueEncoder::<T>decodeValue)
        .collect(Collectors.toList());
  }
}
