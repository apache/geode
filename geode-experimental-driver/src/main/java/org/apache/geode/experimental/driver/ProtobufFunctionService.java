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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;

public class ProtobufFunctionService implements FunctionService {
  private final ProtobufChannel channel;

  public ProtobufFunctionService(ProtobufChannel channel) {
    this.channel = channel;
  }

  @Override
  public <T> Function newFunction(String functionId) {
    return new ProtobufFunction<T>(functionId);
  }

  class ProtobufFunction<T> implements Function<T> {
    private final String functionId;
    private Set<String> members;
    private Set<String> groups;

    public ProtobufFunction(String functionId) {
      this.functionId = functionId;
    }

    @Override
    public List<T> executeOnRegion(Object arguments, String regionName, Object... keyFilters)
        throws IOException {
      List<BasicTypes.EncodedValue> encodedFilters = Arrays.asList(keyFilters).stream()
          .map(ValueEncoder::encodeValue).collect(Collectors.toList());
      ClientProtocol.Message request = ClientProtocol.Message.newBuilder()
          .setExecuteFunctionOnRegionRequest(FunctionAPI.ExecuteFunctionOnRegionRequest.newBuilder()
              .setRegion(regionName).addAllKeyFilter(encodedFilters).setFunctionID(functionId))
          .build();
      final FunctionAPI.ExecuteFunctionOnRegionResponse response = channel
          .sendRequest(request,
              ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONREGIONRESPONSE)
          .getExecuteFunctionOnRegionResponse();
      return response.getResultsList().stream().map(value -> (T) ValueEncoder.decodeValue(value))
          .collect(Collectors.toList());
    }

    @Override
    public List<T> executeOnMember(Object arguments, Object... members) throws IOException {
      final List<String> stringMembers =
          Arrays.asList(Arrays.copyOf(members, members.length, String[].class));
      ClientProtocol.Message request = ClientProtocol.Message.newBuilder()
          .setExecuteFunctionOnMemberRequest(FunctionAPI.ExecuteFunctionOnMemberRequest.newBuilder()
              .addAllMemberName(stringMembers).setFunctionID(functionId))
          .build();
      final FunctionAPI.ExecuteFunctionOnMemberResponse response = channel
          .sendRequest(request,
              ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONMEMBERRESPONSE)
          .getExecuteFunctionOnMemberResponse();
      return response.getResultsList().stream().map(value -> (T) ValueEncoder.decodeValue(value))
          .collect(Collectors.toList());
    }

    @Override
    public List<T> executeOnGroup(Object arguments, Object... groups) throws IOException {
      final List<String> stringGroups =
          Arrays.asList(Arrays.copyOf(groups, groups.length, String[].class));
      ClientProtocol.Message request = ClientProtocol.Message.newBuilder()
          .setExecuteFunctionOnGroupRequest(FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder()
              .addAllGroupName(stringGroups).setFunctionID(functionId))
          .build();
      final FunctionAPI.ExecuteFunctionOnGroupResponse response = channel
          .sendRequest(request,
              ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONGROUPRESPONSE)
          .getExecuteFunctionOnGroupResponse();
      return response.getResultsList().stream().map(value -> (T) ValueEncoder.decodeValue(value))
          .collect(Collectors.toList());
    }
  }
}
