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

import java.util.Collection;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;

@Experimental
public class GetRegionNamesRequestOperationHandler implements
    ProtobufOperationHandler<RegionAPI.GetRegionNamesRequest, RegionAPI.GetRegionNamesResponse> {

  @Override
  public Result<RegionAPI.GetRegionNamesResponse> process(
      ProtobufSerializationService serializationService, RegionAPI.GetRegionNamesRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {

    Collection<String> regions = messageExecutionContext.getSecureCache().getRegionNames();

    RegionAPI.GetRegionNamesResponse.Builder builder =
        RegionAPI.GetRegionNamesResponse.newBuilder();
    regions.forEach(builder::addRegions);

    return Success.of(builder.build());
  }
}
