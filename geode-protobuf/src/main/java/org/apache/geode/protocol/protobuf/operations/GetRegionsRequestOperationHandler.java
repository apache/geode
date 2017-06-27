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
package org.apache.geode.protocol.protobuf.operations;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.EncodingTypeTranslator;
import org.apache.geode.protocol.protobuf.ProtobufUtilities;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

import sun.reflect.generics.tree.BaseType;

import java.util.Set;

public class GetRegionsRequestOperationHandler
    implements OperationHandler<RegionAPI.GetRegionsRequest, RegionAPI.GetRegionsResponse> {

  @Override
  public RegionAPI.GetRegionsResponse process(SerializationService serializationService,
      RegionAPI.GetRegionsRequest request, Cache cache) {
    Set<Region<?, ?>> regions = cache.rootRegions();

    RegionAPI.GetRegionsResponse.Builder builder = RegionAPI.GetRegionsResponse.newBuilder();

    for (Region region : regions) {
      builder.addRegions(BasicTypes.Region.newBuilder().setName(region.getName()));
    }
    builder.setSuccess(true);
    return builder.build();
  }
}
