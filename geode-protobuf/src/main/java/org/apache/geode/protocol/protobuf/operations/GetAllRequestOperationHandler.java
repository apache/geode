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
import org.apache.geode.protocol.protobuf.Failure;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GetAllRequestOperationHandler
    implements OperationHandler<RegionAPI.GetAllRequest, RegionAPI.GetAllResponse> {

  @Override
  public Result<RegionAPI.GetAllResponse> process(SerializationService serializationService,
      RegionAPI.GetAllRequest request, Cache cache) {
    String regionName = request.getRegionName();
    Region region = cache.getRegion(regionName);
    if (region == null) {
      return Failure
          .of(BasicTypes.ErrorResponse.newBuilder().setMessage("Region not found").build());
    }

    try {
      Set<Object> keys = new HashSet<>();
      for (BasicTypes.EncodedValue key : request.getKeyList()) {
        keys.add(ProtobufUtilities.decodeValue(serializationService, key));
      }
      Map<?, ?> results = region.getAll(keys);
      Set<BasicTypes.Entry> entries = new HashSet<>();
      for (Map.Entry entry : results.entrySet()) {
        entries.add(
            ProtobufUtilities.createEntry(serializationService, entry.getKey(), entry.getValue()));
      }
      return Success.of(RegionAPI.GetAllResponse.newBuilder().addAllEntries(entries).build());
    } catch (UnsupportedEncodingTypeException ex) {
      return Failure
          .of(BasicTypes.ErrorResponse.newBuilder().setMessage("Encoding not supported.").build());
    } catch (CodecNotRegisteredForTypeException ex) {
      return Failure.of(BasicTypes.ErrorResponse.newBuilder()
          .setMessage("Codec error in protobuf deserialization.").build());
    }
  }
}
