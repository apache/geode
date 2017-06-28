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
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.ProtobufUtilities;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

public class PutRequestOperationHandler
    implements OperationHandler<RegionAPI.PutRequest, RegionAPI.PutResponse> {

  @Override
  public RegionAPI.PutResponse process(SerializationService serializationService,
      RegionAPI.PutRequest request, Cache cache) {
    try {
      String regionName = request.getRegionName();
      BasicTypes.Entry entry = request.getEntry();

      Object decodedValue = ProtobufUtilities.decodeValue(serializationService, entry.getValue());
      Object decodedKey = ProtobufUtilities.decodeValue(serializationService, entry.getKey());

      Region region = cache.getRegion(regionName);
      if (region == null) {
        cache.getLogger().error("Region passed by client did not exist:" + region);
      } else {
        try {
          region.put(decodedKey, decodedValue);
          return RegionAPI.PutResponse.newBuilder().setSuccess(true).build();
        } catch (ClassCastException ex) {
          cache.getLogger()
              .error("invalid key or value type for region " + regionName + ",passed key: "
                  + entry.getKey().getEncodingType() + " value: "
                  + entry.getValue().getEncodingType(), ex);
        }
      }
    } catch (UnsupportedEncodingTypeException ex) {
      cache.getLogger().error("encoding not supported ", ex);
    } catch (CodecNotRegisteredForTypeException ex) {
      cache.getLogger().error("codec error in protobuf deserialization ", ex);
    }
    return RegionAPI.PutResponse.newBuilder().setSuccess(false).build();
  }
}
