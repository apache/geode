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
import org.apache.geode.protocol.protobuf.ProtocolErrorCode;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

public class PutRequestOperationHandler
    implements OperationHandler<RegionAPI.PutRequest, RegionAPI.PutResponse> {

  @Override
  public Result<RegionAPI.PutResponse> process(SerializationService serializationService,
      RegionAPI.PutRequest request, Cache cache) {
    String regionName = request.getRegionName();
    Region region = cache.getRegion(regionName);
    if (region == null) {
      return Failure.of(BasicTypes.ErrorResponse.newBuilder()
          .setErrorCode(ProtocolErrorCode.REGION_NOT_FOUND.codeValue)
          .setMessage("Region passed by client did not exist: " + regionName).build());
    }

    try {
      BasicTypes.Entry entry = request.getEntry();

      Object decodedValue = ProtobufUtilities.decodeValue(serializationService, entry.getValue());
      Object decodedKey = ProtobufUtilities.decodeValue(serializationService, entry.getKey());
      try {
        region.put(decodedKey, decodedValue);
        return Success.of(RegionAPI.PutResponse.newBuilder().build());
      } catch (ClassCastException ex) {
        return Failure.of(BasicTypes.ErrorResponse.newBuilder()
            .setErrorCode(ProtocolErrorCode.CONSTRAINT_VIOLATION.codeValue)
            .setMessage("invalid key or value type for region " + regionName).build());
      }
    } catch (UnsupportedEncodingTypeException ex) {
      return Failure.of(BasicTypes.ErrorResponse.newBuilder()
          .setErrorCode(ProtocolErrorCode.VALUE_ENCODING_ERROR.codeValue)
          .setMessage(ex.getMessage()).build());
    } catch (CodecNotRegisteredForTypeException ex) {
      return Failure.of(BasicTypes.ErrorResponse.newBuilder()
          .setErrorCode(ProtocolErrorCode.VALUE_ENCODING_ERROR.codeValue)
          .setMessage(ex.getMessage()).build());
    }
  }
}
