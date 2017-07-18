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
import org.apache.geode.protocol.protobuf.utilities.ProtobufResponseUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RemoveRequestOperationHandler
    implements OperationHandler<RegionAPI.RemoveRequest, RegionAPI.RemoveResponse> {
  private static Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.RemoveResponse> process(SerializationService serializationService,
      RegionAPI.RemoveRequest request, Cache cache) {

    String regionName = request.getRegionName();
    Region region = cache.getRegion(regionName);
    if (region == null) {
      return Failure
          .of(BasicTypes.ErrorResponse.newBuilder().setMessage("Region not found").build());
    }

    try {
      Object decodedKey = ProtobufUtilities.decodeValue(serializationService, request.getKey());
      region.remove(decodedKey);

      return Success.of(RegionAPI.RemoveResponse.newBuilder().build());
    } catch (UnsupportedEncodingTypeException ex) {
      // can be thrown by encoding or decoding.
      return Failure.of(ProtobufResponseUtilities
          .createAndLogErrorResponse("Encoding not supported.", logger, ex));
    } catch (CodecNotRegisteredForTypeException ex) {
      return Failure.of(ProtobufResponseUtilities
          .createAndLogErrorResponse("Codec error in protobuf deserialization.", logger, ex));
    }
  }
}
