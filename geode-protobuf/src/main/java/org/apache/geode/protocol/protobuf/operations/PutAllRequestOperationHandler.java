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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class PutAllRequestOperationHandler
    implements OperationHandler<RegionAPI.PutAllRequest, RegionAPI.PutAllResponse> {
  private static Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.PutAllResponse> process(SerializationService serializationService,
      RegionAPI.PutAllRequest request, Cache cache) {
    String regionName = request.getRegionName();
    Region region = cache.getRegion(regionName);

    if (region == null) {
      return Failure.of(ProtobufResponseUtilities.createAndLogErrorResponse(
          "Region passed by client did not exist: " + regionName, logger, null));
    }

    Map entries = extractPutAllEntries(serializationService, request);
    try {
      region.putAll(entries);
    } catch (Exception ex) {
      return Failure
          .of(ProtobufResponseUtilities.createAndLogErrorResponse(ex.getMessage(), logger, ex));
    }

    return Success.of(RegionAPI.PutAllResponse.newBuilder().build());
  }

  // Read all of the entries out of the protobuf and return an error (without performing any puts)
  // if any of the entries can't be decoded
  private Map extractPutAllEntries(SerializationService serializationService,
      RegionAPI.PutAllRequest putAllRequest) {
    Map entries = new HashMap();
    try {
      for (BasicTypes.Entry entry : putAllRequest.getEntryList()) {
        Object decodedValue = ProtobufUtilities.decodeValue(serializationService, entry.getValue());
        Object decodedKey = ProtobufUtilities.decodeValue(serializationService, entry.getKey());

        entries.put(decodedKey, decodedValue);
      }
    } catch (UnsupportedEncodingTypeException ex) {
      throw new RuntimeException("This exception still needs to be handled in an ErrorMessage");
    } catch (CodecNotRegisteredForTypeException ex) {
      throw new RuntimeException("This exception still needs to be handled in an ErrorMessage");
    }

    return entries;
  }
}
