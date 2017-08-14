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
import org.apache.geode.protocol.protobuf.utilities.ProtobufResponseUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

import com.fasterxml.jackson.databind.introspect.TypeResolutionContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.stream.Collectors;

public class PutAllRequestOperationHandler
    implements OperationHandler<RegionAPI.PutAllRequest, RegionAPI.PutAllResponse> {
  private static Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.PutAllResponse> process(SerializationService serializationService,
      RegionAPI.PutAllRequest putAllRequest, Cache cache) {
    Region region = cache.getRegion(putAllRequest.getRegionName());

    if (region == null) {
      return Failure.of(ProtobufResponseUtilities.createAndLogErrorResponse(
          ProtocolErrorCode.REGION_NOT_FOUND,
          "Region passed by client did not exist: " + putAllRequest.getRegionName(), logger, null));
    }

    RegionAPI.PutAllResponse.Builder builder = RegionAPI.PutAllResponse.newBuilder()
        .addAllFailedKeys(putAllRequest.getEntryList().stream()
            .map((entry) -> singlePut(serializationService, region, entry)).filter(Objects::nonNull)
            .collect(Collectors.toList()));
    return Success.of(builder.build());
  }

  private BasicTypes.KeyedErrorResponse singlePut(SerializationService serializationService,
      Region region, BasicTypes.Entry entry) {
    try {
      Object decodedValue = ProtobufUtilities.decodeValue(serializationService, entry.getValue());
      Object decodedKey = ProtobufUtilities.decodeValue(serializationService, entry.getKey());

      region.put(decodedKey, decodedValue);
    } catch (UnsupportedEncodingTypeException ex) {
      return buildAndLogKeyedError(entry, ProtocolErrorCode.VALUE_ENCODING_ERROR,
          "Encoding not supported", ex);
    } catch (CodecNotRegisteredForTypeException ex) {
      return buildAndLogKeyedError(entry, ProtocolErrorCode.VALUE_ENCODING_ERROR,
          "Codec error in protobuf deserialization", ex);
    } catch (ClassCastException ex) {
      return buildAndLogKeyedError(entry, ProtocolErrorCode.CONSTRAINT_VIOLATION,
          "Invalid key or value type for region", ex);
    }
    return null;
  }

  private BasicTypes.KeyedErrorResponse buildAndLogKeyedError(BasicTypes.Entry entry,
      ProtocolErrorCode errorCode, String message, Exception ex) {
    logger.error(message, ex);

    return BasicTypes.KeyedErrorResponse.newBuilder().setKey(entry.getKey())
        .setError(
            BasicTypes.Error.newBuilder().setErrorCode(errorCode.codeValue).setMessage(message))
        .build();
  }
}
