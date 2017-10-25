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
package org.apache.geode.internal.protocol.protobuf.operations;

import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;

import static org.apache.geode.internal.protocol.ProtocolErrorCode.*;

@Experimental
public class PutAllRequestOperationHandler implements
    OperationHandler<RegionAPI.PutAllRequest, RegionAPI.PutAllResponse, ClientProtocol.ErrorResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.PutAllResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, RegionAPI.PutAllRequest putAllRequest,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {
    String regionName = putAllRequest.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);

    if (region == null) {
      logger.error("Received PutAll request for non-existing region {}", regionName);
      return Failure.of(ProtobufResponseUtilities.makeErrorResponse(REGION_NOT_FOUND,
          "Region passed does not exist: " + regionName));
    }

    RegionAPI.PutAllResponse.Builder builder = RegionAPI.PutAllResponse.newBuilder()
        .addAllFailedKeys(putAllRequest.getEntryList().stream()
            .map((entry) -> singlePut(serializationService, region, entry)).filter(Objects::nonNull)
            .collect(Collectors.toList()));
    return Success.of(builder.build());
  }

  private BasicTypes.KeyedError singlePut(SerializationService serializationService, Region region,
      BasicTypes.Entry entry) {
    try {
      Object decodedValue = ProtobufUtilities.decodeValue(serializationService, entry.getValue());
      Object decodedKey = ProtobufUtilities.decodeValue(serializationService, entry.getKey());

      region.put(decodedKey, decodedValue);
    } catch (UnsupportedEncodingTypeException ex) {
      return buildAndLogKeyedError(entry, VALUE_ENCODING_ERROR, "Encoding not supported", ex);
    } catch (CodecNotRegisteredForTypeException ex) {
      return buildAndLogKeyedError(entry, VALUE_ENCODING_ERROR,
          "Codec error in protobuf deserialization", ex);
    } catch (ClassCastException ex) {
      return buildAndLogKeyedError(entry, CONSTRAINT_VIOLATION,
          "Invalid key or value type for region", ex);
    }
    return null;
  }

  private BasicTypes.KeyedError buildAndLogKeyedError(BasicTypes.Entry entry,
      ProtocolErrorCode errorCode, String message, Exception ex) {
    logger.error(message, ex);

    return BasicTypes.KeyedError.newBuilder().setKey(entry.getKey())
        .setError(
            BasicTypes.Error.newBuilder().setErrorCode(errorCode.codeValue).setMessage(message))
        .build();
  }
}
