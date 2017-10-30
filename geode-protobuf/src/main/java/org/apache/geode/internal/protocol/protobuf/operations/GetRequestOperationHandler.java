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

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.Failure;
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
public class GetRequestOperationHandler implements
    OperationHandler<RegionAPI.GetRequest, RegionAPI.GetResponse, ClientProtocol.ErrorResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<RegionAPI.GetResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, RegionAPI.GetRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {
    String regionName = request.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);
    if (region == null) {
      logger.error("Received Get request for non-existing region {}", regionName);
      return Failure
          .of(ProtobufResponseUtilities.makeErrorResponse(REGION_NOT_FOUND, "Region not found"));
    }

    try {
      Object decodedKey = ProtobufUtilities.decodeValue(serializationService, request.getKey());
      Object resultValue = region.get(decodedKey);

      if (resultValue == null) {
        return Success.of(RegionAPI.GetResponse.newBuilder().build());
      }

      BasicTypes.EncodedValue encodedValue =
          ProtobufUtilities.createEncodedValue(serializationService, resultValue);
      return Success.of(RegionAPI.GetResponse.newBuilder().setResult(encodedValue).build());
    } catch (UnsupportedEncodingTypeException ex) {
      logger.error("Received Get request with unsupported encoding: {}", ex);
      return Failure.of(ProtobufResponseUtilities.makeErrorResponse(VALUE_ENCODING_ERROR,
          "Encoding not supported."));
    } catch (CodecNotRegisteredForTypeException ex) {
      logger.error("Got codec error when decoding Get request: {}", ex);
      return Failure.of(ProtobufResponseUtilities.makeErrorResponse(VALUE_ENCODING_ERROR,
          "Codec error in protobuf deserialization."));
    }
  }
}
