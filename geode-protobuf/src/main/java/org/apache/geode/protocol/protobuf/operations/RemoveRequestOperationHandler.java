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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.tier.sockets.ExecutionContext;
import org.apache.geode.internal.cache.tier.sockets.InvalidExecutionContextException;
import org.apache.geode.protocol.operations.OperationHandler;
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

@Experimental
public class RemoveRequestOperationHandler
    implements OperationHandler<RegionAPI.RemoveRequest, RegionAPI.RemoveResponse> {
  private static Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.RemoveResponse> process(SerializationService serializationService,
      RegionAPI.RemoveRequest request, ExecutionContext executionContext)
      throws InvalidExecutionContextException {

    String regionName = request.getRegionName();
    Region region = executionContext.getCache().getRegion(regionName);
    if (region == null) {
      return Failure.of(ProtobufResponseUtilities
          .makeErrorResponse(ProtocolErrorCode.REGION_NOT_FOUND.codeValue, "Region not found"));
    }

    try {
      Object decodedKey = ProtobufUtilities.decodeValue(serializationService, request.getKey());
      region.remove(decodedKey);

      return Success.of(RegionAPI.RemoveResponse.newBuilder().build());
    } catch (UnsupportedEncodingTypeException ex) {
      // can be thrown by encoding or decoding.
      return Failure.of(ProtobufResponseUtilities.createAndLogErrorResponse(
          ProtocolErrorCode.VALUE_ENCODING_ERROR, "Encoding not supported.", logger, ex));
    } catch (CodecNotRegisteredForTypeException ex) {
      return Failure.of(ProtobufResponseUtilities.createAndLogErrorResponse(
          ProtocolErrorCode.VALUE_ENCODING_ERROR, "Codec error in protobuf deserialization.",
          logger, ex));
    }
  }
}
