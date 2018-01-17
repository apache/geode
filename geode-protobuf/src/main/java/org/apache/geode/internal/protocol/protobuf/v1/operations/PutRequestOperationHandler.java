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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.apache.geode.internal.protocol.ProtocolErrorCode.INVALID_REQUEST;
import static org.apache.geode.internal.protocol.ProtocolErrorCode.SERVER_ERROR;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.serialization.exception.EncodingException;

@Experimental
public class PutRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.PutRequest, RegionAPI.PutResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<RegionAPI.PutResponse, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService, RegionAPI.PutRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {
    String regionName = request.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);
    if (region == null) {
      logger.warn("Received Put request for non-existing region: {}", regionName);
      return Failure.of(ProtobufResponseUtilities.makeErrorResponse(SERVER_ERROR,
          "Region passed by client did not exist: " + regionName));
    }

    long startTime = messageExecutionContext.getStatistics().startOperation();
    try {
      BasicTypes.Entry entry = request.getEntry();

      Object decodedValue = serializationService.decode(entry.getValue());
      Object decodedKey = serializationService.decode(entry.getKey());
      try {
        region.put(decodedKey, decodedValue);
        return Success.of(RegionAPI.PutResponse.newBuilder().build());
      } catch (ClassCastException ex) {
        logger.error("Received Put request with invalid key type: {}", ex);
        return Failure.of(ProtobufResponseUtilities.makeErrorResponse(SERVER_ERROR, ex.toString()));
      }
    } catch (EncodingException ex) {
      logger.error("Got error when decoding Put request: {}", ex);
      return Failure
          .of(ProtobufResponseUtilities.makeErrorResponse(INVALID_REQUEST, ex.toString()));
    } finally {
      messageExecutionContext.getStatistics().endOperation(startTime);
    }
  }
}
