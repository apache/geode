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

import static org.apache.geode.internal.protocol.protobuf.v1.ProtobufErrorCode.SERVER_ERROR;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufResponseUtilities;

@Experimental
public class RemoveRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.RemoveRequest, RegionAPI.RemoveResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.RemoveResponse, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService, RegionAPI.RemoveRequest request,
      MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException {

    String regionName = request.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);
    if (region == null) {
      logger.error("Received Remove request for non-existing region {}", regionName);
      return Failure
          .of(ProtobufResponseUtilities.makeErrorResponse(SERVER_ERROR, "Region not found"));
    }

    long startTime = messageExecutionContext.getStatistics().startOperation();
    try {
      Object decodedKey = serializationService.decode(request.getKey());
      region.remove(decodedKey);

      return Success.of(RegionAPI.RemoveResponse.newBuilder().build());
    } finally {
      messageExecutionContext.getStatistics().endOperation(startTime);
    }
  }
}
