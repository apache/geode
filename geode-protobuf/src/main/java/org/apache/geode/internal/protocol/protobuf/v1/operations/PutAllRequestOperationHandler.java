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
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufErrorCode;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.SerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;

@Experimental
public class PutAllRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.PutAllRequest, RegionAPI.PutAllResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.PutAllResponse, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService, RegionAPI.PutAllRequest putAllRequest,
      MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException {
    String regionName = putAllRequest.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);

    if (region == null) {
      logger.error("Received PutAll request for non-existing region {}", regionName);
      return Failure.of(ProtobufResponseUtilities.makeErrorResponse(SERVER_ERROR,
          "Region passed does not exist: " + regionName));
    }

    long startTime = messageExecutionContext.getStatistics().startOperation();
    RegionAPI.PutAllResponse.Builder builder = RegionAPI.PutAllResponse.newBuilder();
    try {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(true);

      for (BasicTypes.Entry entry : putAllRequest.getEntryList()) {
        BasicTypes.KeyedError error = singlePut(serializationService, region, entry);
        if (error != null) {
          builder.addFailedKeys(error);
        }
      }

    } finally {
      messageExecutionContext.getStatistics().endOperation(startTime);
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(false);
    }
    return Success.of(builder.build());
  }

  private BasicTypes.KeyedError singlePut(SerializationService serializationService, Region region,
      BasicTypes.Entry entry) throws DecodingException {
    try {
      Object decodedValue = serializationService.decode(entry.getValue());
      Object decodedKey = serializationService.decode(entry.getKey());

      region.put(decodedKey, decodedValue);
    } catch (ClassCastException ex) {
      return buildAndLogKeyedError(entry, SERVER_ERROR, ex.toString(), ex);
    }
    return null;
  }

  private BasicTypes.KeyedError buildAndLogKeyedError(BasicTypes.Entry entry,
      ProtobufErrorCode errorCode, String message, Exception ex) {
    logger.error(message, ex);

    return BasicTypes.KeyedError.newBuilder().setKey(entry.getKey())
        .setError(BasicTypes.Error.newBuilder()
            .setErrorCode(ProtobufUtilities.getProtobufErrorCode(errorCode)).setMessage(message))
        .build();
  }
}
