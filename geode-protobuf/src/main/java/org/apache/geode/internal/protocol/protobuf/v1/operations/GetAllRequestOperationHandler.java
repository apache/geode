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

import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.INVALID_REQUEST;
import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.SERVER_ERROR;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;

@Experimental
public class GetAllRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.GetAllRequest, RegionAPI.GetAllResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<RegionAPI.GetAllResponse> process(ProtobufSerializationService serializationService,
      RegionAPI.GetAllRequest request, MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException {
    String regionName = request.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);
    if (region == null) {
      logger.error("Received get-all request for nonexistent region: {}", regionName);
      return Failure.of(BasicTypes.ErrorCode.SERVER_ERROR,
          "Region \"" + regionName + "\" not found");
    }

    RegionAPI.GetAllResponse.Builder responseBuilder = RegionAPI.GetAllResponse.newBuilder();
    try {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(true);
      request.getKeyList().stream()
          .forEach((key) -> processSingleKey(responseBuilder, serializationService, region, key));
    } finally {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(false);
    }

    return Success.of(responseBuilder.build());
  }

  private void processSingleKey(RegionAPI.GetAllResponse.Builder responseBuilder,
      ProtobufSerializationService serializationService, Region region,
      BasicTypes.EncodedValue key) {
    try {

      Object decodedKey = serializationService.decode(key);
      if (decodedKey == null) {
        responseBuilder
            .addFailures(buildKeyedError(key, "NULL is not a valid key for get.", INVALID_REQUEST));
        return;
      }
      Object value = region.get(decodedKey);
      BasicTypes.Entry entry =
          ProtobufUtilities.createEntry(serializationService, decodedKey, value);
      responseBuilder.addEntries(entry);

    } catch (DecodingException ex) {
      logger.info("Key encoding not supported: {}", ex);
      responseBuilder
          .addFailures(buildKeyedError(key, "Key encoding not supported.", INVALID_REQUEST));
    } catch (EncodingException ex) {
      logger.info("Value encoding not supported: {}", ex);
      responseBuilder
          .addFailures(buildKeyedError(key, "Value encoding not supported.", INVALID_REQUEST));
    } catch (Exception ex) {
      logger.warn("Failure in protobuf getAll operation for key: " + key, ex);
      responseBuilder.addFailures(buildKeyedError(key, ex.toString(), SERVER_ERROR));
    }
  }

  private BasicTypes.KeyedError buildKeyedError(BasicTypes.EncodedValue key, String errorMessage,
      BasicTypes.ErrorCode errorCode) {
    return BasicTypes.KeyedError.newBuilder().setKey(key)
        .setError(BasicTypes.Error.newBuilder().setErrorCode(errorCode).setMessage(errorMessage))
        .build();
  }

}
