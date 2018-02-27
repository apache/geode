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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.SerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;

@Experimental
public class PutAllRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.PutAllRequest, RegionAPI.PutAllResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.PutAllResponse> process(ProtobufSerializationService serializationService,
      RegionAPI.PutAllRequest putAllRequest, MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException {
    String regionName = putAllRequest.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);

    if (region == null) {
      logger.error("Received put-all request for nonexistent region: {}", regionName);
      return Failure.of(BasicTypes.ErrorCode.SERVER_ERROR,
          "Region \"" + regionName + "\" not found");
    }

    RegionAPI.PutAllResponse.Builder builder = RegionAPI.PutAllResponse.newBuilder();
    try {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(true);

      putAllRequest.getEntryList().stream()
          .forEach((entry) -> processSinglePut(builder, serializationService, region, entry));

    } finally {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(false);
    }
    return Success.of(builder.build());
  }

  private void processSinglePut(RegionAPI.PutAllResponse.Builder builder,
      SerializationService serializationService, Region region, BasicTypes.Entry entry) {
    try {
      Object decodedValue = serializationService.decode(entry.getValue());
      Object decodedKey = serializationService.decode(entry.getKey());
      if (decodedKey == null || decodedValue == null) {
        builder.addFailedKeys(
            buildKeyedError(entry, INVALID_REQUEST, "Key and value must both be non-NULL"));
      }
      region.put(decodedKey, decodedValue);

    } catch (DecodingException ex) {
      logger.info("Encoding not supported: " + ex);
      builder.addFailedKeys(this.buildKeyedError(entry, INVALID_REQUEST, "Encoding not supported"));
    } catch (ClassCastException ex) {
      builder.addFailedKeys(buildKeyedError(entry, SERVER_ERROR, ex.toString()));
    } catch (Exception ex) {
      logger.warn("Error processing putAll entry", ex);
      builder.addFailedKeys(buildKeyedError(entry, SERVER_ERROR, ex.toString()));
    }
  }

  private BasicTypes.KeyedError buildKeyedError(BasicTypes.Entry entry,
      BasicTypes.ErrorCode errorCode, String message) {
    return BasicTypes.KeyedError.newBuilder().setKey(entry.getKey())
        .setError(BasicTypes.Error.newBuilder().setErrorCode(errorCode).setMessage(message))
        .build();
  }
}
