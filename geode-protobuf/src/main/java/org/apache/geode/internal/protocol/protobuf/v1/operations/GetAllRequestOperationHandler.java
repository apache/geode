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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.serialization.exception.EncodingException;

@Experimental
public class GetAllRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.GetAllRequest, RegionAPI.GetAllResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<RegionAPI.GetAllResponse, ClientProtocol.ErrorResponse> process(
      ProtobufSerializationService serializationService, RegionAPI.GetAllRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {
    String regionName = request.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);
    if (region == null) {
      logger.error("Received GetAll request for non-existing region {}", regionName);
      return Failure
          .of(ProtobufResponseUtilities.makeErrorResponse(SERVER_ERROR, "Region not found"));
    }

    Map<Boolean, List<Object>> resultsCollection = request.getKeyList().stream()
        .map((key) -> processOneMessage(serializationService, region, key))
        .collect(Collectors.partitioningBy(x -> x instanceof BasicTypes.Entry));
    RegionAPI.GetAllResponse.Builder responseBuilder = RegionAPI.GetAllResponse.newBuilder();

    for (Object entry : resultsCollection.get(true)) {
      responseBuilder.addEntries((BasicTypes.Entry) entry);
    }

    for (Object entry : resultsCollection.get(false)) {
      responseBuilder.addFailures((BasicTypes.KeyedError) entry);
    }

    return Success.of(responseBuilder.build());
  }

  private Object processOneMessage(ProtobufSerializationService serializationService, Region region,
      BasicTypes.EncodedValue key) {
    try {
      Object decodedKey = serializationService.decode(key);
      Object value = region.get(decodedKey);
      return ProtobufUtilities.createEntry(serializationService, decodedKey, value);
    } catch (EncodingException ex) {
      logger.error("Encoding not supported: {}", ex);
      return createKeyedError(key, "Encoding not supported.", INVALID_REQUEST);
    } catch (Exception ex) {
      logger.error("Failure in protobuf getAll operation for key: " + key, ex);
      return createKeyedError(key, ex.toString(), SERVER_ERROR);
    }
  }

  private Object createKeyedError(BasicTypes.EncodedValue key, String errorMessage,
      ProtocolErrorCode errorCode) {
    return BasicTypes.KeyedError.newBuilder().setKey(key).setError(BasicTypes.Error.newBuilder()
        .setErrorCode(ProtobufUtilities.getProtobufErrorCode(errorCode)).setMessage(errorMessage))
        .build();
  }
}
