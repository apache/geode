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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.Failure;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.Result;
import org.apache.geode.internal.protocol.Success;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufResponseUtilities;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;

import static org.apache.geode.internal.protocol.ProtocolErrorCode.*;

@Experimental
public class GetAllRequestOperationHandler implements
    OperationHandler<RegionAPI.GetAllRequest, RegionAPI.GetAllResponse, ClientProtocol.ErrorResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<RegionAPI.GetAllResponse, ClientProtocol.ErrorResponse> process(
      SerializationService serializationService, RegionAPI.GetAllRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException {
    String regionName = request.getRegionName();
    Region region = messageExecutionContext.getCache().getRegion(regionName);
    if (region == null) {
      logger.error("Received GetAll request for non-existing region {}", regionName);
      return Failure
          .of(ProtobufResponseUtilities.makeErrorResponse(REGION_NOT_FOUND, "Region not found"));
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

  private Object processOneMessage(SerializationService serializationService, Region region,
      BasicTypes.EncodedValue key) {
    try {
      Object decodedKey = ProtobufUtilities.decodeValue(serializationService, key);
      Object value = region.get(decodedKey);
      return ProtobufUtilities.createEntry(serializationService, decodedKey, value);
    } catch (CodecNotRegisteredForTypeException | UnsupportedEncodingTypeException ex) {
      logger.error("Encoding not supported: {}", ex);
      return createKeyedError(key, "Encoding not supported.", VALUE_ENCODING_ERROR);
    } catch (org.apache.geode.distributed.LeaseExpiredException | TimeoutException e) {
      logger.error("Operation timed out: {}", e);
      return createKeyedError(key, "Operation timed out: " + e.getMessage(), OPERATION_TIMEOUT);
    } catch (CacheLoaderException | PartitionedRegionStorageException e) {
      logger.error("Data unreachable: {}", e);
      return createKeyedError(key, "Data unreachable: " + e.getMessage(), DATA_UNREACHABLE);
    } catch (NullPointerException | IllegalArgumentException e) {
      logger.error("Invalid input: {}", e);
      return createKeyedError(key, "Invalid input: " + e.getMessage(), CONSTRAINT_VIOLATION);
    }
  }

  private Object createKeyedError(BasicTypes.EncodedValue key, String errorMessage,
      ProtocolErrorCode errorCode) {
    return BasicTypes.KeyedError.newBuilder().setKey(key).setError(
        BasicTypes.Error.newBuilder().setErrorCode(errorCode.codeValue).setMessage(errorMessage))
        .build();
  }
}
