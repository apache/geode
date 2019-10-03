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

import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.SERVER_ERROR;

import java.util.Collection;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.security.SecureCache;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.logging.internal.LogService;

@Experimental
public class GetAllRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.GetAllRequest, RegionAPI.GetAllResponse> {
  private static final Logger logger = LogService.getLogger();

  @Override
  public Result<RegionAPI.GetAllResponse> process(ProtobufSerializationService serializationService,
      RegionAPI.GetAllRequest request, MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException, ConnectionStateException {
    String regionName = request.getRegionName();

    RegionAPI.GetAllResponse.Builder responseBuilder = RegionAPI.GetAllResponse.newBuilder();
    SecureCache cache = messageExecutionContext.getSecureCache();
    Collection<Object> keys = serializationService.decodeList(request.getKeyList());
    cache.getAll(regionName, keys,
        (key, value) -> addEntry(serializationService, responseBuilder, key, value),
        (key, exception) -> addException(serializationService, responseBuilder, key, exception));

    return Success.of(responseBuilder.build());
  }

  private void addException(ProtobufSerializationService serializationService,
      RegionAPI.GetAllResponse.Builder responseBuilder, Object key, Object exception) {
    logger.warn("Failure in protobuf getAll operation for key: " + key, exception);
    BasicTypes.EncodedValue encodedKey = serializationService.encode(key);
    BasicTypes.KeyedError failure = BasicTypes.KeyedError.newBuilder().setKey(encodedKey).setError(
        BasicTypes.Error.newBuilder().setErrorCode(SERVER_ERROR).setMessage(exception.toString()))
        .build();
    responseBuilder.addFailures(failure);
  }

  private void addEntry(ProtobufSerializationService serializationService,
      RegionAPI.GetAllResponse.Builder responseBuilder, Object key, Object value) {
    BasicTypes.Entry entry = ProtobufUtilities.createEntry(serializationService, key, value);
    responseBuilder.addEntries(entry);
  }
}
