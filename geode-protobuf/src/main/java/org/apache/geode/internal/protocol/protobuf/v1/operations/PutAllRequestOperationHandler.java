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

import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.AUTHORIZATION_FAILED;
import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.INVALID_REQUEST;
import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.SERVER_ERROR;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
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
import org.apache.geode.security.NotAuthorizedException;

@Experimental
public class PutAllRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.PutAllRequest, RegionAPI.PutAllResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.PutAllResponse> process(ProtobufSerializationService serializationService,
      RegionAPI.PutAllRequest putAllRequest, MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException {
    String regionName = putAllRequest.getRegionName();

    RegionAPI.PutAllResponse.Builder builder = RegionAPI.PutAllResponse.newBuilder();
    SecureCache cache = messageExecutionContext.getSecureCache();
    Map<Object, Object> entries = new HashMap<>(putAllRequest.getEntryList().size());

    putAllRequest.getEntryList()
        .forEach(entry -> entries.put(serializationService.decode(entry.getKey()),
            serializationService.decode(entry.getValue())));
    cache.putAll(regionName, entries,
        (key, exception) -> addError(builder, serializationService.encode(key), exception));


    return Success.of(builder.build());
  }

  private void addError(RegionAPI.PutAllResponse.Builder builder, BasicTypes.EncodedValue key,
      Exception exception) {

    BasicTypes.ErrorCode errorCode;
    if (exception instanceof NotAuthorizedException) {
      errorCode = AUTHORIZATION_FAILED;
    } else if (exception instanceof DecodingException) {
      errorCode = INVALID_REQUEST;
    } else {
      errorCode = SERVER_ERROR;
    }

    builder.addFailedKeys(BasicTypes.KeyedError.newBuilder().setKey(key).setError(
        BasicTypes.Error.newBuilder().setErrorCode(errorCode).setMessage(exception.toString())));
  }
}
