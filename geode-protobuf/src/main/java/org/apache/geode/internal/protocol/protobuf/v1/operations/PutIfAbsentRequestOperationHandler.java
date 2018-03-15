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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.security.ResourcePermission;

public class PutIfAbsentRequestOperationHandler implements
    ProtobufOperationHandler<RegionAPI.PutIfAbsentRequest, RegionAPI.PutIfAbsentResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.PutIfAbsentResponse> process(
      ProtobufSerializationService serializationService, RegionAPI.PutIfAbsentRequest request,
      MessageExecutionContext messageExecutionContext) throws InvalidExecutionContextException,
      ConnectionStateException, EncodingException, DecodingException {

    final String regionName = request.getRegionName();

    Region<Object, Object> region;
    try {
      region = messageExecutionContext.getCache().getRegion(regionName);
    } catch (IllegalArgumentException ex) {
      return Failure.of(BasicTypes.ErrorCode.INVALID_REQUEST,
          "Invalid region name: \"" + regionName + "\"");
    }

    if (region == null) {
      logger.error("Received PutIfAbsentRequest for nonexistent region: {}", regionName);
      return Failure.of(BasicTypes.ErrorCode.SERVER_ERROR,
          "Region \"" + regionName + "\" not found");
    }

    final BasicTypes.Entry entry = request.getEntry();

    Object decodedValue = serializationService.decode(entry.getValue());
    Object decodedKey = serializationService.decode(entry.getKey());

    if (decodedKey == null || decodedValue == null) {
      return Failure.of(BasicTypes.ErrorCode.INVALID_REQUEST,
          "Key and value must both be non-NULL");
    }

    final Object oldValue = region.putIfAbsent(decodedKey, decodedValue);

    return Success.of(RegionAPI.PutIfAbsentResponse.newBuilder()
        .setOldValue(serializationService.encode(oldValue)).build());
  }

  public static ResourcePermission determineRequiredPermission(RegionAPI.PutIfAbsentRequest request,
      ProtobufSerializationService serializer) throws DecodingException {
    return new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.WRITE, request.getRegionName(),
        serializer.decode(request.getEntry().getKey()).toString());
  }
}
