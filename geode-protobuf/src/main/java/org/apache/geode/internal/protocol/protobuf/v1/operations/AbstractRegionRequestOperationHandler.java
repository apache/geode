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

/**
 * An abstract base class for requests that operate on a Region. It contains the common logic for
 * getting a region from a region name, and the operation logic is implemented by child classes.
 */
public abstract class AbstractRegionRequestOperationHandler<RequestType, ResponseType>
    implements ProtobufOperationHandler<RequestType, ResponseType> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public final Result<ResponseType> process(ProtobufSerializationService serializationService,
      RequestType request, MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, EncodingException, DecodingException {

    final String regionName = getRegionName(request);

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

    try {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(true);
      return doOp(serializationService, request, region);
    } finally {
      messageExecutionContext.getCache().setReadSerializedForCurrentThread(false);
    }
  }

  protected abstract Result<ResponseType> doOp(ProtobufSerializationService serializationService,
      RequestType request, Region<Object, Object> region);

  protected abstract String getRegionName(RequestType request);
}
