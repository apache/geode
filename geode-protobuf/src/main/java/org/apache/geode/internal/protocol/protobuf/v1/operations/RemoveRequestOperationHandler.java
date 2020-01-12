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

import org.apache.geode.annotations.Experimental;
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

@Experimental
public class RemoveRequestOperationHandler
    implements ProtobufOperationHandler<RegionAPI.RemoveRequest, RegionAPI.RemoveResponse> {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Result<RegionAPI.RemoveResponse> process(ProtobufSerializationService serializationService,
      RegionAPI.RemoveRequest request, MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException {

    String regionName = request.getRegionName();

    Object decodedKey = serializationService.decode(request.getKey());
    if (decodedKey == null) {
      return Failure.of(BasicTypes.ErrorCode.INVALID_REQUEST,
          "NULL is not a valid key for removal.");
    }

    messageExecutionContext.getSecureCache().remove(regionName, decodedKey);

    return Success.of(RegionAPI.RemoveResponse.newBuilder().build());
  }
}
