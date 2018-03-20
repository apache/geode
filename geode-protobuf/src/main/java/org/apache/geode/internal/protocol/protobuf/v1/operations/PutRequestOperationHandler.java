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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class PutRequestOperationHandler
    extends AbstractRegionRequestOperationHandler<RegionAPI.PutRequest, RegionAPI.PutResponse> {
  @Override
  public Result<RegionAPI.PutResponse> doOp(ProtobufSerializationService serializationService,
      RegionAPI.PutRequest request, Region<Object, Object> region) throws DecodingException {

    BasicTypes.Entry entry = request.getEntry();

    Object decodedValue = serializationService.decode(entry.getValue());
    Object decodedKey = serializationService.decode(entry.getKey());
    if (decodedKey == null || decodedValue == null) {
      return Failure.of(BasicTypes.ErrorCode.INVALID_REQUEST,
          "Key and value must both be non-NULL");
    }

    region.put(decodedKey, decodedValue);
    return Success.of(RegionAPI.PutResponse.newBuilder().build());
  }

  @Override
  protected String getRegionName(RegionAPI.PutRequest request) {
    return request.getRegionName();
  }

  public static ResourcePermission determineRequiredPermission(RegionAPI.PutRequest request,
      ProtobufSerializationService serializer) throws DecodingException {
    return new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.WRITE, request.getRegionName(),
        serializer.decode(request.getEntry().getKey()).toString());
  }
}
