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

import org.apache.geode.cache.Region;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.security.ResourcePermission;

public class PutIfAbsentRequestOperationHandler extends
    AbstractRegionRequestOperationHandler<RegionAPI.PutIfAbsentRequest, RegionAPI.PutIfAbsentResponse> {
  @Override
  protected Result<RegionAPI.PutIfAbsentResponse> doOp(
      ProtobufSerializationService serializationService, RegionAPI.PutIfAbsentRequest request,
      Region<Object, Object> region) {
    final BasicTypes.Entry entry = request.getEntry();

    Object decodedValue = serializationService.decode(entry.getValue());
    Object decodedKey = serializationService.decode(entry.getKey());

    final Object oldValue = region.putIfAbsent(decodedKey, decodedValue);

    return Success.of(RegionAPI.PutIfAbsentResponse.newBuilder()
        .setOldValue(serializationService.encode(oldValue)).build());
  }

  @Override
  protected String getRegionName(RegionAPI.PutIfAbsentRequest request) {
    return request.getRegionName();
  }

  public static ResourcePermission determineRequiredPermission(RegionAPI.PutIfAbsentRequest request,
      ProtobufSerializationService serializer) throws DecodingException {
    return new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.WRITE, request.getRegionName(),
        serializer.decode(request.getEntry().getKey()).toString());
  }
}
