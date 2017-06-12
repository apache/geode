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
package org.apache.geode.protocol.operations.protobuf;

import org.apache.geode.ProtobufUtilities;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.protobuf.translation.EncodingTypeTranslator;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

public class GetRequestOperationHandler
    implements OperationHandler<RegionAPI.GetRequest, RegionAPI.GetResponse> {

  @Override
  public RegionAPI.GetResponse process(SerializationService serializationService,
      RegionAPI.GetRequest request, Cache cache)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    String regionName = request.getRegionName();
    BasicTypes.EncodedValue key = request.getKey();
    BasicTypes.EncodingType encodingType = key.getEncodingType();
    byte[] value = key.getValue().toByteArray();
    Object decodedValue = serializationService.decode(encodingType, value);

    Region region = cache.getRegion(regionName);
    Object resultValue = region.get(decodedValue);

    BasicTypes.EncodingType resultEncodingType =
        EncodingTypeTranslator.getEncodingTypeForObject(resultValue);
    byte[] resultEncodedValue = serializationService.encode(resultEncodingType, resultValue);

    return RegionAPI.GetResponse.newBuilder()
        .setResult(ProtobufUtilities.getEncodedValue(resultEncodingType, resultEncodedValue))
        .build();
  }

  @Override
  public int getOperationCode() {
    return ClientProtocol.Request.RequestAPICase.GETREQUEST.getNumber();
  }
}
