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
package org.apache.geode.internal.protocol;

import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;

public class MessageUtil {

  public static RegionAPI.GetRegionRequest makeGetRegionRequest(String requestRegion) {
    return RegionAPI.GetRegionRequest.newBuilder().setRegionName(requestRegion).build();
  }

  public static ClientProtocol.Message makeGetRegionRequestMessage(String requestRegion) {
    ClientProtocol.Request request = ClientProtocol.Request.newBuilder()
        .setGetRegionRequest(makeGetRegionRequest(requestRegion)).build();
    return ClientProtocol.Message.newBuilder().setRequest(request).build();
  }

  public static ClientProtocol.Message createGetRequestMessage() {
    ClientProtocol.Message.Builder messageBuilder = ClientProtocol.Message.newBuilder();
    ClientProtocol.Request.Builder requestBuilder = getRequestBuilder();
    requestBuilder.setGetRequest(getGetRequestBuilder());
    messageBuilder.setRequest(requestBuilder);
    return messageBuilder.build();
  }

  public static ClientProtocol.Message makePutRequestMessage(
      SerializationService serializationService, String requestKey, String requestValue,
      String requestRegion)
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException {
    BasicTypes.Entry entry = ProtobufUtilities.createEntry(
        ProtobufUtilities.createEncodedValue(serializationService, requestKey),
        ProtobufUtilities.createEncodedValue(serializationService, requestValue));

    ClientProtocol.Request request =
        ProtobufRequestUtilities.createPutRequest(requestRegion, entry);
    return ProtobufUtilities.createProtobufMessage(request);
  }

  public static ClientProtocol.Message makeGetRequestMessage(
      SerializationService serializationService, Object requestKey, String requestRegion)
      throws Exception {
    ClientProtocol.Request request = ProtobufRequestUtilities.createGetRequest(requestRegion,
        ProtobufUtilities.createEncodedValue(serializationService, requestKey));
    return ProtobufUtilities.createProtobufMessage(request);
  }

  private static ClientProtocol.Request.Builder getRequestBuilder() {
    return ClientProtocol.Request.newBuilder();
  }

  private static RegionAPI.GetRequest.Builder getGetRequestBuilder() {
    return RegionAPI.GetRequest.newBuilder();
  }
}
