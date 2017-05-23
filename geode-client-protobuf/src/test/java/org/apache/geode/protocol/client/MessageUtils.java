/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.protocol.client;

import static org.apache.geode.protocol.client.EncodingTypeThingyKt.serializerFromProtoEnum;

import com.google.protobuf.ByteString;

import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.RegionAPI;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class MessageUtils {
  public static ByteArrayInputStream loadMessageIntoInputStream(ClientProtocol.Message message)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    message.writeDelimitedTo(byteArrayOutputStream);
    byte[] messageByteArray = byteArrayOutputStream.toByteArray();
    return new ByteArrayInputStream(messageByteArray);
  }

  public static ClientProtocol.Message makeGetMessageFor(String region, String key) {
    Random random = new Random();
    ClientProtocol.MessageHeader.Builder messageHeader =
        ClientProtocol.MessageHeader.newBuilder().setCorrelationId(random.nextInt());

    BasicTypes.EncodedValue.Builder keyBuilder = getEncodedValueBuilder(key);

    RegionAPI.GetRequest.Builder getRequest =
        RegionAPI.GetRequest.newBuilder().setRegionName(region).setKey(keyBuilder);
    ClientProtocol.Request.Builder request =
        ClientProtocol.Request.newBuilder().setGetRequest(getRequest);

    return ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setRequest(request)
        .build();
  }

  public static ClientProtocol.Message makePutMessageFor(String region, Object key, Object value) {
    Random random = new Random();
    ClientProtocol.MessageHeader.Builder messageHeader =
        ClientProtocol.MessageHeader.newBuilder().setCorrelationId(random.nextInt());

    BasicTypes.EncodedValue.Builder keyBuilder = getEncodedValueBuilder(key);
    BasicTypes.EncodedValue.Builder valueBuilder = getEncodedValueBuilder(value);

    RegionAPI.PutRequest.Builder putRequestBuilder =
        RegionAPI.PutRequest.newBuilder().setRegionName(region)
            .setEntry(BasicTypes.Entry.newBuilder().setKey(keyBuilder).setValue(valueBuilder));

    ClientProtocol.Request.Builder request =
        ClientProtocol.Request.newBuilder().setPutRequest(putRequestBuilder);
    ClientProtocol.Message.Builder message =
        ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setRequest(request);

    return message.build();
  }

  private static BasicTypes.EncodedValue.Builder getEncodedValueBuilder(Object value) {
    BasicTypes.EncodingType encodingType = EncodingTypeThingyKt.getEncodingTypeForObjectKT(value);

    return BasicTypes.EncodedValue.newBuilder().setEncodingType(encodingType)
        .setValue(ByteString.copyFrom(serializerFromProtoEnum(encodingType).serialize(value)));
  }
}
