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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.RegionAPI;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

public class MessageUtils {
  public static ByteArrayInputStream loadMessageIntoInputStream(ClientProtocol.Message message)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    message.writeDelimitedTo(byteArrayOutputStream);
    byte[] messageByteArray = byteArrayOutputStream.toByteArray();
    return new ByteArrayInputStream(messageByteArray);
  }

  public static ClientProtocol.Message makePutMessage() {
    Random random = new Random();
    ClientProtocol.MessageHeader.Builder messageHeader =
        ClientProtocol.MessageHeader.newBuilder().setCorrelationId(random.nextInt());

    BasicTypes.Key.Builder key =
        BasicTypes.Key.newBuilder().setKey(ByteString.copyFrom(createByteArrayOfSize(64)));

    BasicTypes.Value.Builder value =
        BasicTypes.Value.newBuilder().setValue(ByteString.copyFrom(createByteArrayOfSize(512)));

    RegionAPI.PutRequest.Builder putRequestBuilder =
        RegionAPI.PutRequest.newBuilder().setRegionName("TestRegion")
            .setEntry(BasicTypes.Entry.newBuilder().setKey(key).setValue(value));

    ClientProtocol.Request.Builder request =
        ClientProtocol.Request.newBuilder().setPutRequest(putRequestBuilder);

    ClientProtocol.Message.Builder message =
        ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setRequest(request);

    return message.build();
  }

  public static ClientProtocol.Message makePutMessageFor(String region, String key, String value) {
    Random random = new Random();
    ClientProtocol.MessageHeader.Builder messageHeader =
        ClientProtocol.MessageHeader.newBuilder().setCorrelationId(random.nextInt());

    BasicTypes.Key.Builder keyBuilder =
        BasicTypes.Key.newBuilder().setKey(ByteString.copyFromUtf8(key));

    BasicTypes.Value.Builder valueBuilder =
        BasicTypes.Value.newBuilder().setValue(ByteString.copyFromUtf8(value));

    RegionAPI.PutRequest.Builder putRequestBuilder =
        RegionAPI.PutRequest.newBuilder().setRegionName(region)
            .setEntry(BasicTypes.Entry.newBuilder().setKey(keyBuilder).setValue(valueBuilder));

    ClientProtocol.Request.Builder request =
        ClientProtocol.Request.newBuilder().setPutRequest(putRequestBuilder);
    ClientProtocol.Message.Builder message =
        ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setRequest(request);

    return message.build();
  }

  public static ClientProtocol.Message makeGetMessageFor(String region, String key) {
    Random random = new Random();
    ClientProtocol.MessageHeader.Builder messageHeader =
        ClientProtocol.MessageHeader.newBuilder().setCorrelationId(random.nextInt());

    BasicTypes.Key.Builder keyBuilder =
        BasicTypes.Key.newBuilder().setKey(ByteString.copyFromUtf8(key));

    RegionAPI.GetRequest.Builder getRequest =
        RegionAPI.GetRequest.newBuilder().setRegionName(region).setKey(keyBuilder);
    ClientProtocol.Request.Builder request =
        ClientProtocol.Request.newBuilder().setGetRequest(getRequest);

    return ClientProtocol.Message.newBuilder().setMessageHeader(messageHeader).setRequest(request)
        .build();
  }

  private static byte[] createByteArrayOfSize(int msgSize) {
    byte[] array = new byte[msgSize];
    for (int i = 0; i < msgSize; i++) {
      array[i] = 'a';
    }
    return array;
  }
}
