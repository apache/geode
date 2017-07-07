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
package org.apache.geode.protocol.serializer;

import org.apache.geode.protocol.MessageUtil;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ServiceLoader;

@Category(UnitTest.class)
public class ProtobufProtocolSerializerJUnitTest {
  private ProtocolSerializer<ClientProtocol.Message> protocolSerializer;

  @Before
  public void startup() {
    ServiceLoader<ProtocolSerializer> serviceLoader = ServiceLoader.load(ProtocolSerializer.class);
    for (ProtocolSerializer protocolSerializer : serviceLoader) {
      if (protocolSerializer instanceof ProtobufProtocolSerializer) {
        this.protocolSerializer = protocolSerializer;
      }
    }
  }

  @Test
  public void testDeserializeByteArrayToMessage()
      throws IOException, InvalidProtocolMessageException {
    ClientProtocol.Message expectedRequestMessage = MessageUtil.createGetRequestMessage();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    expectedRequestMessage.writeDelimitedTo(byteArrayOutputStream);
    InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

    ClientProtocol.Message actualMessage = protocolSerializer.deserialize(inputStream);
    Assert.assertEquals(expectedRequestMessage, actualMessage);
  }

  @Test(expected = InvalidProtocolMessageException.class)
  public void testDeserializeInvalidByteThrowsException()
      throws IOException, InvalidProtocolMessageException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.write("Some incorrect byte array".getBytes());
    InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    protocolSerializer.deserialize(inputStream);
  }

  @Test
  public void testSerializeMessageToByteArray() throws IOException {
    ClientProtocol.Message message = MessageUtil.createGetRequestMessage();
    ByteArrayOutputStream expectedByteArrayOutputStream = new ByteArrayOutputStream();
    message.writeDelimitedTo(expectedByteArrayOutputStream);
    byte[] expectedByteArray = expectedByteArrayOutputStream.toByteArray();

    ByteArrayOutputStream actualByteArrayOutputStream = new ByteArrayOutputStream();
    protocolSerializer.serialize(message, actualByteArrayOutputStream);
    Assert.assertArrayEquals(expectedByteArray, actualByteArrayOutputStream.toByteArray());
  }
}
