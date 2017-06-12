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
package org.apache.geode.protocol.handler;

import org.apache.geode.client.protocol.MessageUtil;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.handler.ProtocolHandler;
import org.apache.geode.protocol.handler.protobuf.ProtobufProtocolHandler;
import org.apache.geode.protocol.protobuf.ClientProtocol;
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
public class ProtobufProtocolHandlerJUnitTest {
  private ProtocolHandler<ClientProtocol.Message> protocolHandler;

  @Before
  public void startup() {
    ServiceLoader<ProtocolHandler> serviceLoader = ServiceLoader.load(ProtocolHandler.class);
    for (ProtocolHandler protocolHandler : serviceLoader) {
      if (protocolHandler instanceof ProtobufProtocolHandler) {
        this.protocolHandler = protocolHandler;
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

    ClientProtocol.Message actualMessage = protocolHandler.deserialize(inputStream);
    Assert.assertEquals(expectedRequestMessage, actualMessage);
  }

  @Test
  public void testDeserializeInvalidByteThrowsException() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.write("Some incorrect byte array".getBytes());
    InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

    boolean caughtException = false;
    try {
      protocolHandler.deserialize(inputStream);
    } catch (InvalidProtocolMessageException e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);
  }

  @Test
  public void testSerializeMessageToByteArray() throws IOException {
    ClientProtocol.Message message = MessageUtil.createGetRequestMessage();
    ByteArrayOutputStream expectedByteArrayOutputStream = new ByteArrayOutputStream();
    message.writeDelimitedTo(expectedByteArrayOutputStream);
    byte[] expectedByteArray = expectedByteArrayOutputStream.toByteArray();

    ByteArrayOutputStream actualByteArrayOutputStream = new ByteArrayOutputStream();
    protocolHandler.serialize(message, actualByteArrayOutputStream);
    Assert.assertArrayEquals(expectedByteArray, actualByteArrayOutputStream.toByteArray());
  }
}
