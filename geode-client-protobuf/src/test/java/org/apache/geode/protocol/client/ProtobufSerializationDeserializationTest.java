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


import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.serialization.Deserializer;
import org.apache.geode.serialization.SerializationType;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.apache.geode.protocol.client.MessageUtils.makePutMessage;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;


@Category(UnitTest.class)
public class ProtobufSerializationDeserializationTest {
  @Test
  public void protobufSerializationSmokeTest() throws InvalidProtocolBufferException {
    ClientProtocol.Message message = makePutMessage();
    byte[] bytes = message.toByteArray();
    ClientProtocol.Message message1 = ClientProtocol.Message.parseFrom(bytes);
    assertEquals(message, message1);
  }

  /**
   * Given a serialized message that we've built, verify that the server part does the right call to
   * the Cache it gets passed.
   */
  @Test
  public void testNewClientProtocolPutsOnPutMessage() throws IOException {
    final String testRegion = "testRegion";
    final String testKey = "testKey";
    final String testValue = "testValue";
    ClientProtocol.Message message = MessageUtils.makePutMessageFor(testRegion, testKey, testValue);

    Deserializer deserializer = SerializationType.BYTE_BLOB.deserializer;
    Cache mockCache = Mockito.mock(Cache.class);
    Region mockRegion = Mockito.mock(Region.class);
    when(mockCache.getRegion("testRegion")).thenReturn(mockRegion);
    OutputStream mockOutputStream = Mockito.mock(OutputStream.class);

    ProtobufProtocolMessageHandler newClientProtocol = new ProtobufProtocolMessageHandler();
    newClientProtocol.receiveMessage(MessageUtils.loadMessageIntoInputStream(message),
        mockOutputStream, deserializer, mockCache);

    verify(mockRegion).put(testKey.getBytes(), testValue.getBytes());
  }

  @Test
  public void testServerRespondsToPutMessage() throws IOException {
    final String testRegion = "testRegion";
    final String testKey = "testKey";
    final String testValue = "testValue";
    ClientProtocol.Message message = MessageUtils.makePutMessageFor(testRegion, testKey, testValue);

    Deserializer deserializer = SerializationType.BYTE_BLOB.deserializer;
    Cache mockCache = Mockito.mock(Cache.class);
    Region mockRegion = Mockito.mock(Region.class);
    when(mockCache.getRegion("testRegion")).thenReturn(mockRegion);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(128);

    ProtobufProtocolMessageHandler newClientProtocol = new ProtobufProtocolMessageHandler();
    newClientProtocol.receiveMessage(MessageUtils.loadMessageIntoInputStream(message), outputStream,
        deserializer, mockCache);

    ClientProtocol.Message responseMessage = ClientProtocol.Message
        .parseDelimitedFrom(new ByteArrayInputStream(outputStream.toByteArray()));

    assertEquals(responseMessage.getMessageTypeCase(),
        ClientProtocol.Message.MessageTypeCase.RESPONSE);
    assertEquals(responseMessage.getResponse().getResponseAPICase(),
        ClientProtocol.Response.ResponseAPICase.PUTRESPONSE);
    assertTrue(responseMessage.getResponse().getPutResponse().getSuccess());
  }
}

