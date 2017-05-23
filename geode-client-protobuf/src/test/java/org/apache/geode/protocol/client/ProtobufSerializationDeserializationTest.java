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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;


@Category(UnitTest.class)
public class ProtobufSerializationDeserializationTest {

  private Cache mockCache;
  private Region mockRegion;
  private final String testRegion = "testRegion";
  private final String testKey = "testKey";
  private final String testValue = "testValue";

  @Before
  public void start() {
    mockCache = Mockito.mock(Cache.class);
    mockRegion = Mockito.mock(Region.class);
    when(mockCache.getRegion(testRegion)).thenReturn(mockRegion);

  }

  /**
   * Given a serialized message that we've built, verify that the server part does the right call to
   * the Cache it gets passed.
   */
  @Test
  public void testNewClientProtocolPutsOnPutMessage() throws IOException {
    ClientProtocol.Message message = MessageUtils.INSTANCE
        .makePutMessageFor(testRegion, testKey, testValue);

    OutputStream mockOutputStream = Mockito.mock(OutputStream.class);

    ProtobufProtocolMessageHandler newClientProtocol = new ProtobufProtocolMessageHandler();
    newClientProtocol.receiveMessage(MessageUtils.INSTANCE.loadMessageIntoInputStream(message),
        mockOutputStream, mockCache);

    verify(mockRegion).put(testKey.getBytes(), testValue.getBytes());
  }

  @Test
  public void testServerRespondsToPutMessage() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(128);
    ClientProtocol.Message message = MessageUtils.INSTANCE
        .makePutMessageFor(testRegion, testKey, testValue);

    ProtobufProtocolMessageHandler newClientProtocol = new ProtobufProtocolMessageHandler();
    newClientProtocol.receiveMessage(MessageUtils.INSTANCE.loadMessageIntoInputStream(message), outputStream,
        mockCache);

    ClientProtocol.Message responseMessage = ClientProtocol.Message
        .parseDelimitedFrom(new ByteArrayInputStream(outputStream.toByteArray()));

    assertEquals(responseMessage.getMessageTypeCase(),
        ClientProtocol.Message.MessageTypeCase.RESPONSE);
    assertEquals(responseMessage.getResponse().getResponseAPICase(),
        ClientProtocol.Response.ResponseAPICase.PUTRESPONSE);
    assertTrue(responseMessage.getResponse().getPutResponse().getSuccess());
  }
}

