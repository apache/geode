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
package org.apache.geode.internal.cache.tier.sockets;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.geode.test.junit.categories.ClientServerTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.Version;
import org.apache.geode.test.junit.categories.UnitTest;

@Category({UnitTest.class, ClientServerTest.class})
public class MessageJUnitTest {

  private Message message;

  @Before
  public void setUp() throws Exception {
    Socket mockSocket = mock(Socket.class);
    this.message = new Message(2, Version.CURRENT);
    assertEquals(2, this.message.getNumberOfParts());
    MessageStats mockStats = mock(MessageStats.class);
    ByteBuffer msgBuffer = ByteBuffer.allocate(1000);
    ServerConnection mockServerConnection = mock(ServerConnection.class);
    this.message.setComms(mockServerConnection, mockSocket, msgBuffer, mockStats);
  }

  @Test
  public void clearDoesNotThrowNPE() throws Exception {
    // unsetComms clears the message's ByteBuffer, which was causing an NPE during shutdown
    // when clear() was invoked
    this.message.unsetComms();
    this.message.clear();
  }

  @Test
  public void numberOfPartsIsAdjusted() {
    int numParts = this.message.getNumberOfParts();
    this.message.setNumberOfParts(2 * numParts + 1);
    assertEquals(2 * numParts + 1, this.message.getNumberOfParts());
    this.message.addBytesPart(new byte[1]);
    this.message.addIntPart(2);
    this.message.addLongPart(3);
    this.message.addObjPart("4");
    this.message.addStringPart("5");
    assertEquals(5, this.message.getNextPartNumber());
  }

  @Test
  public void messageLongerThanMaxIntIsRejected() throws Exception {
    Part mockPart1 = mock(Part.class);
    when(mockPart1.getLength()).thenReturn(Integer.MAX_VALUE / 2);
    Part[] parts = new Part[2];
    parts[0] = mockPart1;
    parts[1] = mockPart1;
    this.message.setParts(parts);
    try {
      this.message.send();
      fail("expected an exception but none was thrown");
    } catch (MessageTooLargeException e) {
      assertTrue(e.getMessage().contains("exceeds maximum integer value"));
    }
  }

  @Test
  public void maxMessageSizeIsRespected() throws Exception {
    Part mockPart1 = mock(Part.class);
    when(mockPart1.getLength()).thenReturn(Message.DEFAULT_MAX_MESSAGE_SIZE / 2);
    Part[] parts = new Part[2];
    parts[0] = mockPart1;
    parts[1] = mockPart1;
    this.message.setParts(parts);
    try {
      this.message.send();
      fail("expected an exception but none was thrown");
    } catch (MessageTooLargeException e) {
      assertFalse(e.getMessage().contains("exceeds maximum integer value"));
    }
  }

  /**
   * geode-1468: Message should clear the chunks in its Parts when performing cleanup.
   */
  @Test
  public void streamBuffersAreClearedDuringCleanup() throws Exception {
    Part mockPart1 = mock(Part.class);
    when(mockPart1.getLength()).thenReturn(100);
    Part[] parts = new Part[2];
    parts[0] = mockPart1;
    parts[1] = mockPart1;
    this.message.setParts(parts);
    this.message.clearParts();
    verify(mockPart1, times(2)).clear();
  }

}
