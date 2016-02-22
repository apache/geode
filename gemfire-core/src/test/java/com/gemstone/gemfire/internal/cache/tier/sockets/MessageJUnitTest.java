/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.net.Socket;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.offheap.HeapByteBufferMemoryChunkJUnitTest;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class MessageJUnitTest {

  Message message;
  Socket mockSocket;
  MessageStats mockStats;
  ByteBuffer msgBuffer;
  ServerConnection mockServerConnection;
  
  @Before
  public void setUp() throws Exception {
    mockSocket = mock(Socket.class);
    message = new Message(2, Version.CURRENT);
    assertEquals(2, message.getNumberOfParts());
    mockStats = mock(MessageStats.class);
    msgBuffer = ByteBuffer.allocate(1000);
    mockServerConnection = mock(ServerConnection.class);
    message.setComms(mockServerConnection, mockSocket, msgBuffer, mockStats);
  }

  @Test
  public void clearDoesNotThrowNPE() throws Exception{
    // unsetComms clears the message's ByteBuffer, which was causing an NPE during shutdown
    // when clear() was invoked
    message.unsetComms();
    message.clear();
  }
  
  @Test
  public void numberOfPartsIsAdjusted() {
    int numParts = message.getNumberOfParts();
    message.setNumberOfParts(2*numParts+1);
    assertEquals(2*numParts+1, message.getNumberOfParts());
    message.addBytesPart(new byte[1]);
    message.addIntPart(2);
    message.addLongPart(3);
    message.addObjPart("4");
    message.addStringPart("5");
    assertEquals(5, message.getNextPartNumber());
  }
  
  @Test
  public void messageLongerThanMaxIntIsRejected() throws Exception {
    Part[] parts = new Part[2];
    Part mockPart1 = mock(Part.class);
    when(mockPart1.getLength()).thenReturn(Integer.MAX_VALUE/2);
    parts[0] = mockPart1;
    parts[1] = mockPart1;
    message.setParts(parts);
    try {
      message.send();
    } catch (MessageTooLargeException e) {
      assertTrue(e.getMessage().contains("exceeds maximum integer value"));
      return;
    }
    fail("expected an exception but none was thrown");
  }
  
  @Test
  public void maxMessageSizeIsRespected() throws Exception {
    Part[] parts = new Part[2];
    Part mockPart1 = mock(Part.class);
    when(mockPart1.getLength()).thenReturn(Message.MAX_MESSAGE_SIZE/2);
    parts[0] = mockPart1;
    parts[1] = mockPart1;
    message.setParts(parts);
    try {
      message.send();
    } catch (MessageTooLargeException e) {
      assertFalse(e.getMessage().contains("exceeds maximum integer value"));
      return;
    }
    fail("expected an exception but none was thrown");
  }
  
  
  // TODO many more tests are needed

}
