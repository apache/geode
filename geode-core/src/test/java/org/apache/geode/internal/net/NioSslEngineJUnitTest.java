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

package org.apache.geode.internal.net;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DMStats;

public class NioSslEngineJUnitTest {
  SSLEngine engine;
  SSLSession session;

  @Before
  public void setup() {
    engine = mock(SSLEngine.class);
    session = mock(SSLSession.class);
    when(engine.getSession()).thenReturn(session);
    when(session.getApplicationBufferSize()).thenReturn(100);
    when(session.getPacketBufferSize()).thenReturn(100);
  }

  @Test
  public void newBuffer() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(256);
    buffer.clear();
    for (int i = 0; i < 256; i++) {
      byte b = (byte) (i & 0xff);
      buffer.put(b);
    }
    createAndVerifyNewBuffer(buffer, false);

    createAndVerifyNewBuffer(buffer, true);
  }

  private void createAndVerifyNewBuffer(ByteBuffer buffer, boolean useDirectBuffer) {
    ByteBuffer newBuffer = new NioSslEngine(mock(SocketChannel.class), engine,
        mock(DMStats.class)).expandBuffer(NioSslEngine.BufferType.UNTRACKED, buffer, 500);
    assertEquals(buffer.position(), newBuffer.position());
    assertEquals(500, newBuffer.capacity());
    newBuffer.flip();
    for (int i = 0; i < 256; i++) {
      byte expected = (byte) (i & 0xff);
      byte actual = (byte) (newBuffer.get() & 0xff);
      assertEquals(expected, actual);
    }
  }
}
