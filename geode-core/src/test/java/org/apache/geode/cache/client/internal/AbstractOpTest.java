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
package org.apache.geode.cache.client.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class AbstractOpTest {

  @Test
  public void shouldBeMockable() throws Exception {
    AbstractOp mockAbstractOp = mock(AbstractOp.class);
    Object mockObject = mock(Object.class);
    when(mockAbstractOp.processObjResponse(any(), anyString())).thenReturn(mockObject);
    assertThat(mockAbstractOp.processObjResponse(mock(Message.class), "string"))
        .isEqualTo(mockObject);
  }

  @Test(expected = IOException.class)
  public void processChunkedResponseShouldThrowIOExceptionWhenSocketBroken() throws Exception {
    ChunkedMessage msg = mock(ChunkedMessage.class);
    AbstractOp abstractOp = new AbstractOp(MessageType.PING, 0) {
      @Override
      protected Object processResponse(Message msg) throws Exception {
        return null;
      }

      @Override
      protected boolean isErrorResponse(int msgType) {
        return false;
      }

      @Override
      protected long startAttempt(ConnectionStats stats) {
        return 0;
      }

      @Override
      protected void endSendAttempt(ConnectionStats stats, long start) {

      }

      @Override
      protected void endAttempt(ConnectionStats stats, long start) {

      }
    };
    doNothing().when(msg).readHeader();
    when(msg.getMessageType()).thenReturn(MessageType.PING);
    abstractOp = spy(abstractOp);
    abstractOp.processChunkedResponse(msg, "removeAll", null);
  }
}
