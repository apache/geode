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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class AbstractOpTest {
  private AbstractOp op;
  private Connection connection;
  private ServerLocation server;
  private Message message;

  @Before
  public void before() throws Exception {
    op = mock(AbstractOp.class);
    connection = mock(Connection.class);
    server = mock(ServerLocation.class);
    when(connection.getServer()).thenReturn(server);
    message = mock(Message.class);
  }

  @Test
  public void shouldBeMockable() throws Exception {
    Object mockObject = mock(Object.class);
    when(op.processObjResponse(any(), anyString())).thenReturn(mockObject);
    assertThat(op.processObjResponse(mock(Message.class), "string"))
        .isEqualTo(mockObject);
  }

  @Test(expected = IOException.class)
  public void processChunkedResponseShouldThrowIOExceptionWhenSocketBroken() throws Exception {
    ChunkedMessage msg = mock(ChunkedMessage.class);
    doCallRealMethod().when(op).processChunkedResponse(any(ChunkedMessage.class), anyString(),
        any());
    doNothing().when(msg).readHeader();
    when(msg.getMessageType()).thenReturn(MessageType.PING);
    op.processChunkedResponse(msg, "removeAll", null);
  }

  @Test
  public void invalidUserIdShouldThrowException() throws Exception {
    doCallRealMethod().when(op).sendMessage(any(Connection.class));
    when(server.getUserId()).thenReturn(-1L);
    when(server.getRequiresCredentials()).thenReturn(true);
    when(op.getMessage()).thenReturn(message);
    assertThatThrownBy(() -> op.sendMessage(connection))
        .isInstanceOf(ServerConnectivityException.class)
        .hasMessageContaining("Invalid userId");
  }
}
