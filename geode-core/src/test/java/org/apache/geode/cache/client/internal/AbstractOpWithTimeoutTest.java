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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.net.Socket;

import org.junit.Test;


public class AbstractOpWithTimeoutTest {

  private abstract static class AbstractTestOpWithTimeout extends AbstractOpWithTimeout {
    public AbstractTestOpWithTimeout(int msgType, int msgParts, int timeoutMs) {
      super(msgType, msgParts, timeoutMs);
    }

    @Override
    protected void attemptSend(ClientCacheConnection cnx) {}

    @Override
    protected Object attemptReadResponse(ClientCacheConnection cnx) {
      return null;
    }
  }

  @Test
  public void attemptWillSetAndResetSoTimeout() throws Exception {
    final AbstractOpWithTimeout mockOp = mock(AbstractTestOpWithTimeout.class,
        withSettings().useConstructor(0, 0, 123).defaultAnswer(CALLS_REAL_METHODS));

    final Socket socket = mock(Socket.class);
    when(socket.getSoTimeout()).thenReturn(456);

    final ClientCacheConnection connection = mock(ClientCacheConnection.class);
    when(connection.getSocket()).thenReturn(socket);

    mockOp.attempt(connection);

    verify(socket).getSoTimeout();
    verify(socket).setSoTimeout(123);
    verify(socket).setSoTimeout(456);
    verifyNoMoreInteractions(socket);
  }

  @Test
  public void attemptWontSetAndResetSoTimeout() throws Exception {
    final AbstractOpWithTimeout mockOp = mock(AbstractTestOpWithTimeout.class,
        withSettings().useConstructor(0, 0, 123).defaultAnswer(CALLS_REAL_METHODS));

    final Socket socket = mock(Socket.class);
    when(socket.getSoTimeout()).thenReturn(123);

    final ClientCacheConnection connection = mock(ClientCacheConnection.class);
    when(connection.getSocket()).thenReturn(socket);

    mockOp.attempt(connection);

    verify(socket).getSoTimeout();
    verifyNoMoreInteractions(socket);
  }

  @Test
  public void getTimeoutMs() {
    final AbstractOpWithTimeout mockOp = mock(AbstractOpWithTimeout.class,
        withSettings().useConstructor(0, 0, 123).defaultAnswer(CALLS_REAL_METHODS));

    assertThat(mockOp.getTimeoutMs()).isEqualTo(123);
  }
}
