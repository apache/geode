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
package org.apache.geode.internal.cache.execute;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;

public class ServerToClientFunctionResultSender65JUnitTest
    extends ServerToClientFunctionResultSenderJUnitTest {

  @Override
  protected ServerToClientFunctionResultSender getResultSender() {
    ChunkedMessage msg = mock(ChunkedMessage.class);
    serverConnection = mock(ServerConnection.class);
    Function function = mock(Function.class);
    ExecuteFunctionOperationContext executeFunctionOperationContext =
        mock(ExecuteFunctionOperationContext.class);
    AcceptorImpl acceptor = mock(AcceptorImpl.class);
    when(serverConnection.getAcceptor()).thenReturn(acceptor);
    when(acceptor.isSelector()).thenReturn(true);
    when(acceptor.isRunning()).thenReturn(true);
    return new ServerToClientFunctionResultSender65(msg, 1, serverConnection, function,
        executeFunctionOperationContext);
  }
}
