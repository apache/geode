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

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.security.NotAuthorizedException;

public class ServerToClientFunctionResultSenderJUnitTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  ServerConnection serverConnection;

  protected ServerToClientFunctionResultSender getResultSender() {
    ChunkedMessage msg = mock(ChunkedMessage.class);
    serverConnection = mock(ServerConnection.class);
    Function function = mock(Function.class);
    ExecuteFunctionOperationContext executeFunctionOperationContext =
        mock(ExecuteFunctionOperationContext.class);
    // sc.getAcceptor().isSelector();
    AcceptorImpl acceptor = mock(AcceptorImpl.class);
    when(serverConnection.getAcceptor()).thenReturn(acceptor);
    when(acceptor.isSelector()).thenReturn(true);
    when(acceptor.isRunning()).thenReturn(true);

    return new ServerToClientFunctionResultSender(msg, 1, serverConnection, function,
        executeFunctionOperationContext);
  }

  @Test
  public void whenLastResultReceivedIsSetThenLastResultMustReturnImmediately() throws IOException {
    ServerToClientFunctionResultSender resultSender = getResultSender();
    resultSender.lastResultReceived = true;

    Object object = mock(Object.class);
    DistributedMember memberId = mock(DistributedMember.class);
    resultSender.lastResult(object, memberId);
    verify(serverConnection, times(0)).getPostAuthzRequest();


    resultSender.lastResult(object);
    verify(serverConnection, times(0)).getPostAuthzRequest();

  }

  @Test
  public void whenExceptionOccursThenLastResultReceivedMustNotBeSet() throws Exception {
    ServerToClientFunctionResultSender resultSender = getResultSender();
    resultSender.ids = mock(InternalDistributedSystem.class);
    when(resultSender.ids.isDisconnecting()).thenReturn(false);
    Object object = mock(Object.class);
    resultSender.lastResultReceived = false;
    DistributedMember distributedMember = mock(DistributedMember.class);
    when(serverConnection.getPostAuthzRequest())
        .thenThrow(new NotAuthorizedException("Should catch this exception"));
    CachedRegionHelper cachedRegionHelper = mock(CachedRegionHelper.class);
    when(serverConnection.getCachedRegionHelper()).thenReturn(cachedRegionHelper);
    InternalCache cache = mock(InternalCache.class);
    when(cachedRegionHelper.getCache()).thenReturn(cache);
    when(cache.isClosed()).thenReturn(false);
    expectedException.expect(NotAuthorizedException.class);
    resultSender.lastResult(object, distributedMember);
    assertFalse(resultSender.lastResultReceived);

    resultSender.lastResult(object);
    assertFalse(resultSender.lastResultReceived);
  }

  @Test
  public void whenLastResultReceivedIsSetThenSendResultMustReturnImmediately() throws IOException {
    ServerToClientFunctionResultSender resultSender = getResultSender();
    resultSender.lastResultReceived = true;

    Object object = mock(Object.class);
    DistributedMember memberId = mock(DistributedMember.class);
    resultSender.sendResult(object, memberId);
    verify(serverConnection, times(0)).getPostAuthzRequest();


    resultSender.sendResult(object);
    verify(serverConnection, times(0)).getPostAuthzRequest();

  }

}
