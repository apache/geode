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
package org.apache.geode.internal.cache.tier.sockets.command;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.transaction.Status;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.client.internal.TXSynchronizationOp;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;

public class TXSynchronizationCommandTest {
  private Message clientMessage;
  private ServerConnection serverConnection;
  private TXManagerImpl txManager;
  private TXStateProxyImpl txStateProxy;
  private TXId txId;
  private TXCommitMessage txCommitMessage;
  private InternalDistributedMember member;
  private Part part0;
  private Part part1;
  private Part part2;
  private RuntimeException exception;
  private TXSynchronizationCommand command;

  @Before
  public void setup() {
    clientMessage = mock(Message.class);
    serverConnection = mock(ServerConnection.class);
    txManager = mock(TXManagerImpl.class);
    member = mock(InternalDistributedMember.class);
    txStateProxy = mock(TXStateProxyImpl.class);
    txId = mock(TXId.class);
    txCommitMessage = mock(TXCommitMessage.class);
    part0 = mock(Part.class);
    part1 = mock(Part.class);
    part2 = mock(Part.class);
    exception = new RuntimeException();
    command = mock(TXSynchronizationCommand.class);

    when(clientMessage.getPart(0)).thenReturn(part0);
    when(clientMessage.getPart(1)).thenReturn(part1);
    when(clientMessage.getPart(2)).thenReturn(part2);
    doReturn(txManager).when(command).getTXManager(serverConnection);
    doReturn(member).when(command).getDistributedMember(serverConnection);
    when(txManager.getTXState()).thenReturn(txStateProxy);
    when(txStateProxy.getTxId()).thenReturn(txId);
  }

  @Test
  public void commandCanSendBackCommitMessageIfAlreadyCommitted() throws Exception {
    when(part0.getInt()).thenReturn(TXSynchronizationOp.CompletionType.AFTER_COMPLETION.ordinal());
    when(txManager.getRecentlyCompletedMessage(txId)).thenReturn(txCommitMessage);
    doNothing().when(command).writeCommitResponse(clientMessage, serverConnection, txCommitMessage);

    doCallRealMethod().when(command).cmdExecute(clientMessage, serverConnection, null, 1);
    command.cmdExecute(clientMessage, serverConnection, null, 1);

    verify(command, times(1)).writeCommitResponse(clientMessage, serverConnection, txCommitMessage);
    verify(serverConnection, times(1)).setAsTrue(Command.RESPONDED);
  }

  @Test
  public void commandCanInvokeBeforeCompletion() throws Exception {
    when(part0.getInt()).thenReturn(TXSynchronizationOp.CompletionType.BEFORE_COMPLETION.ordinal());

    doCallRealMethod().when(command).cmdExecute(clientMessage, serverConnection, null, 1);
    command.cmdExecute(clientMessage, serverConnection, null, 1);

    verify(txStateProxy, times(1)).beforeCompletion();
    verify(serverConnection, times(1)).setAsTrue(Command.RESPONDED);
  }

  @Test
  public void commandCanSendBackCommitMessageAfterInvokeAfterCompletion() throws Exception {
    when(part0.getInt()).thenReturn(TXSynchronizationOp.CompletionType.AFTER_COMPLETION.ordinal());
    when(part2.getInt()).thenReturn(Status.STATUS_COMMITTED);
    when(txStateProxy.getCommitMessage()).thenReturn(txCommitMessage);

    doCallRealMethod().when(command).cmdExecute(clientMessage, serverConnection, null, 1);
    command.cmdExecute(clientMessage, serverConnection, null, 1);

    verify(txStateProxy, times(1)).afterCompletion(Status.STATUS_COMMITTED);
    verify(command, times(1)).writeCommitResponse(clientMessage, serverConnection, txCommitMessage);
    verify(serverConnection, times(1)).setAsTrue(Command.RESPONDED);
  }
}
