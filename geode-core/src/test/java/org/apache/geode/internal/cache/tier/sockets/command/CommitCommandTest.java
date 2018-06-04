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

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.TransactionInDoubtException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Exposes GEODE-537: NPE in JTA AFTER_COMPLETION command processing
 */
@Category({UnitTest.class, ClientServerTest.class})
public class CommitCommandTest {

  /**
   * Test for GEODE-537 No NPE should be thrown from the
   * {@link CommitCommand#writeCommitResponse(org.apache.geode.internal.cache.TXCommitMessage, Message, ServerConnection)}
   * if the response message is null as it is the case when JTA transaction is rolled back with
   * TX_SYNCHRONIZATION AFTER_COMPLETION STATUS_ROLLEDBACK
   */
  @Test
  public void testWriteNullResponse() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    Message origMsg = mock(Message.class);
    ServerConnection servConn = mock(ServerConnection.class);
    when(servConn.getResponseMessage()).thenReturn(mock(Message.class));
    when(servConn.getCache()).thenReturn(cache);
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    CommitCommand.writeCommitResponse(null, origMsg, servConn);
  }

  /**
   * GEODE-5269 CommitConflictException after TransactionInDoubtException
   * CommitCommand needs to stall waiting for the host of a transaction to
   * finish shutting down before sending a TransactionInDoubtException to
   * the client.
   */
  @Test
  public void testTransactionInDoubtWaitsForTargetDeparture() throws Exception {
    CommitCommand command = (CommitCommand) CommitCommand.getCommand();
    Message clientMessage = mock(Message.class);
    ServerConnection serverConnection = mock(ServerConnection.class);
    TXManagerImpl txMgr = mock(TXManagerImpl.class);
    TXStateProxy txProxy = mock(TXStateProxy.class);
    InternalCache cache = mock(InternalCache.class);
    DistributionManager distributionManager = mock(DistributionManager.class);
    MembershipManager membershipManager = mock(MembershipManager.class);
    ServerSideHandshake handshake = mock(ServerSideHandshake.class);
    boolean wasInProgress = false;

    doReturn(cache).when(serverConnection).getCache();
    doReturn(distributionManager).when(cache).getDistributionManager();
    doReturn(membershipManager).when(distributionManager).getMembershipManager();
    doReturn(false).when(distributionManager).isCurrentMember(isA(
        InternalDistributedMember.class));

    doReturn(mock(Message.class)).when(serverConnection).getErrorResponseMessage();
    doReturn(handshake).when(serverConnection).getHandshake();
    doReturn(1000).when(handshake).getClientReadTimeout();

    doReturn(new InternalDistributedMember(InetAddress.getLocalHost(), 1234)).when(txProxy)
        .getTarget();

    TransactionInDoubtException transactionInDoubtException =
        new TransactionInDoubtException("tx in doubt");
    transactionInDoubtException.initCause(new CacheClosedException("testing"));
    doThrow(transactionInDoubtException).when(txMgr).commit();

    command.commitTransaction(
        clientMessage, serverConnection, txMgr, wasInProgress, txProxy);

    verify(txMgr, atLeastOnce()).commit();
    verify(membershipManager, times(1)).waitForDeparture(isA(DistributedMember.class),
        isA(Integer.class));
  }
}
