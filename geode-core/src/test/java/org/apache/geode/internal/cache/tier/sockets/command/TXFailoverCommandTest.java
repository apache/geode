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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.FindRemoteTXMessage.FindRemoteTXMessageReplyProcessor;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class TXFailoverCommandTest {

  @Test
  public void testTXFailoverSettingTargetNode() throws Exception {
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    FindRemoteTXMessageReplyProcessor processor = mock(FindRemoteTXMessageReplyProcessor.class);
    InternalCache cache = mock(InternalCache.class);
    InternalDistributedMember client = mock(InternalDistributedMember.class);
    InternalDistributedMember host = mock(InternalDistributedMember.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    Message message = mock(Message.class);
    ServerConnection serverConnection = mock(ServerConnection.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);

    int uniqueId = 1;
    TXId txId = new TXId(client, uniqueId);
    TXStateProxyImpl proxy = new TXStateProxyImpl(cache, txManager, txId, null, disabledClock());

    when(cache.getCacheTransactionManager()).thenReturn(txManager);
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributedSystem()).thenReturn(system);
    when(clientProxyMembershipID.getDistributedMember()).thenReturn(client);
    when(message.getTransactionId()).thenReturn(uniqueId);
    when(processor.getHostingMember()).thenReturn(host);
    when(serverConnection.getProxyID()).thenReturn(clientProxyMembershipID);
    when(serverConnection.getCache()).thenReturn(cache);
    when(txManager.getTXState()).thenReturn(proxy);

    when(serverConnection.getReplyMessage()).thenReturn(mock(Message.class));

    TXFailoverCommand command = spy(new TXFailoverCommand());
    doReturn(txId).when(command).createTXId(client, uniqueId);
    doReturn(processor).when(command).sendFindRemoteTXMessage(cache, txId);

    command.cmdExecute(message, serverConnection, null, 1);

    assertNotNull(proxy.getRealDeal(host));
    assertEquals(proxy.getTarget(), host);
  }
}
