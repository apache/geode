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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.FindRemoteTXMessage;
import org.apache.geode.internal.cache.FindRemoteTXMessage.FindRemoteTXMessageReplyProcessor;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.UnitTest")
@PrepareForTest({FindRemoteTXMessage.class})
public class TXFailoverCommandTest {
  @Test
  public void testTXFailoverSettingTargetNode()
      throws ClassNotFoundException, IOException, InterruptedException {
    TXFailoverCommand cmd = mock(TXFailoverCommand.class);
    Message msg = mock(Message.class);
    ServerConnection serverConnection = mock(ServerConnection.class);
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    InternalDistributedMember client = mock(InternalDistributedMember.class);
    TXManagerImpl txMgr = mock(TXManagerImpl.class);
    InternalCache cache = Fakes.cache();
    int uniqueId = 1;
    TXId txId = new TXId(client, uniqueId);
    TXStateProxyImpl proxy = new TXStateProxyImpl(txMgr, txId, null);
    FindRemoteTXMessageReplyProcessor processor = mock(FindRemoteTXMessageReplyProcessor.class);
    InternalDistributedMember host = mock(InternalDistributedMember.class);

    doCallRealMethod().when(cmd).cmdExecute(msg, serverConnection, null, 1);
    when(serverConnection.getProxyID()).thenReturn(clientProxyMembershipID);
    when(clientProxyMembershipID.getDistributedMember()).thenReturn(client);
    when(msg.getTransactionId()).thenReturn(uniqueId);
    when(serverConnection.getCache()).thenReturn(cache);
    when(cache.getCacheTransactionManager()).thenReturn(txMgr);
    when(txMgr.getTXState()).thenReturn(proxy);
    when(cmd.createTXId(client, uniqueId)).thenReturn(txId);
    PowerMockito.mockStatic(FindRemoteTXMessage.class);
    PowerMockito.when(FindRemoteTXMessage.send(cache, txId)).thenReturn(processor);
    when(processor.getHostingMember()).thenReturn(host);
    when(proxy.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(mock(DistributedSystem.class));

    cmd.cmdExecute(msg, serverConnection, null, 1);
    assertNotNull(proxy.getRealDeal(host));
    assertEquals(proxy.getTarget(), host);
  }
}
