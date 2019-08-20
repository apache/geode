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
package org.apache.geode.internal.tcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.InternalMembershipManager;
import org.apache.geode.internal.alerting.AlertingAction;
import org.apache.geode.internal.net.BufferPool;
import org.apache.geode.internal.net.SocketCloser;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class ConnectionJUnitTest {

  /**
   * Test whether suspicion is raised about a member that closes its shared/unordered TCPConduit
   * connection
   */
  @Test
  public void testSuspicionRaised() throws Exception {
    // this test has to create a lot of mocks because Connection
    // uses a lot of objects

    // mock the socket
    ConnectionTable table = mock(ConnectionTable.class);
    DistributionManager distMgr = mock(DistributionManager.class);
    InternalMembershipManager membership = mock(InternalMembershipManager.class);
    TCPConduit conduit = mock(TCPConduit.class);
    DMStats stats = mock(DMStats.class);

    // mock the connection table and conduit

    when(table.getConduit()).thenReturn(conduit);
    when(table.getBufferPool()).thenReturn(new BufferPool(stats));

    CancelCriterion stopper = mock(CancelCriterion.class);
    when(stopper.cancelInProgress()).thenReturn(null);
    when(conduit.getCancelCriterion()).thenReturn(stopper);

    when(conduit.getSocketId())
        .thenReturn(new InetSocketAddress(SocketCreator.getLocalHost(), 10337));

    // mock the distribution manager and membership manager
    when(distMgr.getMembershipManager()).thenReturn(membership);
    when(conduit.getDM()).thenReturn(distMgr);
    when(conduit.getStats()).thenReturn(stats);
    when(table.getDM()).thenReturn(distMgr);
    SocketCloser closer = mock(SocketCloser.class);
    when(table.getSocketCloser()).thenReturn(closer);

    SocketChannel channel = SocketChannel.open();

    Connection conn = new Connection(table, channel.socket());
    conn.setSharedUnorderedForTest();
    conn.run();
    verify(membership).suspectMember(isNull(InternalDistributedMember.class), any(String.class));
  }

  @Test
  public void connectTimeoutIsShortWhenAlerting() throws UnknownHostException {
    ConnectionTable table = mock(ConnectionTable.class);
    TCPConduit conduit = mock(TCPConduit.class);
    when(table.getConduit()).thenReturn(conduit);
    when(conduit.getSocketId())
        .thenReturn(new InetSocketAddress(SocketCreator.getLocalHost(), 12345));
    DistributionConfig config = mock(DistributionConfig.class);
    when(config.getMemberTimeout()).thenReturn(100);
    Connection connection = new Connection(table, mock(Socket.class));
    int normalTimeout = connection.getP2PConnectTimeout(config);
    assertThat(normalTimeout).isEqualTo(600);
    AlertingAction.execute(() -> {
      assertThat(connection.getP2PConnectTimeout(config)).isEqualTo(100);
    });

  }
}
