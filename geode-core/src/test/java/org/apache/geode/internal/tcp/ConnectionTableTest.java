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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.categories.UnitTest;


@Category({UnitTest.class, MembershipTest.class})
public class ConnectionTableTest {
  private ConnectionTable connectionTable;
  private Socket socket;
  private PeerConnectionFactory factory;
  private Connection connection;

  @Before
  public void initConnectionTable() throws Exception {
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(system.isShareSockets()).thenReturn(false);

    DistributionManager dm = mock(DistributionManager.class);
    when(dm.getSystem()).thenReturn(system);

    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    DMStats dmStats = mock(DMStats.class);

    TCPConduit tcpConduit = mock(TCPConduit.class);
    when(tcpConduit.getDM()).thenReturn(dm);
    when(tcpConduit.getCancelCriterion()).thenReturn(cancelCriterion);
    when(tcpConduit.getStats()).thenReturn(dmStats);

    connection = mock(Connection.class);

    socket = mock(Socket.class);

    connectionTable = ConnectionTable.create(tcpConduit);

    factory = mock(PeerConnectionFactory.class);
    when(factory.createReceiver(connectionTable, socket)).thenReturn(connection);
  }

  @Test
  public void testConnectionsClosedDuringCreateAreNotAddedAsReceivers() throws Exception {
    when(connection.isReceiverStopped()).thenReturn(false);
    when(connection.isSocketClosed()).thenReturn(true); // Pretend this closed as soon at it was
                                                        // created

    connectionTable.acceptConnection(socket, factory);
    assertEquals(0, connectionTable.getNumberOfReceivers());
  }

  @Test
  public void testThreadStoppedNotAddedAsReceivers() throws Exception {
    when(connection.isSocketClosed()).thenReturn(false); // connection is not closed

    when(connection.isReceiverStopped()).thenReturn(true);// but receiver is stopped

    connectionTable.acceptConnection(socket, factory);
    assertEquals(0, connectionTable.getNumberOfReceivers());
  }

  @Test
  public void testSocketNotClosedAddedAsReceivers() throws Exception {
    when(connection.isSocketClosed()).thenReturn(false);// connection is not closed

    connectionTable.acceptConnection(socket, factory);
    assertEquals(1, connectionTable.getNumberOfReceivers());
  }

  @Test
  public void testThreadOwnedSocketsAreRemoved() throws Exception {
    Boolean wantsResources = ConnectionTable.getThreadOwnsResourcesRegistration();
    ConnectionTable.threadWantsOwnResources();
    try {
      Map<DistributedMember, Connection> threadConnectionMap = new HashMap<>();
      connectionTable.threadOrderedConnMap.set(threadConnectionMap);
      ConnectionTable.releaseThreadsSockets();
      assertEquals(0, threadConnectionMap.size());
    } finally {
      if (wantsResources != Boolean.FALSE) {
        ConnectionTable.threadWantsSharedResources();
      }
    }
  }
}
