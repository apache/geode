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

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.Socket;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@Category(UnitTest.class)
public class ConnectionTableTest {

  @Test
  public void testConnectionsClosedDuringCreateAreNotAddedAsReceivers() throws Exception {
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(system.isShareSockets()).thenReturn(false);

    DM dm = mock(DM.class);
    when(dm.getSystem()).thenReturn(system);

    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    DMStats dmStats = mock(DMStats.class);

    TCPConduit tcpConduit = mock(TCPConduit.class);
    when(tcpConduit.getDM()).thenReturn(dm);
    when(tcpConduit.getCancelCriterion()).thenReturn(cancelCriterion);
    when(tcpConduit.getStats()).thenReturn(dmStats);

    Connection connection = mock(Connection.class);
    when(connection.isSocketClosed()).thenReturn(true); // Pretend this closed as soon at it was
                                                        // created

    Socket socket = mock(Socket.class);

    ConnectionTable table = ConnectionTable.create(tcpConduit);

    PeerConnectionFactory factory = mock(PeerConnectionFactory.class);
    when(factory.createReceiver(table, socket)).thenReturn(connection);

    table.acceptConnection(socket, factory);
    assertEquals(0, table.getNumberOfReceivers());
  }
}
