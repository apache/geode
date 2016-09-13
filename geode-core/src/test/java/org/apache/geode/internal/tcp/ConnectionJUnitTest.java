/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.tcp;

import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.internal.net.SocketCloser;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionJUnitTest {

  /**
   * Test whether suspicion is raised about a member that
   * closes its shared/unordered TCPConduit connection
   */
  @Test
  public void testSuspicionRaised() throws Exception {
    // this test has to create a lot of mocks because Connection
    // uses a lot of objects
    
    // mock the socket
    ConnectionTable table = mock(ConnectionTable.class);
    DM distMgr = mock(DM.class);
    MembershipManager membership = mock(MembershipManager.class);
    TCPConduit conduit = mock(TCPConduit.class);

    // mock the connection table and conduit
    
    when(table.getConduit()).thenReturn(conduit);

    CancelCriterion stopper = mock(CancelCriterion.class);
    when(stopper.cancelInProgress()).thenReturn(null);
    when(conduit.getCancelCriterion()).thenReturn(stopper);

    when(conduit.getId()).thenReturn(new InetSocketAddress(SocketCreator.getLocalHost(), 10337));
    
    // NIO can't be mocked because SocketChannel has a final method that
    // is used by Connection - configureBlocking
    when(conduit.useNIO()).thenReturn(false);
    
    // mock the distribution manager and membership manager
    when(distMgr.getMembershipManager()).thenReturn(membership);
    when(conduit.getDM()).thenReturn(distMgr);
    when(table.getDM()).thenReturn(distMgr);
    SocketCloser closer = mock(SocketCloser.class);
    when(table.getSocketCloser()).thenReturn(closer);

    InputStream instream = mock(InputStream.class);
    when(instream.read()).thenReturn(-1);
    Socket socket = mock(Socket.class);
    when(socket.getInputStream()).thenReturn(instream);
    
    Connection conn = new Connection(table, socket);
    conn.setSharedUnorderedForTest();
    conn.run();
    verify(membership).suspectMember(any(InternalDistributedMember.class), any(String.class));
  }
}
