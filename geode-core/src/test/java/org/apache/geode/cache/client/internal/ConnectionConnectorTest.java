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
package org.apache.geode.cache.client.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.PoolCancelledException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionConnectorTest {
  private CancelCriterion cancelCriterion;
  private EndpointManager endpointManager;
  private InternalDistributedSystem ds;
  private ClientProxyMembershipID proxyId;
  private ClientSideHandshakeImpl handshake;
  private SocketCreator socketCreator;
  private ConnectionImpl connection;


  @Before
  public void setUp() throws Exception {
    cancelCriterion = mock(CancelCriterion.class);
    endpointManager = mock(EndpointManager.class);
    ds = mock(InternalDistributedSystem.class);
    proxyId = mock(ClientProxyMembershipID.class);
    handshake = mock(ClientSideHandshakeImpl.class);
    socketCreator = mock(SocketCreator.class);
    connection = mock(ConnectionImpl.class);

    // mocks don't seem to work well with CancelCriterion so let's create a real one
    cancelCriterion = new CancelCriterion() {
      @Override
      public String cancelInProgress() {
        return "shutting down for test";
      }

      @Override
      public RuntimeException generateCancelledException(Throwable throwable) {
        return new PoolCancelledException(cancelInProgress(), throwable);
      }
    };

  }

  @After
  public void tearDown() throws Exception {}

  @Test(expected = GemFireSecurityException.class)
  public void failedConnectionIsDestroyed() throws IOException {

    ConnectionConnector spyConnector =
        spy(new ConnectionConnector(endpointManager, ds, 0, 0, 0, cancelCriterion, false,
            null, socketCreator, handshake));
    doReturn(connection).when(spyConnector).getConnection(ds, cancelCriterion);
    doReturn(handshake).when(spyConnector).getClientSideHandshake(handshake);

    when(connection.connect(any(), any(), any(), anyInt(), anyInt(), anyInt(), any(), any(), any()))
        .thenThrow(new GemFireSecurityException("Expected exception"));
    try {
      spyConnector.connectClientToServer(mock(ServerLocation.class), false);
    } finally {
      verify(spyConnector).destroyConnection(any());
    }
  }

  @Test
  public void successfulConnectionIsNotDestroyed() throws IOException {

    ConnectionConnector spyConnector =
        spy(new ConnectionConnector(endpointManager, ds, 0, 0, 0, cancelCriterion, false,
            null, socketCreator, handshake));
    doReturn(connection).when(spyConnector).getConnection(ds, cancelCriterion);
    doReturn(handshake).when(spyConnector).getClientSideHandshake(handshake);

    try {
      spyConnector.connectClientToServer(mock(ServerLocation.class), false);
    } finally {
      verify(spyConnector, times(0)).destroyConnection(any());
    }
  }
}
