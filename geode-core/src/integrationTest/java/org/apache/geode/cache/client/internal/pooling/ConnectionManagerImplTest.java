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
package org.apache.geode.cache.client.internal.pooling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.ConnectionFactory;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.EndpointManager;
import org.apache.geode.distributed.PoolCancelledException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.logging.InternalLogWriter;

public class ConnectionManagerImplTest {
  ConnectionManagerImpl connectionManager;
  public final String poolName = "poolName";
  public final ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
  public final EndpointManager endpointManager = mock(EndpointManager.class);
  public final InternalLogWriter securityLogger = mock(InternalLogWriter.class);
  public final CancelCriterion cancelCriterion = mock(CancelCriterion.class);
  public final PoolStats poolStats = mock(PoolStats.class);
  public final ScheduledExecutorService backgroundProcessor = mock(ScheduledExecutorService.class);
  public int maxConnections = 800;
  public int minConnections = 10;
  public long idleTimeout = 1000;
  public long timeout = 1000;
  public int lifetimeTimeout = 1000;
  public long pingInterval = 10;

  private ConnectionManagerImpl createDefaultConnectionManager() {
    return new ConnectionManagerImpl(poolName, connectionFactory, endpointManager, maxConnections,
        minConnections, idleTimeout, lifetimeTimeout, securityLogger, pingInterval, cancelCriterion,
        poolStats);
  }

  @Test
  public void startExecutedPrefillConnectionsOnce() {
    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);
    connectionManager.start(backgroundProcessor);
    verify(backgroundProcessor, times(1)).execute(any());

    connectionManager.close(false);
  }

  @Test
  public void startShouldEatRejectedExecutionException() {
    doThrow(RejectedExecutionException.class).when(backgroundProcessor).execute(any());

    connectionManager = createDefaultConnectionManager();
    assertThatCode(() -> connectionManager.start(backgroundProcessor)).doesNotThrowAnyException();

    connectionManager.close(false);
  }

  @Test
  public void borrowConnectionThrowsWhenUsingExistingConnectionsAndNoConnectionsExist() {
    ServerLocation serverLocation = mock(ServerLocation.class);

    connectionManager = createDefaultConnectionManager();
    assertThatThrownBy(() -> connectionManager.borrowConnection(serverLocation, timeout, true))
        .isInstanceOf(AllConnectionsInUseException.class);

    connectionManager.close(false);
  }

  @Test
  public void borrowConnectionCreatesAConnectionOnSpecifiedServerWhenNoneExist() {
    Connection connection = mock(Connection.class);
    ServerLocation serverLocation = mock(ServerLocation.class);
    when(connectionFactory.createClientToServerConnection(serverLocation, false))
        .thenReturn(connection);

    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);

    assertThat(connectionManager.borrowConnection(serverLocation, timeout, false))
        .isInstanceOf(PooledConnection.class);
    assertThat(connectionManager.getConnectionCount()).isEqualTo(1);

    connectionManager.close(false);
  }

  @Test
  public void borrowConnectionCreatesAConnectionWhenNoneExist() {
    Connection connection = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(any())).thenReturn(connection);

    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);

    assertThat(connectionManager.borrowConnection(timeout)).isInstanceOf(PooledConnection.class);
    assertThat(connectionManager.getConnectionCount()).isEqualTo(1);

    connectionManager.close(false);
  }

  @Test
  public void borrowConnectionReturnsAnActiveConnection() {
    Connection connection = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(any())).thenReturn(connection);

    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);

    PooledConnection heldConnection =
        (PooledConnection) connectionManager.borrowConnection(timeout);
    assertThatThrownBy(() -> heldConnection.activate()).isInstanceOf(InternalGemFireException.class)
        .hasMessageContaining("Connection already active");

    connectionManager.close(false);
  }

  @Test
  public void borrowConnectionReturnsAConnectionWhenOneExists() {
    ServerLocation serverLocation = mock(ServerLocation.class);
    Endpoint endpoint = mock(Endpoint.class);
    Connection connection = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(any())).thenReturn(connection);
    when(connection.getServer()).thenReturn(serverLocation);
    when(connection.getEndpoint()).thenReturn(endpoint);
    when(endpoint.getLocation()).thenReturn(serverLocation);

    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);

    Connection heldConnection = connectionManager.borrowConnection(timeout);
    connectionManager.returnConnection(heldConnection);
    heldConnection = connectionManager.borrowConnection(timeout);
    assertThat(heldConnection.getServer()).isEqualTo(connection.getServer());
    assertThat(connectionManager.getConnectionCount()).isEqualTo(1);

    connectionManager.close(false);
  }

  @Test
  public void borrowConnectionThrowsExceptionWhenUnableToCreateConnection() {
    when(connectionFactory.createClientToServerConnection(any())).thenReturn(null);
    doNothing().when(backgroundProcessor).execute(any());

    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);
    assertThatThrownBy(() -> connectionManager.borrowConnection(timeout))
        .isInstanceOf(NoAvailableServersException.class);
    assertThat(connectionManager.getConnectionCount()).isEqualTo(0);

    connectionManager.close(false);
  }

  @Test
  public void borrowConnectionWillSchedulePrefillIfUnderMinimumConnections() {
    when(connectionFactory.createClientToServerConnection(any())).thenReturn(null);
    doNothing().when(backgroundProcessor).execute(any());

    pingInterval = 20000000; // set it high to prevent prefill retry
    connectionManager = spy(createDefaultConnectionManager());
    connectionManager.start(backgroundProcessor);
    assertThatThrownBy(() -> connectionManager.borrowConnection(timeout))
        .isInstanceOf(NoAvailableServersException.class);
    assertThat(connectionManager.getConnectionCount()).isEqualTo(0);

    verify(connectionManager, times(2)).startBackgroundPrefill();
    connectionManager.close(false);
  }

  @Test
  public void borrowConnectionGivesUpWhenShuttingDown() {
    int maxConnections = 1;
    Connection connection = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(any())).thenReturn(connection);

    connectionManager = new ConnectionManagerImpl(poolName, connectionFactory, endpointManager,
        maxConnections, 1, idleTimeout, lifetimeTimeout, securityLogger, pingInterval,
        cancelCriterion, poolStats);
    connectionManager.start(backgroundProcessor);
    connectionManager.shuttingDown.set(true);

    // reach max connection count so we can't create a new connection and end up in the wait loop
    connectionManager.borrowConnection(timeout);
    assertThat(connectionManager.getConnectionCount()).isEqualTo(maxConnections);

    assertThatThrownBy(() -> connectionManager.borrowConnection(timeout))
        .isInstanceOf(PoolCancelledException.class);

    connectionManager.close(false);
  }

  @Test
  public void borrowConnectionTimesOutWithException() {
    int maxConnections = 1;
    Connection connection = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(any())).thenReturn(connection);

    connectionManager = new ConnectionManagerImpl(poolName, connectionFactory, endpointManager,
        maxConnections, 1, idleTimeout, lifetimeTimeout, securityLogger, pingInterval,
        cancelCriterion, poolStats);
    connectionManager.start(backgroundProcessor);

    // reach max connection count so we can't create a new connection and end up in the wait loop
    connectionManager.borrowConnection(timeout);
    assertThat(connectionManager.getConnectionCount()).isEqualTo(maxConnections);

    assertThatThrownBy(() -> connectionManager.borrowConnection(10))
        .isInstanceOf(AllConnectionsInUseException.class);

    connectionManager.close(false);
  }

  @Test
  public void borrowWithServerLocationBreaksMaxConnectionContract() {
    int maxConnections = 2;

    ServerLocation serverLocation1 = mock(ServerLocation.class);
    Connection connection1 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation1, false))
        .thenReturn(connection1);

    ServerLocation serverLocation2 = mock(ServerLocation.class);
    Connection connection2 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation2, false))
        .thenReturn(connection2);

    ServerLocation serverLocation3 = mock(ServerLocation.class);
    Connection connection3 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation3, false))
        .thenReturn(connection3);

    connectionManager = new ConnectionManagerImpl(poolName, connectionFactory, endpointManager,
        maxConnections, 1, idleTimeout, lifetimeTimeout, securityLogger, pingInterval,
        cancelCriterion, poolStats);
    connectionManager.start(backgroundProcessor);

    connectionManager.borrowConnection(serverLocation1, timeout, false);
    connectionManager.borrowConnection(serverLocation2, timeout, false);
    connectionManager.borrowConnection(serverLocation3, timeout, false);

    assertThat(connectionManager.getConnectionCount()).isGreaterThan(maxConnections);

    connectionManager.close(false);
  }

  @Test
  public void returnConnectionReturnsToHead() {
    ServerLocation serverLocation1 = mock(ServerLocation.class);
    Connection connection1 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation1, false))
        .thenReturn(connection1);
    when(connection1.getServer()).thenReturn(serverLocation1);

    ServerLocation serverLocation2 = mock(ServerLocation.class);
    Connection connection2 = mock(Connection.class);
    Endpoint endpoint2 = mock(Endpoint.class);
    when(connectionFactory.createClientToServerConnection(serverLocation2, false))
        .thenReturn(connection2);
    when(connection2.getServer()).thenReturn(serverLocation2);
    when(connection2.getEndpoint()).thenReturn(endpoint2);
    when(endpoint2.getLocation()).thenReturn(serverLocation2);

    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);
    Connection heldConnection1 =
        connectionManager.borrowConnection(serverLocation1, timeout, false);
    Connection heldConnection2 =
        connectionManager.borrowConnection(serverLocation2, timeout, false);
    assertThat(connectionManager.getConnectionCount()).isEqualTo(2);

    connectionManager.returnConnection(heldConnection1, true);
    connectionManager.returnConnection(heldConnection2, true);

    assertThat(connectionManager.borrowConnection(timeout).getServer())
        .isEqualTo(connection2.getServer());

    connectionManager.close(false);
  }

  @Test
  public void shouldDestroyConnectionsDoNotGetReturnedToPool() {
    Connection connection = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(any())).thenReturn(connection);

    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);

    Connection heldConnection = connectionManager.borrowConnection(timeout);
    heldConnection.destroy();
    connectionManager.returnConnection(heldConnection, true);

    assertThat(connectionManager.borrowConnection(timeout)).isNotEqualTo(connection);
    verify(connectionFactory, times(2)).createClientToServerConnection(any());

    connectionManager.close(false);
  }

  @Test
  public void connectionGetsDestroyedWhenReturningToPoolAndOverMaxConnections() {
    int maxConnections = 2;

    ServerLocation serverLocation1 = mock(ServerLocation.class);
    Connection connection1 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation1, false))
        .thenReturn(connection1);

    ServerLocation serverLocation2 = mock(ServerLocation.class);
    Connection connection2 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation2, false))
        .thenReturn(connection2);

    ServerLocation serverLocation3 = mock(ServerLocation.class);
    Connection connection3 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation3, false))
        .thenReturn(connection3);

    connectionManager = new ConnectionManagerImpl(poolName, connectionFactory, endpointManager,
        maxConnections, 1, idleTimeout, lifetimeTimeout, securityLogger, pingInterval,
        cancelCriterion, poolStats);
    connectionManager.start(backgroundProcessor);

    Connection heldConnection1 =
        connectionManager.borrowConnection(serverLocation1, timeout, false);
    Connection heldConnection2 =
        connectionManager.borrowConnection(serverLocation2, timeout, false);
    Connection heldConnection3 =
        connectionManager.borrowConnection(serverLocation3, timeout, false);

    assertThat(connectionManager.getConnectionCount()).isGreaterThan(maxConnections);

    connectionManager.returnConnection(heldConnection3);
    assertThat(connectionManager.getConnectionCount()).isEqualTo(maxConnections);

    connectionManager.returnConnection(heldConnection1);
    connectionManager.returnConnection(heldConnection2);
    assertThat(connectionManager.getConnectionCount()).isEqualTo(maxConnections);

    connectionManager.close(false);
  }

  @Test
  public void exchangeCreatesNewConnectionIfNoneAreAvailable() {
    Set<ServerLocation> excluded = Collections.emptySet();

    ServerLocation serverLocation1 = mock(ServerLocation.class);
    Connection connection1 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation1, false))
        .thenReturn(connection1);

    ServerLocation serverLocation2 = mock(ServerLocation.class);
    Endpoint endpoint2 = mock(Endpoint.class);
    Connection connection2 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(eq(Collections.EMPTY_SET)))
        .thenReturn(connection2);
    when(connection2.getServer()).thenReturn(serverLocation2);
    when(connection2.getEndpoint()).thenReturn(endpoint2);
    when(endpoint2.getLocation()).thenReturn(serverLocation2);

    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);

    Connection heldConnection = connectionManager.borrowConnection(serverLocation1, timeout, false);
    heldConnection = connectionManager.exchangeConnection(heldConnection, excluded);

    assertThat(heldConnection.getServer()).isEqualTo(connection2.getServer());
    assertThat(connectionManager.getConnectionCount()).isEqualTo(2);
    verify(connectionFactory, times(1)).createClientToServerConnection(Collections.EMPTY_SET);

    connectionManager.close(false);
  }

  @Test
  public void exchangeBreaksMaxConnectionContractWhenNoConnectionsAreAvailable() {
    int maxConnections = 2;
    Set<ServerLocation> excluded = Collections.emptySet();

    ServerLocation serverLocation1 = mock(ServerLocation.class);
    Connection connection1 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation1, false))
        .thenReturn(connection1);

    ServerLocation serverLocation2 = mock(ServerLocation.class);
    Connection connection2 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation2, false))
        .thenReturn(connection2);

    ServerLocation serverLocation3 = mock(ServerLocation.class);
    Connection connection3 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation3, false))
        .thenReturn(connection3);

    ServerLocation serverLocation4 = mock(ServerLocation.class);
    Endpoint endpoint4 = mock(Endpoint.class);
    Connection connection4 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(eq(Collections.EMPTY_SET)))
        .thenReturn(connection4);
    when(connection4.getServer()).thenReturn(serverLocation4);
    when(connection4.getEndpoint()).thenReturn(endpoint4);
    when(endpoint4.getLocation()).thenReturn(serverLocation4);

    connectionManager = new ConnectionManagerImpl(poolName, connectionFactory, endpointManager,
        maxConnections, 1, idleTimeout, lifetimeTimeout, securityLogger, pingInterval,
        cancelCriterion, poolStats);
    connectionManager.start(backgroundProcessor);

    Connection heldConnection = connectionManager.borrowConnection(serverLocation1, timeout, false);
    connectionManager.borrowConnection(serverLocation2, timeout, false);
    connectionManager.borrowConnection(serverLocation3, timeout, false);
    assertThat(connectionManager.getConnectionCount()).isGreaterThan(maxConnections);

    heldConnection = connectionManager.exchangeConnection(heldConnection, excluded);
    assertThat(connectionManager.getConnectionCount()).isGreaterThan(maxConnections);
    assertThat(heldConnection.getServer()).isEqualTo(connection4.getServer());
    verify(connectionFactory, times(1)).createClientToServerConnection(Collections.EMPTY_SET);

    connectionManager.close(false);
  }

  @Test
  public void exchangeReturnsExistingConnectionIfOneExists() {
    Set<ServerLocation> excluded = Collections.emptySet();

    ServerLocation serverLocation1 = mock(ServerLocation.class);
    Connection connection1 = mock(Connection.class);
    when(connectionFactory.createClientToServerConnection(serverLocation1, false))
        .thenReturn(connection1);

    ServerLocation serverLocation2 = mock(ServerLocation.class);
    Connection connection2 = mock(Connection.class);
    Endpoint endpoint2 = mock(Endpoint.class);
    when(connectionFactory.createClientToServerConnection(serverLocation2, false))
        .thenReturn(connection2);
    when(connection2.getServer()).thenReturn(serverLocation2);
    when(connection2.getEndpoint()).thenReturn(endpoint2);
    when(endpoint2.getLocation()).thenReturn(serverLocation2);

    connectionManager = createDefaultConnectionManager();
    connectionManager.start(backgroundProcessor);

    Connection heldConnection1 =
        connectionManager.borrowConnection(serverLocation1, timeout, false);
    Connection heldConnection2 =
        connectionManager.borrowConnection(serverLocation2, timeout, false);

    connectionManager.returnConnection(heldConnection2);
    heldConnection2 = connectionManager.exchangeConnection(heldConnection1, excluded);
    assertThat(heldConnection2.getServer()).isEqualTo(connection2.getServer());
    assertThat(connectionManager.getConnectionCount()).isEqualTo(2);

    connectionManager.close(false);
  }
}
