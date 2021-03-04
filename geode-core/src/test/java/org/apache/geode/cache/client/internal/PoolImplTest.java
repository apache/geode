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

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.pooling.ConnectionManagerImpl;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class PoolImplTest {

  @Test
  public void calculateRetryAttemptsDoesNotDecrementRetryCountForFailureWithUnexpectedSocketClose() {
    List servers = mock(List.class);
    when(servers.size()).thenReturn(1);
    ConnectionSource connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);
    ServerConnectivityException serverConnectivityException =
        mock(ServerConnectivityException.class);
    when(serverConnectivityException.getMessage())
        .thenReturn(ConnectionManagerImpl.UNEXPECTED_SOCKET_CLOSED_MSG);

    PoolImpl poolImpl = spy(getPool(PoolFactory.DEFAULT_RETRY_ATTEMPTS, false));
    when(poolImpl.getConnectionSource()).thenReturn(connectionSource);

    assertThat(poolImpl.calculateRetryAttempts(serverConnectivityException)).isEqualTo(1);
  }

  @Test
  public void calculateRetryAttemptsDoesNotDecrementRetryCountForFailureDuringBorrowConnection() {
    List servers = mock(List.class);
    when(servers.size()).thenReturn(1);
    ConnectionSource connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);
    ServerConnectivityException serverConnectivityException =
        mock(ServerConnectivityException.class);
    when(serverConnectivityException.getMessage())
        .thenReturn(ConnectionManagerImpl.BORROW_CONN_ERROR_MSG);

    PoolImpl poolImpl = spy(getPool(PoolFactory.DEFAULT_RETRY_ATTEMPTS, false));
    when(poolImpl.getConnectionSource()).thenReturn(connectionSource);

    assertThat(poolImpl.calculateRetryAttempts(serverConnectivityException)).isEqualTo(1);
  }

  @Test
  public void calculateRetryAttemptsDecrementsRetryCountForFailureAfterSendingTheRequest() {
    List servers = mock(List.class);
    when(servers.size()).thenReturn(1);
    ConnectionSource connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);
    ServerConnectivityException serverConnectivityException =
        mock(ServerConnectivityException.class);
    when(serverConnectivityException.getMessage())
        .thenReturn(ConnectionManagerImpl.SOCKET_TIME_OUT_MSG);

    PoolImpl poolImpl = spy(getPool(PoolFactory.DEFAULT_RETRY_ATTEMPTS, false));
    when(poolImpl.getConnectionSource()).thenReturn(connectionSource);

    assertThat(poolImpl.calculateRetryAttempts(serverConnectivityException)).isEqualTo(0);
  }

  @Test
  public void calculateRetryAttemptsReturnsTheRetyCountConfiguredWithPool() {
    int retryCount = 1;
    List servers = mock(List.class);
    when(servers.size()).thenReturn(1);
    ConnectionSource connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);
    ServerConnectivityException serverConnectivityException =
        mock(ServerConnectivityException.class);
    when(serverConnectivityException.getMessage()).thenReturn("Timeout Exception");

    PoolImpl poolImpl = spy(getPool(retryCount, false));
    when(poolImpl.getConnectionSource()).thenReturn(connectionSource);

    assertThat(poolImpl.calculateRetryAttempts(serverConnectivityException)).isEqualTo(retryCount);
  }

  @Test
  public void checkPoolCreatedWithRequestLocatorInternalAddressEnabled_False() {
    int retryCount = 1;
    boolean requestInternalAddress = false;

    PoolImpl poolImpl = spy(getPool(retryCount, requestInternalAddress));

    assertThat(poolImpl.isRequestLocatorInternalAddressEnabled()).isEqualTo(requestInternalAddress);
  }

  @Test
  public void checkPoolCreatedWithRequestLocatorInternalAddressEnabled_True() {
    int retryCount = 1;
    boolean requestInternalAddress = true;

    PoolImpl poolImpl = spy(getPool(retryCount, requestInternalAddress));

    assertThat(poolImpl.isRequestLocatorInternalAddressEnabled()).isEqualTo(requestInternalAddress);
  }

  private PoolImpl getPool(int retryAttemptsAttribute,
      boolean requestLocatorInternalAddressEnabled) {
    final DistributionConfig distributionConfig = mock(DistributionConfig.class);
    doReturn(new SecurableCommunicationChannel[] {}).when(distributionConfig)
        .getSecurableCommunicationChannels();

    final Properties properties = new Properties();
    properties.put(DURABLE_CLIENT_ID, "1");

    final Statistics statistics = mock(Statistics.class);

    final PoolFactoryImpl.PoolAttributes poolAttributes =
        mock(PoolFactoryImpl.PoolAttributes.class);

    /*
     * These are the minimum pool attributes required
     * so that basic validation and setup completes successfully. The values of
     * these attributes have no importance to the assertions of the test itself.
     */
    doReturn(1).when(poolAttributes).getMaxConnections();
    doReturn((long) 10e8).when(poolAttributes).getPingInterval();
    doReturn(retryAttemptsAttribute).when(poolAttributes).getRetryAttempts();
    doReturn(requestLocatorInternalAddressEnabled).when(poolAttributes)
        .isRequestLocatorInternalAddressEnabled();

    final CancelCriterion cancelCriterion = mock(CancelCriterion.class);

    final InternalCache internalCache = mock(InternalCache.class);
    doReturn(cancelCriterion).when(internalCache).getCancelCriterion();

    final InternalDistributedSystem internalDistributedSystem =
        mock(InternalDistributedSystem.class);
    doReturn(distributionConfig).when(internalDistributedSystem).getConfig();
    doReturn(properties).when(internalDistributedSystem).getProperties();
    doReturn(statistics).when(internalDistributedSystem).createAtomicStatistics(any(), anyString());

    final PoolManagerImpl poolManager = mock(PoolManagerImpl.class);
    doReturn(true).when(poolManager).isNormal();

    final ThreadsMonitoring tMonitoring = mock(ThreadsMonitoring.class);

    return PoolImpl.create(poolManager, "pool", poolAttributes, new LinkedList<HostAndPort>(),
        internalDistributedSystem, internalCache, tMonitoring);
  }

}
