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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.LinkedList;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.cache.client.internal.ClientCacheConnection;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.EndpointManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.pooling.PooledConnection;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

public class GatewaySenderEventRemoteDispatcherIntegrationTest {

  /*
   * Sometimes hostname lookup is flaky. We don't want such a failure to cripple our
   * event processor.
   *
   * This test assumes hostname lookup (of IP number) succeeds when establishing the initial
   * connection, but fails when constructing the InternalDistributedSystem object in response to a
   * remote server crash.
   */
  @Test
  public void canProcessesEventAfterHostnameLookupFailsInNotifyServerCrashed() throws Exception {

    final PoolImpl pool = getPool();

    final ServerLocation serverLocation = mock(ServerLocation.class);

    final AbstractGatewaySenderEventProcessor eventProcessor =
        getMockedAbstractGatewaySenderEventProcessor(pool, serverLocation);

    final Endpoint endpoint = getMockedEndpoint(serverLocation);
    final ClientCacheConnection connection = getMockedConnection(serverLocation, endpoint);

    /*
     * In order for listeners to be notified, the endpoint must be referenced by the
     * endpointManager so that it can be removed when the RuntimeException() is thrown by the
     * connection
     */
    final EndpointManager endpointManager = pool.getEndpointManager();
    endpointManager.referenceEndpoint(serverLocation, mock(InternalDistributedMember.class));

    final GatewaySenderEventRemoteDispatcher dispatcher =
        new GatewaySenderEventRemoteDispatcher(eventProcessor, connection);

    /*
     * We have mocked our connection to throw a RuntimeException when readAcknowledgement() is
     * called, then in the exception handling for that RuntimeException, the UnknownHostException
     * will be thrown when trying to notify listeners of the crash.
     */
    dispatcher.readAcknowledgement();

    /*
     * The handling of the UnknownHostException should not result in the event processor being
     * stopped, so assert that setIsStopped(true) was never called.
     */
    verify(eventProcessor, Mockito.times(0)).setIsStopped(true);
  }

  private PoolImpl getPool() {
    final DistributionConfig distributionConfig = mock(DistributionConfig.class);
    doReturn(new SecurableCommunicationChannel[] {}).when(distributionConfig)
        .getSecurableCommunicationChannels();

    SocketCreatorFactory.setDistributionConfig(distributionConfig);

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

    return PoolImpl.create(poolManager, "pool", poolAttributes, new LinkedList<>(),
        internalDistributedSystem, internalCache, tMonitoring);
  }

  private ClientCacheConnection getMockedConnection(ServerLocation serverLocation,
      Endpoint endpoint)
      throws Exception {
    /*
     * Mock the connection to throw a RuntimeException() when connection.Execute() is called,
     * so that we attempt to notify listeners in the exception handling logic in
     * OpExecutorImpl.executeWithPossibleReAuthentication()
     */
    final ClientCacheConnection connection = mock(PooledConnection.class);
    doReturn(serverLocation).when(connection).getServer();
    doReturn(endpoint).when(connection).getEndpoint();
    doThrow(new RuntimeException()).when(connection).execute(any());
    return connection;
  }

  private AbstractGatewaySenderEventProcessor getMockedAbstractGatewaySenderEventProcessor(
      PoolImpl pool, ServerLocation serverLocation) {
    final AbstractGatewaySender abstractGatewaySender = mock(AbstractGatewaySender.class);
    doReturn(serverLocation).when(abstractGatewaySender).getServerLocation();
    doReturn(pool).when(abstractGatewaySender).getProxy();

    final AbstractGatewaySenderEventProcessor eventProcessor =
        mock(AbstractGatewaySenderEventProcessor.class);
    doReturn(abstractGatewaySender).when(eventProcessor).getSender();
    return eventProcessor;
  }

  private Endpoint getMockedEndpoint(ServerLocation serverLocation) {
    final Endpoint endpoint = mock(Endpoint.class);
    doReturn(serverLocation).when(endpoint).getLocation();
    return endpoint;
  }

}
