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

import static java.util.Collections.emptyList;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINEST;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.cache.client.SubscriptionNotEnabledException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.logging.LocalLogWriter;

@Tag("ClientServerTest")
public class QueueManagerIntegrationTest {

  private TestPool pool;
  private LocalLogWriter logger;
  private DistributedSystem ds;
  private EndpointManagerImpl endpoints;
  private TestConnectionSource source;
  private TestConnectionFactory factory;
  private QueueManagerImpl manager;
  private ScheduledExecutorService background;
  private PoolStats stats;
  private List<Op> opList;

  @BeforeEach
  public void setUp() {
    logger = new LocalLogWriter(FINEST.intLevel(), System.out);

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");

    ds = DistributedSystem.connect(properties);

    stats = new PoolStats(ds, "QueueManagerJUnitTest");
    pool = new TestPool();
    endpoints = new EndpointManagerImpl("pool", ds, ds.getCancelCriterion(), pool.getStats());
    source = new TestConnectionSource();
    factory = new TestConnectionFactory();
    background = Executors.newSingleThreadScheduledExecutor();
    opList = new LinkedList<>();
    addIgnoredException("Could not find any server to host primary client queue.");
    addIgnoredException("Could not find any server to host redundant client queue.");
  }

  @AfterEach
  public void tearDown() {
    background.shutdownNow();
    manager.close(false);
    endpoints.close();

    ds.disconnect();
  }

  @Test
  public void testBasic() {
    factory.addConnection(0, 0, 1);
    factory.addConnection(0, 0, 2);
    factory.addConnection(0, 0, 3);
    manager = new QueueManagerImpl(pool, endpoints, source, factory, 2, 2000, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);

    assertPortEquals(1, manager.getAllConnections().getPrimary());
    assertPortEquals(new int[] {2, 3}, manager.getAllConnections().getBackups());
  }

  @Test
  public void testUseBestRedundant() {
    factory.addConnection(0, 0, 1);
    factory.addConnection(1, 23, 2);
    factory.addConnection(1, 11, 3);
    manager = new QueueManagerImpl(pool, endpoints, source, factory, 2, 2000, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);

    assertPortEquals(2, manager.getAllConnections().getPrimary());
    assertPortEquals(new int[] {3, 1}, manager.getAllConnections().getBackups());
  }

  @Test
  public void testHandleErrorsOnInit() {
    factory.addError();
    factory.addConnection(0, 0, 1);
    factory.addError();
    factory.addConnection(1, 23, 2);
    factory.addError();
    factory.addError();
    factory.addError();
    factory.addConnection(0, 0, 3);
    manager = new QueueManagerImpl(pool, endpoints, source, factory, 3, 2000, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);

    // The primary queue can be set before we try to fill in for all of the failed backup servers,
    // so we need to wait for the initialization to finish rather than counting on the
    // manager.getAllConnections() to wait for a primary
    await().untilAsserted(() -> {
      assertPortEquals(2, manager.getAllConnections().getPrimary());
      assertPortEquals(new int[] {1, 3}, manager.getAllConnections().getBackups());
    });

    manager.close(false);

    factory.addError();
    factory.addError();
    factory.addError();
    factory.addError();
    factory.addError();
    factory.addConnection(0, 0, 1);
    factory.addConnection(0, 0, 2);
    factory.addConnection(0, 0, 3);
    manager = new QueueManagerImpl(pool, endpoints, source, factory, 3, 2000, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);

    // wait for backups to come online.
    await().untilAsserted(() -> {
      assertPortEquals(1, manager.getAllConnections().getPrimary());
      assertPortEquals(new int[] {2, 3}, manager.getAllConnections().getBackups());
    });
  }

  @Test
  public void testMakeNewPrimary() {
    factory.addConnection(0, 0, 1);
    factory.addConnection(0, 0, 2);
    factory.addConnection(0, 0, 3);
    factory.addConnection(0, 0, 4);
    factory.addConnection(0, 0, 5);
    manager = new QueueManagerImpl(pool, endpoints, source, factory, 3, 2000, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);

    assertPortEquals(1, manager.getAllConnections().getPrimary());
    assertPortEquals(new int[] {2, 3, 4}, manager.getAllConnections().getBackups());

    manager.getAllConnections().getPrimary().destroy();

    assertPortEquals(2, manager.getAllConnections().getPrimary());

    await().untilAsserted(
        () -> assertPortEquals(new int[] {3, 4, 5}, manager.getAllConnections().getBackups()));
  }

  @Test
  public void testWatchForNewRedundant() {
    factory.addConnection(0, 0, 1);
    factory.addConnection(0, 0, 2);
    manager = new QueueManagerImpl(pool, endpoints, source, factory, 2, 20, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);

    assertPortEquals(1, manager.getAllConnections().getPrimary());
    assertPortEquals(new int[] {2}, manager.getAllConnections().getBackups());

    factory.addConnection(0, 0, 3);
    factory.addConnection(0, 0, 4);

    assertPortEquals(1, manager.getAllConnections().getPrimary());

    await().untilAsserted(
        () -> assertPortEquals(new int[] {2, 3}, manager.getAllConnections().getBackups()));

    Connection backup = manager.getAllConnections().getBackups().get(0);
    backup.destroy();

    assertPortEquals(1, manager.getAllConnections().getPrimary());

    await().untilAsserted(
        () -> assertPortEquals(new int[] {3, 4}, manager.getAllConnections().getBackups()));
  }

  @Test
  public void testWaitForPrimary() {
    factory.addConnection(0, 0, 1);
    manager = new QueueManagerImpl(pool, endpoints, source, factory, 2, 20, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);
    manager.getAllConnections().getPrimary().destroy();

    assertThatThrownBy(() -> manager.getAllConnections().getPrimary())
        .isInstanceOf(NoSubscriptionServersAvailableException.class);
    factory.addConnection(0, 0, 2);
    factory.addConnection(0, 0, 3);

    await().untilAsserted(
        () -> assertThatCode(() -> manager.getAllConnections()).doesNotThrowAnyException());

    assertPortEquals(2, manager.getAllConnections().getPrimary());
  }

  @Test
  public void testThrowsServerRefusedConnectionException() {
    String serverRefusedConnectionExceptionMessage =
        "Peer or client version with ordinal x not supported. Highest known version is x.x.x.";
    // Spy the factory so that the createClientToServerConnection method can be mocked
    TestConnectionFactory factorySpy = spy(factory);
    manager = new QueueManagerImpl(pool, endpoints, source, factorySpy, 2, 20, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    // Cause a ServerRefusedConnectionException to be thrown from createClientToServerConnection
    ServerRefusedConnectionException e =
        new ServerRefusedConnectionException(mock(DistributedMember.class),
            serverRefusedConnectionExceptionMessage);
    ServerLocation sl = new ServerLocation("localhost", 1);
    when(factorySpy.createClientToServerConnection(sl, true)).thenThrow(e);
    // Add a server connection
    factory.addConnection(0, 0, 1);
    // Attempt to start the manager
    assertThatThrownBy(() -> manager.start(background))
        .isInstanceOf(ServerRefusedConnectionException.class)
        .hasMessageContaining(serverRefusedConnectionExceptionMessage);
  }

  @Test
  public void testAddToConnectionListCallsCloseConnectionOpWithKeepAliveTrue2() {
    // Create a TestConnection
    TestConnection connection = factory.addConnection(0, 0, 1);
    assertThat(connection.keepAlive).isFalse();

    // Get and close its Endpoint
    Endpoint endpoint = connection.getEndpoint();
    endpoint.close();

    // Create and start a QueueManagerImpl
    manager = new QueueManagerImpl(pool, endpoints, source, factory, 2, 20, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);

    // Assert that the connection keepAlive is true
    assertThat(connection.keepAlive).isTrue();
  }

  @Test
  public void recoverPrimaryRegistersBeforeSendingReady() {
    Set<ServerLocation> excludedServers = new HashSet<>();
    excludedServers.add(new ServerLocation("localhost", 1));
    excludedServers.add(new ServerLocation("localhost", 2));
    excludedServers.add(new ServerLocation("localhost", 3));
    factory.addConnection(0, 0, 1);
    factory.addConnection(0, 0, 2);
    factory.addConnection(0, 0, 3);

    LocalRegion testRegion = mock(LocalRegion.class);

    InternalPool pool = new RecoveryTestPool();
    ServerRegionProxy serverRegionProxy = new ServerRegionProxy("region", pool);

    when(testRegion.getServerProxy()).thenReturn(serverRegionProxy);
    RegionAttributes<Object, Object> regionAttributes = mock(RegionAttributes.class);
    when(testRegion.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getDataPolicy()).thenReturn(DataPolicy.DEFAULT);

    createRegisterInterestTracker(pool, testRegion);

    manager = new QueueManagerImpl(pool, endpoints, source, factory, 2,
        20, logger, ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);
    manager.setSendClientReadyInTestOnly();
    manager.clearQueueConnections();
    factory.addConnection(0, 0, 4);
    manager.recoverPrimary(excludedServers);

    assertThat(opList.get(0)).isInstanceOf(RegisterInterestListOp.RegisterInterestListOpImpl.class);
    assertThat(opList.get(1)).isInstanceOf(RegisterInterestListOp.RegisterInterestListOpImpl.class);
    assertThat(opList.get(2)).isInstanceOf(RegisterInterestListOp.RegisterInterestListOpImpl.class);
    assertThat(opList.get(3)).isInstanceOf(RegisterInterestListOp.RegisterInterestListOpImpl.class);
    assertThat(opList.get(4)).isInstanceOf(ReadyForEventsOp.ReadyForEventsOpImpl.class);
  }

  private void createRegisterInterestTracker(InternalPool localPool,
      LocalRegion localRegion) {
    final RegisterInterestTracker registerInterestTracker = localPool.getRITracker();

    final ConcurrentHashMap<String, RegisterInterestTracker.RegionInterestEntry> keysConcurrentMap =
        new ConcurrentHashMap<>();
    when(registerInterestTracker.getRegionToInterestsMap(eq(InterestType.KEY), anyBoolean(),
        anyBoolean())).thenReturn(
            keysConcurrentMap);


    for (InterestType interestType : InterestType.values()) {
      final ConcurrentHashMap<String, RegisterInterestTracker.RegionInterestEntry> concurrentMap =
          new ConcurrentHashMap<>();
      when(registerInterestTracker.getRegionToInterestsMap(eq(interestType), anyBoolean(),
          anyBoolean()))
              .thenReturn(concurrentMap);
      if (interestType.equals(InterestType.KEY)) {
        RegisterInterestTracker.RegionInterestEntry registerInterestEntry =
            new RegisterInterestTracker.RegionInterestEntry(localRegion);

        registerInterestEntry.getInterests().put("bob", InterestResultPolicy.NONE);
        concurrentMap.put("testRegion", registerInterestEntry);
      }
    }
  }

  private static void assertPortEquals(int expected, Connection actual) {
    assertThat(actual.getServer().getPort()).isEqualTo(expected);
  }

  private static void assertPortEquals(int[] expected, Iterable<Connection> actual) {
    Collection<Integer> expectedPorts = new ArrayList<>();
    for (int value : expected) {
      expectedPorts.add(value);
    }

    List<Integer> actualPorts = new ArrayList<>();
    for (Connection connection : actual) {
      actualPorts.add(connection.getServer().getPort());
    }

    assertThat(actualPorts).isEqualTo(expectedPorts);
  }

  private class RecoveryTestPool extends TestPool {
    RegisterInterestTracker registerInterestTracker = mock(RegisterInterestTracker.class);

    @Override
    public Object executeOn(Connection con, Op op) {
      opList.add(op);
      return null;
    }

    @Override
    public RegisterInterestTracker getRITracker() {
      return registerInterestTracker;
    }
  }

  private class TestPool implements InternalPool {

    @Override
    public String getPoolOrCacheCancelInProgress() {
      return null;
    }

    @Override
    public boolean getKeepAlive() {
      return false;
    }

    @Override
    public int getPrimaryPort() {
      return 0;
    }

    @Override
    public int getConnectionCount() {
      return 0;
    }

    @Override
    public Object execute(Op op, int retryAttempts) {
      return null;
    }

    @Override
    public Object execute(Op op) {
      return null;
    }

    @Override
    public EndpointManager getEndpointManager() {
      return null;
    }

    @Override
    public Object executeOn(Connection con, Op op) {
      return null;
    }

    @Override
    public Object executeOn(Connection con, Op op, boolean timeoutFatal) {
      return null;
    }

    @Override
    public Object executeOn(ServerLocation server, Op op) {
      return null;
    }

    @Override
    public Object executeOn(ServerLocation server, Op op, boolean accessed,
        boolean onlyUseExistingCnx) {
      return null;
    }

    @Override
    public void executeOnAllQueueServers(Op op)
        throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException {}

    @Override
    public Object executeOnQueuesAndReturnPrimaryResult(Op op) {
      return null;
    }

    @Override
    public Object executeOnPrimary(Op op) {
      return null;
    }

    @Override
    public boolean isDurableClient() {
      return true;
    }

    @Override
    public RegisterInterestTracker getRITracker() {
      return new RegisterInterestTracker();
    }

    @Override
    public void destroy() {
      // nothing
    }

    @Override
    public void destroy(boolean keepAlive) {
      // nothing
    }

    @Override
    public int getSocketConnectTimeout() {
      return 0;
    }

    @Override
    public int getFreeConnectionTimeout() {
      return 0;
    }

    @Override
    public int getServerConnectionTimeout() {
      return 0;
    }

    @Override
    public int getLoadConditioningInterval() {
      return 0;
    }

    @Override
    public long getIdleTimeout() {
      return 0;
    }

    @Override
    public List<InetSocketAddress> getLocators() {
      return null;
    }

    @Override
    public List<InetSocketAddress> getOnlineLocators() {
      return new ArrayList<>();
    }

    @Override
    public int getMaxConnections() {
      return 0;
    }

    @Override
    public int getMinConnections() {
      return 0;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public long getPingInterval() {
      return 0;
    }

    @Override
    public int getStatisticInterval() {
      return -1;
    }

    @Override
    public int getSubscriptionAckInterval() {
      return 5000;
    }

    @Override
    public boolean getSubscriptionEnabled() {
      return false;
    }

    @Override
    public boolean getPRSingleHopEnabled() {
      return false;
    }

    @Override
    public int getSubscriptionMessageTrackingTimeout() {
      return 0;
    }

    @Override
    public int getSubscriptionRedundancy() {
      return 0;
    }

    @Override
    public int getReadTimeout() {
      return 0;
    }

    @Override
    public int getRetryAttempts() {
      return 0;
    }

    @Override
    public String getServerGroup() {
      return null;
    }

    @Override
    public boolean getMultiuserAuthentication() {
      return false;
    }

    @Override
    public List<InetSocketAddress> getServers() {
      return null;
    }

    @Override
    public int getSocketBufferSize() {
      return 0;
    }

    @Override
    public boolean isDestroyed() {
      return false;
    }

    @Override
    public ScheduledExecutorService getBackgroundProcessor() {
      return null;
    }

    @Override
    public CancelCriterion getCancelCriterion() {
      return new CancelCriterion() {

        @Override
        public String cancelInProgress() {
          return null;
        }

        @Override
        public RuntimeException generateCancelledException(Throwable e) {
          return null;
        }
      };
    }

    @Override
    public Map getEndpointMap() {
      return null;
    }

    @Override
    public PoolStats getStats() {
      return stats;
    }

    @Override
    public void detach() {
      // nothing
    }

    @Override
    public QueryService getQueryService() {
      return null;
    }

    @Override
    public int getPendingEventCount() {
      return 0;
    }

    @Override
    public int getSubscriptionTimeoutMultiplier() {
      return 0;
    }

    @Override
    public SocketFactory getSocketFactory() {
      return null;
    }

    @Override
    public void setupServerAffinity(boolean allowFailover) {
      // nothing
    }

    @Override
    public void releaseServerAffinity() {
      // nothing
    }

    @Override
    public ServerLocation getServerAffinityLocation() {
      return null;
    }

    @Override
    public void setServerAffinityLocation(ServerLocation serverLocation) {
      // nothing
    }
  }

  /**
   * A fake factory which returns a list of connections. Fake connections are created by calling
   * addConnection or add error. The factory maintains a queue of connections which will be handed
   * out when the queue manager calls createClientToServerConnection. If an error was added, the
   * factory will return null instead.
   */
  private class TestConnectionFactory implements ConnectionFactory {

    private final LinkedList<TestConnection> nextConnections = new LinkedList<>();

    private void addError() {
      nextConnections.add(null);
    }

    private TestConnection addConnection(int endpointType, int queueSize, int port) {
      TestConnection connection = new TestConnection(endpointType, queueSize, port);
      nextConnections.add(connection);
      return connection;
    }

    @Override
    public ServerLocation findBestServer(ServerLocation currentServer, Set excludedServers) {
      return null;
    }

    @Override
    public Connection createClientToServerConnection(Set excludedServers) {
      return null;
    }

    @Override
    public ServerDenyList getDenyList() {
      return new ServerDenyList(1);
    }

    @Override
    public Connection createClientToServerConnection(ServerLocation location, boolean forQueue) {
      if (nextConnections.isEmpty()) {
        return null;
      }
      return nextConnections.removeFirst();
    }

    @Override
    public ClientUpdater createServerToClientConnection(Endpoint endpoint,
        QueueManager queueManager, boolean isPrimary, ClientUpdater failedUpdater) {
      return new ClientUpdater() {

        @Override
        public void close() {
          // nothing
        }

        @Override
        public boolean isAlive() {
          return true;
        }

        @Override
        public void join(long wait) {
          // nothing
        }

        @Override
        public void setFailedUpdater(ClientUpdater failedUpdater) {
          // nothing
        }

        @Override
        public boolean isProcessing() {
          return true;
        }

        @Override
        public boolean isPrimary() {
          return true;
        }
      };
    }
  }

  private class TestConnectionSource implements ConnectionSource {

    private final AtomicInteger nextPort = new AtomicInteger();

    @Override
    public ServerLocation findServer(Set<ServerLocation> excludedServers) {
      return new ServerLocation("localhost", nextPort.incrementAndGet());
    }

    @Override
    public ServerLocation findReplacementServer(ServerLocation currentServer,
        Set<ServerLocation> excludedServers) {
      return new ServerLocation("localhost", nextPort.incrementAndGet());
    }

    @Override
    public List<ServerLocation> findServersForQueue(Set excludedServers, int numServers,
        ClientProxyMembershipID proxyId, boolean findDurableQueue) {
      numServers = Math.min(numServers, factory.nextConnections.size());
      List<ServerLocation> locations = new ArrayList<>(numServers);
      for (int i = 0; i < numServers; i++) {
        locations.add(findServer(null));
      }
      return locations;
    }

    @Override
    public void start(InternalPool poolImpl) {
      // nothing
    }

    @Override
    public void stop() {
      // nothing
    }

    @Override
    public boolean isBalanced() {
      return false;
    }

    @Override
    public List<ServerLocation> getAllServers() {
      return emptyList();
    }

    @Override
    public List<InetSocketAddress> getOnlineLocators() {
      return emptyList();
    }
  }

  private class TestConnection implements Connection {

    private final ServerQueueStatus status;
    private final ServerLocation location;
    private final Endpoint endpoint;
    private boolean keepAlive;

    private TestConnection(int endpointType, int queueSize, int port) {
      InternalDistributedMember member = new InternalDistributedMember("localhost", 555);
      status = new ServerQueueStatus((byte) endpointType, queueSize, member, 0);
      location = new ServerLocation("localhost", port);
      endpoint = endpoints.referenceEndpoint(location, member);
    }

    @Override
    public void close(boolean keepAlive) {
      this.keepAlive = keepAlive;
    }

    @Override
    public void destroy() {
      // nothing
    }

    @Override
    public Object execute(Op op) {
      return null;
    }

    @Override
    public int getDistributedSystemId() {
      return 0;
    }

    @Override
    public ByteBuffer getCommBuffer() {
      return null;
    }

    @Override
    public Endpoint getEndpoint() {
      return endpoint;
    }

    @Override
    public ServerQueueStatus getQueueStatus() {
      return status;
    }

    @Override
    public ServerLocation getServer() {
      return location;
    }

    @Override
    public Socket getSocket() {
      return null;
    }

    @Override
    public long getBirthDate() {
      return 0;
    }

    @Override
    public void setBirthDate(long ts) {

    }

    @Override
    public ConnectionStats getStats() {
      return null;
    }

    @Override
    public boolean isActive() {
      return false;
    }

    @Override
    public boolean isDestroyed() {
      return false;
    }

    @Override
    public void emergencyClose() {
      // nothing
    }

    @Override
    public short getWanSiteVersion() {
      return -1;
    }

    @Override
    public void setWanSiteVersion(short wanSiteVersion) {
      // nothing
    }

    @Override
    public OutputStream getOutputStream() {
      return null;
    }

    @Override
    public InputStream getInputStream() {
      return null;
    }

    @Override
    public void setConnectionID(long id) {
      // nothing
    }

    @Override
    public long getConnectionID() {
      return 0;
    }
  }
}
