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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.cache.client.SubscriptionNotEnabledException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class QueueManagerJUnitTest {

  private static final String expectedRedundantErrorMsg =
      "Could not find any server to host redundant client queue.";
  private static final String expectedPrimaryErrorMsg =
      "Could not find any server to host primary client queue.";

  private DummyPool pool;
  private LocalLogWriter logger;
  private DistributedSystem ds;
  private EndpointManagerImpl endpoints;
  private DummySource source;
  private DummyFactory factory;
  private QueueManager manager;
  private ScheduledExecutorService background;
  private PoolStats stats;

  @Before
  public void setUp() {
    this.logger = new LocalLogWriter(FINEST.intLevel(), System.out);
    Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, "");
    ds = DistributedSystem.connect(properties);
    stats = new PoolStats(ds, "QueueManagerJUnitTest");
    pool = new DummyPool();
    endpoints = new EndpointManagerImpl("pool", ds, ds.getCancelCriterion(), pool.getStats());
    source = new DummySource();
    factory = new DummyFactory();
    background = Executors.newSingleThreadScheduledExecutor();
    final String addExpectedPEM =
        "<ExpectedException action=add>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final String addExpectedREM =
        "<ExpectedException action=add>" + expectedRedundantErrorMsg + "</ExpectedException>";
    ds.getLogWriter().info(addExpectedPEM);
    ds.getLogWriter().info(addExpectedREM);
  }

  @After
  public void tearDown() {
    background.shutdownNow();
    manager.close(false);
    endpoints.close();
    final String removeExpectedPEM =
        "<ExpectedException action=remove>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final String removeExpectedREM =
        "<ExpectedException action=remove>" + expectedRedundantErrorMsg + "</ExpectedException>";

    ds.getLogWriter().info(removeExpectedPEM);
    ds.getLogWriter().info(removeExpectedREM);

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
    // so we need to wait for the intitialization to finish rather than counting on the
    // manager.getAllConnections()
    // to wait for a primary
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 30 * 1000;) {
        Thread.sleep(200);
        try {
          assertPortEquals(2, manager.getAllConnections().getPrimary());
          assertPortEquals(new int[] {1, 3}, manager.getAllConnections().getBackups());
          manager.close(false);
          done = true;
        } catch (AssertionError ignored) {
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue(done);

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
    done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 30 * 1000;) {
        Thread.sleep(200);
        try {
          assertPortEquals(1, manager.getAllConnections().getPrimary());
          assertPortEquals(new int[] {2, 3}, manager.getAllConnections().getBackups());
          done = true;
        } catch (AssertionError ignored) {
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue(done);
  }

  @Test
  public void testMakeNewPrimary() throws Exception {
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

    // TODO - use a listener
    Thread.sleep(100);
    assertPortEquals(new int[] {3, 4, 5}, manager.getAllConnections().getBackups());
  }

  @Test
  public void testWatchForNewRedundant() throws Exception {
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
    // TODO - use a listener
    Thread.sleep(100);
    assertPortEquals(new int[] {2, 3}, manager.getAllConnections().getBackups());

    Connection backup1 = manager.getAllConnections().getBackups().get(0);
    backup1.destroy();

    assertPortEquals(1, manager.getAllConnections().getPrimary());
    // TODO - use a listener
    Thread.sleep(100);
    assertPortEquals(new int[] {3, 4}, manager.getAllConnections().getBackups());
  }

  @Test
  public void testWaitForPrimary() {
    factory.addConnection(0, 0, 1);
    manager = new QueueManagerImpl(pool, endpoints, source, factory, 2, 20, logger,
        ClientProxyMembershipID.getNewProxyMembership(ds));
    manager.start(background);
    manager.getAllConnections().getPrimary().destroy();

    try {
      manager.getAllConnections().getPrimary();
      fail("Should have received NoQueueServersAvailableException");
    } catch (NoSubscriptionServersAvailableException expected) {
      // do thing
    }
    factory.addConnection(0, 0, 2);
    factory.addConnection(0, 0, 3);

    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 11 * 1000;) {
        Thread.sleep(200);
        try {
          manager.getAllConnections();
          done = true;
        } catch (NoSubscriptionServersAvailableException e) {
          // done = false;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("getAllConnections still throwing NoSubscriptionServersAvailableException", done);

    assertPortEquals(2, manager.getAllConnections().getPrimary());
  }

  private static void assertPortEquals(int expected, Connection actual) {
    assertEquals(expected, actual.getServer().getPort());
  }

  private static void assertPortEquals(int[] expected, List<Connection> actual) {
    ArrayList<Integer> expectedPorts = new ArrayList<>();
    for (int value : expected) {
      expectedPorts.add(value);
    }
    ArrayList<Integer> actualPorts = new ArrayList<>();
    for (Connection connection : actual) {
      actualPorts.add(connection.getServer().getPort());
    }

    assertEquals(expectedPorts, actualPorts);
  }

  private class DummyPool implements InternalPool {

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
    public void destroy() {}

    @Override
    public void destroy(boolean keepAlive) {}

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
    public void detach() {}

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
    public void setupServerAffinity(boolean allowFailover) {}

    @Override
    public void releaseServerAffinity() {}

    @Override
    public ServerLocation getServerAffinityLocation() {
      return null;
    }

    @Override
    public void setServerAffinityLocation(ServerLocation serverLocation) {}

  }

  /**
   * A fake factory which returns a list of connections. Fake connections are created by calling
   * addConnection or add error. The factory maintains a queue of connections which will be handed
   * out when the queue manager calls createClientToServerConnection. If a error was added, the
   * factory will return null instead.
   */
  private class DummyFactory implements ConnectionFactory {

    LinkedList<DummyConnection> nextConnections = new LinkedList<>();

    void addError() {
      nextConnections.add(null);
    }

    void addConnection(int endpointType, int queueSize, int port) {
      nextConnections.add(new DummyConnection(endpointType, queueSize, port));
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
      if (nextConnections == null || nextConnections.isEmpty()) {
        return null;
      }
      return nextConnections.removeFirst();
    }

    @Override
    public ClientUpdater createServerToClientConnection(Endpoint endpoint,
        QueueManager queueManager, boolean isPrimary, ClientUpdater failedUpdater) {
      return new ClientUpdater() {
        @Override
        public void close() {}

        @Override
        public boolean isAlive() {
          return true;
        }

        @Override
        public void join(long wait) {}

        @Override
        public void setFailedUpdater(ClientUpdater failedUpdater) {}

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

  private class DummySource implements ConnectionSource {
    int nextPort = 0;

    @Override
    public ServerLocation findServer(Set<ServerLocation> excludedServers) {
      return new ServerLocation("localhost", nextPort++);
    }

    @Override
    public ServerLocation findReplacementServer(ServerLocation currentServer,
        Set<ServerLocation> excludedServers) {
      return new ServerLocation("localhost", nextPort++);
    }

    @Override
    public List<ServerLocation> findServersForQueue(Set excludedServers, int numServers,
        ClientProxyMembershipID proxyId, boolean findDurableQueue) {
      numServers =
          numServers > factory.nextConnections.size() ? factory.nextConnections.size() : numServers;
      ArrayList<ServerLocation> locations = new ArrayList<>(numServers);
      for (int i = 0; i < numServers; i++) {
        locations.add(findServer(null));
      }
      return locations;
    }

    @Override
    public void start(InternalPool poolImpl) {}

    @Override
    public void stop() {}

    @Override
    public boolean isBalanced() {
      return false;
    }

    @Override
    public List<ServerLocation> getAllServers() {
      return Collections.emptyList();
    }

    @Override
    public List<InetSocketAddress> getOnlineLocators() {
      return Collections.emptyList();
    }
  }

  private class DummyConnection implements Connection {

    private ServerQueueStatus status;
    private ServerLocation location;
    private Endpoint endpoint;

    DummyConnection(int endpointType, int queueSize, int port) {
      InternalDistributedMember member = new InternalDistributedMember("localhost", 555);
      this.status = new ServerQueueStatus((byte) endpointType, queueSize, member, 0);
      this.location = new ServerLocation("localhost", port);
      this.endpoint = endpoints.referenceEndpoint(location, member);
    }

    @Override
    public void close(boolean keepAlive) {}

    @Override
    public void destroy() {}

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
    public ConnectionStats getStats() {
      return null;
    }

    @Override
    public boolean isDestroyed() {
      return false;
    }

    @Override
    public void emergencyClose() {}

    @Override
    public short getWanSiteVersion() {
      return -1;
    }

    @Override
    public void setWanSiteVersion(short wanSiteVersion) {}

    @Override
    public OutputStream getOutputStream() {
      return null;
    }

    @Override
    public InputStream getInputStream() {
      return null;
    }

    @Override
    public void setConnectionID(long id) {}

    @Override
    public long getConnectionID() {
      return 0;
    }
  }

}
