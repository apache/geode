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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.ToDataException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.NoAvailableLocatorsException;
import org.apache.geode.cache.client.SubscriptionNotEnabledException;
import org.apache.geode.cache.client.internal.locator.ClientConnectionRequest;
import org.apache.geode.cache.client.internal.locator.ClientConnectionResponse;
import org.apache.geode.cache.client.internal.locator.LocatorListResponse;
import org.apache.geode.cache.client.internal.locator.ServerLocationRequest;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.gms.membership.HostAddress;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.internal.cache.tier.sockets.TcpServerFactory;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListener;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.junit.categories.ClientServerTest;

@SuppressWarnings("deprecation")
@Category({ClientServerTest.class})
public class AutoConnectionSourceImplJUnitTest {

  private Cache cache;
  private int port;
  private FakeHandler handler;
  private FakePool pool = new FakePool();
  private AutoConnectionSourceImpl source;
  private TcpServer server;
  private ScheduledExecutorService background;
  private PoolStats poolStats;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    DistributedSystem ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
    poolStats = new PoolStats(ds, "pool");
    port = AvailablePortHelper.getRandomAvailableTCPPort();

    handler = new FakeHandler();
    ArrayList responseLocators = new ArrayList();
    responseLocators.add(new ServerLocation(InetAddress.getLocalHost().getHostName(), port));
    handler.nextLocatorListResponse = new LocatorListResponse(responseLocators, false);

    // very irritating, the SystemTimer requires having a distributed system
    Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, "");
    background = Executors.newSingleThreadScheduledExecutor();

    List/* <InetSocketAddress> */ locators = new ArrayList();
    InetAddress ia = InetAddress.getLocalHost();
    InetSocketAddress isa = new InetSocketAddress(ia, port);
    locators.add(isa);
    List<HostAddress> la = new ArrayList<>();
    la.add(new HostAddress(isa, ia.getHostName()));
    source = new AutoConnectionSourceImpl(locators, la, "", 60 * 1000);
    source.start(pool);
  }

  @After
  public void tearDown() {
    background.shutdownNow();
    try {
      cache.close();
    } catch (Exception e) {
      // do nothing
    }
    try {
      if (server != null && server.isAlive()) {
        try {
          new TcpClient().stop(InetAddress.getLocalHost(), port);
        } catch (ConnectException ignore) {
          // must not be running
        }
        server.join(60 * 1000);
      }
    } catch (Exception e) {
      // do nothing
    }

    try {
      InternalDistributedSystem.getAnyInstance().disconnect();
    } catch (Exception e) {
      // do nothing
    }
  }

  /**
   * This test validates the AutoConnectionSourceImpl.updateLocatorInLocatorList method. That method
   * takes InetSocketAddres of locator which unable to connect to locator. And update that
   * InetSocketAddres with hostaddress of locator in locatorlist.
   *
   * In this test we validate this using identityHashCode.
   */
  @Test
  public void testLocatorIpChange() {
    int port = 11011;
    List<InetSocketAddress> locators = new ArrayList();
    InetSocketAddress floc1 = new InetSocketAddress("fakeLocalHost1", port);
    InetSocketAddress floc2 = new InetSocketAddress("fakeLocalHost2", port);

    locators.add(floc1);
    locators.add(floc2);

    List<HostAddress> la = new ArrayList<>();
    la.add(new HostAddress(floc1, floc1.getHostName()));
    la.add(new HostAddress(floc2, floc2.getHostName()));

    AutoConnectionSourceImpl src = new AutoConnectionSourceImpl(locators, la, "", 60 * 1000);

    // This method will create a new InetSocketAddress of floc1
    src.updateLocatorInLocatorList(new HostAddress(floc1, floc1.getHostName()));

    List<InetSocketAddress> cLocList = src.getCurrentLocators();

    Assert.assertEquals(2, cLocList.size());

    Iterator<InetSocketAddress> itr = cLocList.iterator();

    while (itr.hasNext()) {
      InetSocketAddress t = itr.next();
      Assert.assertFalse("Should have replaced floc1 intsance", t == floc1);
    }
  }

  /**
   * This test validates the AutoConnectionSourceImpl.addbadLocators method. That method adds
   * badLocator from badLocator list to new Locator list. And it make sure that new locator list
   * doesn't have similar entry. For that it checks hostname and port only.
   */
  @Test
  public void testAddBadLocator() {
    int port = 11011;
    List<InetSocketAddress> locators = new ArrayList();
    InetSocketAddress floc1 = new InetSocketAddress("fakeLocalHost1", port);
    InetSocketAddress floc2 = new InetSocketAddress("fakeLocalHost2", port);
    locators.add(floc1);
    locators.add(floc2);
    List<HostAddress> la = new ArrayList<>();
    la.add(new HostAddress(floc1, floc1.getHostName()));
    la.add(new HostAddress(floc2, floc2.getHostName()));
    AutoConnectionSourceImpl src = new AutoConnectionSourceImpl(locators, la, "", 60 * 1000);


    InetSocketAddress b1 = new InetSocketAddress("fakeLocalHost1", port);
    InetSocketAddress b2 = new InetSocketAddress("fakeLocalHost3", port);

    Set<HostAddress> bla = new HashSet<>();
    bla.add(new HostAddress(b1, b1.getHostName()));
    bla.add(new HostAddress(b2, b2.getHostName()));


    src.addbadLocators(la, bla);

    System.out.println("new locatores " + la);
    Assert.assertEquals(3, la.size());
  }

  @Test
  public void testNoRespondingLocators() {
    try {
      source.findServer(null);
      fail("Should have gotten a NoAvailableLocatorsException");
    } catch (NoAvailableLocatorsException expected) {
      // do nothing
    }
  }

  @Test
  public void testSourceHandlesToDataException() throws IOException, ClassNotFoundException {
    TcpClient mockConnection = mock(TcpClient.class);
    when(mockConnection.requestToServer(isA(InetSocketAddress.class), any(Object.class),
        isA(Integer.class), isA(Boolean.class))).thenThrow(new ToDataException("testing"));
    try {
      InetSocketAddress address = new InetSocketAddress(NetworkUtils.getServerHostName(), 1234);
      source.queryOneLocatorUsingConnection(new HostAddress(address, "locator[1234]"), mock(
          ServerLocationRequest.class), mockConnection);
      verify(mockConnection).requestToServer(isA(InetSocketAddress.class),
          isA(ServerLocationRequest.class), isA(Integer.class), isA(Boolean.class));
    } catch (NoAvailableLocatorsException expected) {
      // do nothing
    }
  }

  @Test
  public void testServerLocationUsedInListenerNotification() throws Exception {
    final ClientMembershipEvent[] listenerEvents = new ClientMembershipEvent[1];

    ClientMembershipListener listener = new ClientMembershipListener() {

      @Override
      public void memberJoined(final ClientMembershipEvent event) {
        synchronized (listenerEvents) {
          listenerEvents[0] = event;
        }
      }

      @Override
      public void memberLeft(final ClientMembershipEvent event) {}

      @Override
      public void memberCrashed(final ClientMembershipEvent event) {}
    };
    InternalClientMembership.registerClientMembershipListener(listener);

    ServerLocation location = new ServerLocation("1.1.1.1", 0);

    InternalClientMembership.notifyServerJoined(location);
    await("wait for listener notification").until(() -> {
      synchronized (listenerEvents) {
        return listenerEvents[0] != null;
      }
    });

    assertNotNull(listenerEvents[0].getMember().getHost());

    InetAddress addr = InetAddress.getLocalHost();
    location = new ServerLocation(addr.getHostAddress(), 0);

    listenerEvents[0] = null;
    InternalClientMembership.notifyServerJoined(location);
    await("wait for listener notification").until(() -> {
      synchronized (listenerEvents) {
        return listenerEvents[0] != null;
      }
    });

    assertEquals(addr.getCanonicalHostName(), listenerEvents[0].getMember().getHost());
  }

  @Test
  public void testNoServers() throws Exception {
    startFakeLocator();
    handler.nextConnectionResponse = new ClientConnectionResponse(null);
    assertEquals(null, source.findServer(null));
  }

  @Test
  public void testDiscoverServers() throws Exception {
    startFakeLocator();
    ServerLocation loc1 = new ServerLocation("localhost", 4423);
    handler.nextConnectionResponse = new ClientConnectionResponse(loc1);
    assertEquals(loc1, source.findServer(null));
  }

  /**
   * This tests that discovery works even after one of two locators was shut down
   *
   */
  @Test
  public void test_DiscoverLocators_whenOneLocatorWasShutdown() throws Exception {
    startFakeLocator();
    int secondPort = AvailablePortHelper.getRandomAvailableTCPPort();
    TcpServer server2 =
        new TcpServerFactory().makeTcpServer(secondPort, InetAddress.getLocalHost(), null, null,
            handler, new FakeHelper(), "tcp server", null);
    server2.start();

    try {
      ArrayList locators = new ArrayList();
      locators.add(new ServerLocation(InetAddress.getLocalHost().getHostName(), secondPort));
      handler.nextLocatorListResponse = new LocatorListResponse(locators, false);
      Thread.sleep(500);
      try {
        new TcpClient().stop(InetAddress.getLocalHost(), port);
      } catch (ConnectException ignore) {
        // must not be running
      }
      server.join(1000);

      ServerLocation server1 = new ServerLocation("localhost", 10);
      handler.nextConnectionResponse = new ClientConnectionResponse(server1);
      assertEquals(server1, source.findServer(null));
    } finally {
      try {
        new TcpClient().stop(InetAddress.getLocalHost(), secondPort);
      } catch (ConnectException ignore) {
        // must not be running
      }
      server.join(60 * 1000);
    }
  }

  @Test
  public void testDiscoverLocatorsConnectsToLocatorsAfterTheyStartUp() throws Exception {
    ArrayList locators = new ArrayList();
    locators.add(new ServerLocation(InetAddress.getLocalHost().getHostName(), port));
    handler.nextLocatorListResponse = new LocatorListResponse(locators, false);

    try {
      await().until(() -> {
        return source.getOnlineLocators().isEmpty();
      });
      startFakeLocator();

      server.join(1000);

      await().until(() -> {

        return source.getOnlineLocators().size() == 1;
      });
    } finally {
      try {
        new TcpClient().stop(InetAddress.getLocalHost(), port);
      } catch (ConnectException ignore) {
        // must not be running
      }
      server.join(60 * 1000);
    }
  }

  @Test
  public void testSysPropLocatorUpdateInterval() throws Exception {
    long updateLocatorInterval = 543;
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "LOCATOR_UPDATE_INTERVAL",
        String.valueOf(updateLocatorInterval));
    source.start(pool);
    assertEquals(updateLocatorInterval, source.getLocatorUpdateInterval());
  }

  @Test
  public void testDefaultLocatorUpdateInterval() throws Exception {
    long updateLocatorInterval = pool.getPingInterval();
    source.start(pool);
    assertEquals(updateLocatorInterval, source.getLocatorUpdateInterval());
  }

  @Test
  public void testLocatorUpdateIntervalZero() throws Exception {
    long updateLocatorInterval = 0;
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "LOCATOR_UPDATE_INTERVAL",
        String.valueOf(updateLocatorInterval));
    source.start(pool);
    assertEquals(updateLocatorInterval, source.getLocatorUpdateInterval());
  }

  private void startFakeLocator() throws UnknownHostException, IOException, InterruptedException {
    server = new TcpServerFactory().makeTcpServer(port, InetAddress.getLocalHost(), null, null,
        handler, new FakeHelper(), "Tcp Server", null);
    server.start();
    Thread.sleep(500);
  }

  protected static class FakeHandler implements TcpHandler {
    protected volatile ClientConnectionResponse nextConnectionResponse;
    protected volatile LocatorListResponse nextLocatorListResponse;;


    public void init(TcpServer tcpServer) {}

    public Object processRequest(Object request) throws IOException {
      if (request instanceof ClientConnectionRequest) {
        return nextConnectionResponse;
      } else {
        return nextLocatorListResponse;
      }
    }

    public void shutDown() {}

    public void endRequest(Object request, long startTime) {}

    public void endResponse(Object request, long startTime) {}

    public void restarting(DistributedSystem ds, GemFireCache cache,
        InternalConfigurationPersistenceService sharedConfig) {}

  }

  public static class FakeHelper implements PoolStatHelper {

    public void endJob() {}

    public void startJob() {}

  }

  public class FakePool implements InternalPool {
    public String getPoolOrCacheCancelInProgress() {
      return null;
    }

    @Override
    public boolean getKeepAlive() {
      return false;
    }

    public EndpointManager getEndpointManager() {
      return null;
    }

    public String getName() {
      return null;
    }

    public PoolStats getStats() {
      return poolStats;
    }

    public void destroy() {

    }

    public void detach() {}

    public void destroy(boolean keepAlive) {

    }

    public boolean isDurableClient() {
      return false;
    }

    public boolean isDestroyed() {
      return false;
    }

    public int getSocketConnectTimeout() {
      return 0;
    }

    public int getFreeConnectionTimeout() {
      return 0;
    }

    public int getLoadConditioningInterval() {
      return 0;
    }

    public int getSocketBufferSize() {
      return 0;
    }

    public int getReadTimeout() {
      return 0;
    }

    public int getConnectionsPerServer() {
      return 0;
    }

    public boolean getThreadLocalConnections() {
      return false;
    }

    public boolean getSubscriptionEnabled() {
      return false;
    }

    public boolean getPRSingleHopEnabled() {
      return false;
    }

    public int getSubscriptionRedundancy() {
      return 0;
    }

    public int getSubscriptionMessageTrackingTimeout() {
      return 0;
    }

    public String getServerGroup() {
      return "";
    }

    public List/* <InetSocketAddress> */ getLocators() {
      return new ArrayList();
    }

    public List/* <InetSocketAddress> */ getOnlineLocators() {
      return new ArrayList();
    }

    public List/* <InetSocketAddress> */ getServers() {
      return new ArrayList();
    }

    public void releaseThreadLocalConnection() {}

    public ConnectionStats getStats(ServerLocation location) {
      return null;
    }

    public boolean getMultiuserAuthentication() {
      return false;
    }

    public long getIdleTimeout() {
      return 0;
    }

    public int getMaxConnections() {
      return 0;
    }

    public int getMinConnections() {
      return 0;
    }

    public long getPingInterval() {
      return 100;
    }

    public int getStatisticInterval() {
      return -1;
    }

    public int getRetryAttempts() {
      return 0;
    }

    public Object execute(Op op) {
      return null;
    }

    public Object executeOn(ServerLocation server, Op op) {
      return null;
    }

    public Object executeOn(ServerLocation server, Op op, boolean accessed,
        boolean onlyUseExistingCnx) {
      return null;
    }

    public Object executeOnPrimary(Op op) {
      return null;
    }

    public Map getEndpointMap() {
      return null;
    }

    public ScheduledExecutorService getBackgroundProcessor() {
      return background;
    }

    public Object executeOn(Connection con, Op op) {
      return null;
    }

    public Object executeOn(Connection con, Op op, boolean timeoutFatal) {
      return null;
    }

    public RegisterInterestTracker getRITracker() {
      return null;
    }

    public int getSubscriptionAckInterval() {
      return 0;
    }

    public Object executeOnQueuesAndReturnPrimaryResult(Op op) {
      return null;
    }

    public CancelCriterion getCancelCriterion() {
      return new CancelCriterion() {

        public String cancelInProgress() {
          return null;
        }

        public RuntimeException generateCancelledException(Throwable e) {
          return null;
        }

      };
    }

    public void executeOnAllQueueServers(Op op)
        throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException {

    }

    public Object execute(Op op, int retryAttempts) {
      return null;
    }

    public QueryService getQueryService() {
      return null;
    }

    public int getPendingEventCount() {
      return 0;
    }

    @Override
    public int getSubscriptionTimeoutMultiplier() {
      return 0;
    }

    public RegionService createAuthenticatedCacheView(Properties properties) {
      return null;
    }

    public void setupServerAffinity(boolean allowFailover) {}

    public void releaseServerAffinity() {}

    public ServerLocation getServerAffinityLocation() {
      return null;
    }

    public void setServerAffinityLocation(ServerLocation serverLocation) {}
  }
}
