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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.locator.ClientConnectionRequest;
import org.apache.geode.cache.client.internal.locator.ClientConnectionResponse;
import org.apache.geode.cache.client.internal.locator.QueueConnectionRequest;
import org.apache.geode.cache.client.internal.locator.QueueConnectionResponse;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.cache.server.ServerLoadProbeAdapter;
import org.apache.geode.cache.server.ServerMetrics;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class LocatorLoadBalancingDUnitTest extends LocatorTestBase {

  /**
   * The number of connections that we can be off by in the balancing tests We need this little
   * fudge factor, because the locator can receive an update from the cache server after it has made
   * incremented its counter for a client connection, but the client hasn't connected yet. This
   * wipes out the estimation on the locator. This means that we may be slighly off in our balance.
   * <p>
   * TODO grid fix this hole in the locator.
   */
  private static final int ALLOWABLE_ERROR_IN_COUNT = 1;
  protected static final long MAX_WAIT = 60000;

  public LocatorLoadBalancingDUnitTest() {
    super();
  }

  /**
   * Test the locator discovers a cache server and is initialized with the correct load for that
   * cache server.
   */
  @Test
  public void testDiscovery() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    String hostName = NetworkUtils.getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));

    String locators = getLocatorString(hostName, locatorPort);

    int serverPort = vm1.invoke("Start BridgeServer",
        () -> startBridgeServer(new String[] {"a", "b"}, locators));

    ServerLoad expectedLoad = new ServerLoad(0f, 1 / 800.0f, 0f, 1f);
    ServerLocation expectedLocation = new ServerLocation(hostName, serverPort);
    Map expected = new HashMap();
    expected.put(expectedLocation, expectedLoad);

    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));

    int serverPort2 = vm2.invoke("Start BridgeServer",
        () -> startBridgeServer(new String[] {"a", "b"}, locators));

    ServerLocation expectedLocation2 = new ServerLocation(hostName, serverPort2);

    expected.put(expectedLocation2, expectedLoad);
    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));
  }

  /**
   * Test that the locator will properly estimate the load for servers when it receives connection
   * requests.
   */
  @Test
  public void testEstimation() throws IOException, ClassNotFoundException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    String hostName = NetworkUtils.getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));
    String locators = getLocatorString(hostName, locatorPort);

    int serverPort = vm1.invoke("Start BridgeServer",
        () -> startBridgeServer(new String[] {"a", "b"}, locators));

    ServerLoad expectedLoad = new ServerLoad(2 / 800f, 1 / 800.0f, 0f, 1f);
    ServerLocation expectedLocation = new ServerLocation(hostName, serverPort);
    Map expected = new HashMap();
    expected.put(expectedLocation, expectedLoad);

    SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));
    ClientConnectionResponse response;
    response = issueClientConnectionRequest(hostName, locatorPort,
        new ClientConnectionRequest(Collections.EMPTY_SET, null), true);
    Assert.assertEquals(expectedLocation, response.getServer());

    response = issueClientConnectionRequest(hostName, locatorPort,
        new ClientConnectionRequest(Collections.EMPTY_SET, null), true);
    Assert.assertEquals(expectedLocation, response.getServer());

    // we expect that the connection load load will be 2 * the loadPerConnection
    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));

    QueueConnectionResponse response2;
    response2 = issueQueueConnectionRequest(hostName, locatorPort, 2);
    Assert.assertEquals(Collections.singletonList(expectedLocation), response2.getServers());

    response2 = issueQueueConnectionRequest(hostName, locatorPort, 5);
    Assert.assertEquals(Collections.singletonList(expectedLocation), response2.getServers());

    // we expect that the queue load will increase by 2
    expectedLoad.setSubscriptionConnectionLoad(2f);
    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));
  }

  private ClientConnectionResponse issueClientConnectionRequest(final String hostName,
      final int locatorPort,
      final ClientConnectionRequest request,
      final boolean replyExpected)
      throws IOException, ClassNotFoundException {
    return (ClientConnectionResponse) issueRequest(hostName, locatorPort, request, replyExpected);
  }

  private QueueConnectionResponse issueQueueConnectionRequest(final String hostName,
      final int locatorPort,
      final int redundantCopies)
      throws IOException, ClassNotFoundException {
    return (QueueConnectionResponse) issueRequest(hostName, locatorPort,
        new QueueConnectionRequest(null, redundantCopies, Collections.EMPTY_SET, null, false),
        true);
  }

  private Object issueRequest(final String hostName, final int locatorPort,
      final Object request, final boolean replyExpected)
      throws IOException, ClassNotFoundException {
    return new TcpClient(SocketCreatorFactory
        .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT)
            .requestToServer(new HostAndPort(hostName,
                locatorPort),
                request,
                10000, replyExpected);
  }

  /**
   * Test to make sure the cache servers communicate their updated load to the controller when the
   * load on the cache server changes.
   */
  @Test
  public void testLoadMessaging() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    String hostName = NetworkUtils.getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));
    String locators = getLocatorString(hostName, locatorPort);

    final int serverPort = vm1.invoke("Start BridgeServer",
        () -> startBridgeServer(new String[] {"a", "b"}, locators));

    // We expect 0 load
    Map expected = new HashMap();
    ServerLocation expectedLocation = new ServerLocation(hostName, serverPort);
    ServerLoad expectedLoad = new ServerLoad(0f, 1 / 800.0f, 0f, 1f);
    expected.put(expectedLocation, expectedLoad);
    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));
    vm2.invoke("StartBridgeClient", () -> {
      PoolFactoryImpl pf = new PoolFactoryImpl(null);
      pf.addServer(NetworkUtils.getServerHostName(host), serverPort);
      pf.setMinConnections(8);
      pf.setMaxConnections(8);
      pf.setSubscriptionEnabled(true);
      startBridgeClient(pf.getPoolAttributes(), new String[] {REGION_NAME});
      return null;
    });

    // We expect 8 client to server connections. The queue requires
    // an additional client to server connection, but that shouldn't show up here.
    expectedLoad = new ServerLoad(8 / 800f, 1 / 800.0f, 1f, 1f);
    expected.put(expectedLocation, expectedLoad);

    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));

    stopBridgeMemberVM(vm2);

    // Now we expect 0 load
    expectedLoad = new ServerLoad(0f, 1 / 800.0f, 0f, 1f);
    expected.put(expectedLocation, expectedLoad);
    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));
  }

  /**
   * Test to make sure that the locator balancing load between two servers.
   */
  @Test
  public void testBalancing() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    String hostName = NetworkUtils.getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));
    String locators = getLocatorString(hostName, locatorPort);

    vm1.invoke("Start BridgeServer", () -> startBridgeServer(new String[] {"a", "b"}, locators));
    vm2.invoke("Start BridgeServer", () -> startBridgeServer(new String[] {"a", "b"}, locators));

    vm3.invoke("StartBridgeClient", () -> {
      PoolFactoryImpl pf = new PoolFactoryImpl(null);
      pf.addLocator(hostName, locatorPort);
      pf.setMinConnections(80);
      pf.setMaxConnections(80);
      pf.setSubscriptionEnabled(false);
      pf.setIdleTimeout(-1);
      startBridgeClient(pf.getPoolAttributes(), new String[] {REGION_NAME});
      return null;
    });

    vm3.invoke("waitForPrefilledConnections", () -> waitForPrefilledConnections(80));

    vm1.invoke("check connection count", () -> checkConnectionCount(40));
    vm2.invoke("check connection count", () -> checkConnectionCount(40));
  }

  private void checkConnectionCount(final int count) {
    Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
    final CacheServerImpl server = (CacheServerImpl) cache.getCacheServers().get(0);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      int sz = server.getAcceptor().getStats().getCurrentClientConnections();
      if (Math.abs(sz - count) <= ALLOWABLE_ERROR_IN_COUNT) {
        return true;
      }
      System.out.println("Found " + sz + " connections, expected " + count);
      return false;
    });
  }

  private void waitForPrefilledConnections(final int count) throws Exception {
    waitForPrefilledConnections(count, POOL_NAME);
  }

  private void waitForPrefilledConnections(final int count, final String poolName)
      throws Exception {
    final PoolImpl pool = (PoolImpl) PoolManager.getAll().get(poolName);
    await().timeout(300, TimeUnit.SECONDS)
        .until(() -> pool.getConnectionCount() >= count);
  }

  /**
   * Test that the locator balances load between three servers with intersecting server groups.
   * Server: 1 2 3 Groups: a a,b b
   */
  @Test
  public void testIntersectingServerGroups() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    String hostName = NetworkUtils.getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));
    String locators = getLocatorString(hostName, locatorPort);

    int serverPort1 =
        vm1.invoke("Start BridgeServer", () -> startBridgeServer(new String[] {"a"}, locators));
    vm2.invoke("Start BridgeServer", () -> startBridgeServer(new String[] {"a", "b"}, locators));
    vm3.invoke("Start BridgeServer", () -> startBridgeServer(new String[] {"b"}, locators));

    PoolFactoryImpl pf = new PoolFactoryImpl(null);
    pf.addLocator(NetworkUtils.getServerHostName(host), locatorPort);
    pf.setMinConnections(12);
    pf.setSubscriptionEnabled(false);
    pf.setServerGroup("a");
    pf.setIdleTimeout(-1);
    startBridgeClient(pf.getPoolAttributes(), new String[] {REGION_NAME});
    waitForPrefilledConnections(12);

    vm1.invoke("Check Connection Count", () -> checkConnectionCount(6));
    vm2.invoke("Check Connection Count", () -> checkConnectionCount(6));
    vm3.invoke("Check Connection Count", () -> checkConnectionCount(0));

    LogWriterUtils.getLogWriter().info("pool1 prefilled");

    PoolFactoryImpl pf2 = (PoolFactoryImpl) PoolManager.createFactory();
    pf2.init(pf.getPoolAttributes());
    pf2.setServerGroup("b");
    PoolImpl pool2 = (PoolImpl) pf2.create("testPool2");
    waitForPrefilledConnections(12, "testPool2");

    // The load will not be perfect, because we created all of the connections
    // for group A first.
    vm1.invoke("Check Connection Count", () -> checkConnectionCount(6));
    vm2.invoke("Check Connection Count", () -> checkConnectionCount(9));
    vm3.invoke("Check Connection Count", () -> checkConnectionCount(9));

    LogWriterUtils.getLogWriter().info("pool2 prefilled");

    ServerLocation location1 =
        new ServerLocation(NetworkUtils.getServerHostName(host), serverPort1);
    PoolImpl pool1 = (PoolImpl) PoolManager.getAll().get(POOL_NAME);
    Assert.assertEquals("a", pool1.getServerGroup());

    // Use up all of the pooled connections on pool1, and acquire 3 more
    for (int i = 0; i < 15; i++) {
      pool1.acquireConnection();
    }

    LogWriterUtils.getLogWriter().info("aquired 15 connections in pool1");

    // now the load should be equal
    vm1.invoke("Check Connection Count", () -> checkConnectionCount(9));
    vm2.invoke("Check Connection Count", () -> checkConnectionCount(9));
    vm3.invoke("Check Connection Count", () -> checkConnectionCount(9));

    // use up all of the pooled connections on pool2
    for (int i = 0; i < 12; i++) {
      pool2.acquireConnection();
    }

    LogWriterUtils.getLogWriter().info("aquired 12 connections in pool2");

    // interleave creating connections in both pools
    for (int i = 0; i < 6; i++) {
      pool1.acquireConnection();
      pool2.acquireConnection();
    }

    LogWriterUtils.getLogWriter()
        .info("interleaved 6 connections from pool1 with 6 connections from pool2");

    // The load should still be balanced
    vm1.invoke("Check Connection Count", () -> checkConnectionCount(13));
    vm2.invoke("Check Connection Count", () -> checkConnectionCount(13));
    vm3.invoke("Check Connection Count", () -> checkConnectionCount(13));

  }

  @Test
  public void testCustomLoadProbe() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    // VM vm3 = host.getVM(3);

    String hostName = NetworkUtils.getServerHostName(vm0.getHost());
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));
    String locators = getLocatorString(hostName, locatorPort);

    final ServerLoad load1 = new ServerLoad(.3f, .01f, .44f, 4564f);
    final ServerLoad load2 = new ServerLoad(23.2f, 1.1f, 22.3f, .3f);
    int serverPort1 = vm1.invoke("Start BridgeServer", () -> startBridgeServer(null, locators,
        new String[] {REGION_NAME}, new MyLoadProbe(load1), false));
    int serverPort2 = vm2.invoke("Start BridgeServer", () -> startBridgeServer(null, locators,
        new String[] {REGION_NAME}, new MyLoadProbe(load2), false));

    HashMap expected = new HashMap();
    ServerLocation l1 = new ServerLocation(NetworkUtils.getServerHostName(host), serverPort1);
    ServerLocation l2 = new ServerLocation(NetworkUtils.getServerHostName(host), serverPort2);
    expected.put(l1, load1);
    expected.put(l2, load2);
    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));

    load1.setConnectionLoad(25f);
    vm1.invoke("changeLoad", () -> changeLoad(load1));
    load2.setSubscriptionConnectionLoad(3.5f);
    vm2.invoke("changeLoad", () -> changeLoad(load2));
    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));

    final ServerLoad load1Updated = new ServerLoad(1f, .1f, 0f, 1f);
    final ServerLoad load2Updated = new ServerLoad(2f, 5f, 0f, 2f);
    expected.put(l1, load1Updated);
    expected.put(l2, load2Updated);
    vm1.invoke("changeLoad", () -> changeLoad(load1Updated));
    vm2.invoke("changeLoad", () -> changeLoad(load2Updated));
    vm0.invoke("check Locator Load", () -> checkLocatorLoad(expected));

    PoolFactoryImpl pf = new PoolFactoryImpl(null);
    pf.addLocator(NetworkUtils.getServerHostName(host), locatorPort);
    pf.setMinConnections(20);
    pf.setSubscriptionEnabled(true);
    pf.setIdleTimeout(-1);
    startBridgeClient(pf.getPoolAttributes(), new String[] {REGION_NAME});
    waitForPrefilledConnections(20);

    // The first 10 connection should to go vm1, then 1 to vm2, then another 9 to vm1
    // because have unequal values for loadPerConnection
    vm1.invoke("Check Connection Count", () -> checkConnectionCount(19));
    vm2.invoke("Check Connection Count", () -> checkConnectionCount(1));
  }

  public void checkLocatorLoad(final Map expected) {
    List locators = Locator.getLocators();
    Assert.assertEquals(1, locators.size());
    InternalLocator locator = (InternalLocator) locators.get(0);
    final ServerLocator sl = locator.getServerLocatorAdvisee();
    sl.getDistributionAdvisor().dumpProfiles("PROFILES= ");
    await().timeout(300, TimeUnit.SECONDS)
        .until(() -> expected.equals(sl.getLoadMap()));
  }

  private void changeLoad(final ServerLoad newLoad) {
    Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
    CacheServer server = cache.getCacheServers().get(0);
    MyLoadProbe probe = (MyLoadProbe) server.getLoadProbe();
    probe.setLoad(newLoad);
  }

  private static class MyLoadProbe extends ServerLoadProbeAdapter implements Serializable {
    private ServerLoad load;

    public MyLoadProbe(ServerLoad load) {
      this.load = load;
    }

    @Override
    public ServerLoad getLoad(ServerMetrics metrics) {
      float connectionLoad =
          load.getConnectionLoad() + metrics.getConnectionCount() * load.getLoadPerConnection();
      float queueLoad = load.getSubscriptionConnectionLoad()
          + metrics.getSubscriptionConnectionCount() * load.getLoadPerSubscriptionConnection();
      return new ServerLoad(connectionLoad, load.getLoadPerConnection(), queueLoad,
          load.getLoadPerSubscriptionConnection());
    }

    public void setLoad(ServerLoad load) {
      this.load = load;
    }
  }
}
