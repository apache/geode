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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests for durable reconnect issue
 * 
 * @since GemFire 5.2
 */
@Category(DistributedTest.class)
public class DurableClientReconnectDUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  private static VM server1 = null;
  private static VM server2 = null;
  private static VM server3 = null;
  private static VM server4 = null;

  private static PoolImpl pool = null;
  private static Connection conn = null;

  private static Integer PORT1;
  private static Integer PORT2;
  private static Integer PORT3;
  private static Integer PORT4;
  private static String SERVER1;
  private static String SERVER2;
  private static String SERVER3;
  private static String SERVER4;

  private static final String REGION_NAME =
      DurableClientReconnectDUnitTest.class.getSimpleName() + "_region";

  private DurableClientReconnectDUnitTest instance = null;

  @BeforeClass
  public static void caseSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
    server4 = host.getVM(3);

    // start servers first
    PORT1 = ((Integer) server1.invoke(() -> DurableClientReconnectDUnitTest.createServerCache()));
    PORT2 = ((Integer) server2.invoke(() -> DurableClientReconnectDUnitTest.createServerCache()));
    PORT3 = ((Integer) server3.invoke(() -> DurableClientReconnectDUnitTest.createServerCache()));
    PORT4 = ((Integer) server4.invoke(() -> DurableClientReconnectDUnitTest.createServerCache()));
    SERVER1 = NetworkUtils.getServerHostName(host) + PORT1;
    SERVER2 = NetworkUtils.getServerHostName(host) + PORT2;
    SERVER3 = NetworkUtils.getServerHostName(host) + PORT3;
    SERVER4 = NetworkUtils.getServerHostName(host) + PORT4;

    // CacheServerTestUtil.disableShufflingOfEndpoints();
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "false");
  }

  @Test
  public void testDurableReconnectSingleServer() throws Exception {
    createCacheClientAndConnectToSingleServer(NetworkUtils.getServerHostName(Host.getHost(0)), 0);
    List redundantServers = pool.getRedundantNames();
    String primaryName = pool.getPrimaryName();
    assertTrue(redundantServers.isEmpty());
    closeCache(true);

    // Wait for server to cleanup client resources
    // temporary fix for bug 38345.
    Wait.pause(2000);

    createCacheClientAndConnectToSingleServer(NetworkUtils.getServerHostName(Host.getHost(0)), 0);
    List redundantServers2 = pool.getRedundantNames();
    String primaryName2 = pool.getPrimaryName();
    assertTrue(redundantServers2.isEmpty());
    assertTrue(primaryName2.equals(primaryName));
  }

  @Test
  public void testDurableReconnectSingleServerWithZeroConnPerServer() throws Exception {
    createCacheClientAndConnectToSingleServerWithZeroConnPerServer(
        NetworkUtils.getServerHostName(Host.getHost(0)), 0);
    List redundantServers = pool.getRedundantNames();
    String primaryName = pool.getPrimaryName();
    assertTrue(redundantServers.isEmpty());
    closeCache(true);

    createCacheClientAndConnectToSingleServerWithZeroConnPerServer(
        NetworkUtils.getServerHostName(Host.getHost(0)), 0);
    List redundantServers2 = pool.getRedundantNames();
    String primaryName2 = pool.getPrimaryName();
    assertTrue(redundantServers2.isEmpty());
    assertTrue(primaryName2.equals(primaryName));
  }

  @Test
  public void testDurableReconnectNonHA() throws Exception {
    createCacheClient(0);
    List redundantServers = pool.getRedundantNames();
    String primaryName = pool.getPrimaryName();
    assertTrue(redundantServers.isEmpty());
    closeCache(true);

    // Wait for server to cleanup client resources
    // temporary fix for bug 38345.
    Wait.pause(2000);

    createCacheClient(0);
    List redundantServers2 = pool.getRedundantNames();
    String primaryName2 = pool.getPrimaryName();
    assertTrue(redundantServers2.isEmpty());
    assertTrue(primaryName2.equals(primaryName));
  }

  /**
   * (R = 1 ) , four servers , all Servers are up, Check client reconnect to either of server having
   * queue.
   */
  @Test
  public void testDurableReconnect() throws Exception {
    // create client cache and Send clientReady message
    createCacheClient();
    HashSet redundantServers = new HashSet(pool.getRedundantNames());
    redundantServers.add(pool.getPrimaryName());

    instance.determineAndVerfiyRedundantServers(redundantServers);
    instance.determineAndVerfiyNonRedundantServers(redundantServers);

    // Stop the durable client
    closeCache(true);

    // Wait for server to cleanup client resources
    // temporary fix for bug 38345.
    Wait.pause(2000);

    createCacheClient();

    HashSet redundantServersAfterReconnect = new HashSet(pool.getRedundantNames());
    redundantServersAfterReconnect.add(pool.getPrimaryName());

    instance.determineAndVerfiyRedundantServers(redundantServersAfterReconnect);
    instance.determineAndVerfiyNonRedundantServers(redundantServersAfterReconnect);

    assertTrue(redundantServers.equals(redundantServersAfterReconnect));
  }

  @Test
  public void testDurableReconnect_DifferentPrimary() throws Exception {
    // create client cache and Send clientReady message
    createCacheClient();
    HashSet redundantServers = new HashSet(pool.getRedundantNames());
    String primaryBefore = pool.getPrimaryName();
    redundantServers.add(primaryBefore);
    instance.determineAndVerfiyRedundantServers(redundantServers);
    instance.determineAndVerfiyNonRedundantServers(redundantServers);

    // Stop the durable client
    closeCache(true);

    // Wait for server to cleanup client resources
    // temporary fix for bug 38345.
    Wait.pause(2000);

    createCacheClient();

    HashSet redundantServersAfterReconnect = new HashSet(pool.getRedundantNames());
    String primaryAfter = pool.getPrimaryName();
    redundantServersAfterReconnect.add(primaryAfter);
    instance.determineAndVerfiyRedundantServers(redundantServersAfterReconnect);
    instance.determineAndVerfiyNonRedundantServers(redundantServersAfterReconnect);

    assertTrue(redundantServers.equals(redundantServersAfterReconnect));
    assertFalse(primaryBefore.equals(primaryAfter));
  }

  @Test
  public void testDurableReconnectWithOneRedundantServerDown() throws Exception {
    // create client cache and Send clientReady message
    createCacheClient();
    List redundantServers = pool.getRedundantNames();
    redundantServers.add(pool.getPrimaryName());
    assertTrue(redundantServers.size() == 2);

    instance.determineAndVerfiyRedundantServers(redundantServers);
    instance.determineAndVerfiyNonRedundantServers(redundantServers);

    // Stop the durable client
    closeCache(true);

    Object serverArray[] = redundantServers.toArray();
    String rServer1 = (String) serverArray[0];
    String rServer2 = (String) serverArray[1];

    instance.closeServer(rServer1);

    createCacheClient();

    List redundantServersAfterReconnect = pool.getRedundantNames();
    redundantServersAfterReconnect.add(pool.getPrimaryName());

    instance.determineAndVerfiyRedundantServers(redundantServersAfterReconnect);

    List redundantServersHistory = new ArrayList();
    redundantServersHistory.addAll(redundantServersAfterReconnect);
    redundantServersHistory.add(rServer1);
    instance.determineAndVerfiyNonRedundantServers(redundantServersHistory);

    assertFalse(redundantServers.equals(redundantServersAfterReconnect));
    assertTrue(redundantServersAfterReconnect.size() == 2);
    assertFalse(redundantServersAfterReconnect.contains(rServer1));
    assertTrue(redundantServersAfterReconnect.contains(rServer2));
  }

  @Test
  public void testDurableReconnectWithBothRedundantServersDown() throws Exception {
    // create client cache and Send clientReady message
    createCacheClient();
    List redundantServers = pool.getRedundantNames();
    redundantServers.add(pool.getPrimaryName());
    assertTrue(redundantServers.size() == 2);

    instance.determineAndVerfiyRedundantServers(redundantServers);
    instance.determineAndVerfiyNonRedundantServers(redundantServers);

    // Stop the durable client
    closeCache(true);

    Object serverArray[] = redundantServers.toArray();
    String rServer1 = (String) serverArray[0];
    String rServer2 = (String) serverArray[1];

    instance.closeServer(rServer1);
    instance.closeServer(rServer2);

    createCacheClient();

    List redundantServersAfterReconnect = pool.getRedundantNames();
    if (redundantServersAfterReconnect.isEmpty()) {
      redundantServersAfterReconnect = new LinkedList();
    }
    redundantServersAfterReconnect.add(pool.getPrimaryName());


    List redundantServersHistory = new ArrayList();
    redundantServersHistory.addAll(redundantServersAfterReconnect);
    redundantServersHistory.add(rServer1);
    redundantServersHistory.add(rServer2);
    instance.determineAndVerfiyNonRedundantServers(redundantServersHistory);

    assertFalse(redundantServers.equals(redundantServersAfterReconnect));
    assertTrue(redundantServersAfterReconnect.size() == 2);

    assertFalse(redundantServersAfterReconnect.contains(rServer1));
    assertFalse(redundantServersAfterReconnect.contains(rServer2));
  }

  @Test
  public void testDurableReconnectWithBothNonRedundantServersDown() throws Exception {
    // create client cache and Send clientReady message
    createCacheClient();
    HashSet redundantServers = new HashSet(pool.getRedundantNames());
    redundantServers.add(pool.getPrimaryName());
    assertTrue(redundantServers.size() == 2);
    instance.determineAndVerfiyRedundantServers(redundantServers);
    instance.determineAndVerfiyNonRedundantServers(redundantServers);

    // Stop the durable client
    closeCache(true);

    Set nonRedundantSet = new HashSet();
    nonRedundantSet.add(SERVER1);
    nonRedundantSet.add(SERVER2);
    nonRedundantSet.add(SERVER3);
    nonRedundantSet.add(SERVER4);

    nonRedundantSet.removeAll(redundantServers);

    Object serverArray[] = nonRedundantSet.toArray();
    String rServer1 = (String) serverArray[0];
    String rServer2 = (String) serverArray[1];

    // can see sporadic socket closed exceptions
    final IgnoredException expectedEx =
        IgnoredException.addIgnoredException(SocketException.class.getName());

    instance.closeServer(rServer1);
    instance.closeServer(rServer2);

    createCacheClient();

    HashSet redundantServersAfterReconnect = new HashSet(pool.getRedundantNames());
    redundantServersAfterReconnect.add(pool.getPrimaryName());

    List redundantServersHistory = new ArrayList();
    redundantServersHistory.addAll(redundantServersAfterReconnect);
    redundantServersHistory.add(rServer1);
    redundantServersHistory.add(rServer2);
    instance.determineAndVerfiyNonRedundantServers(redundantServersHistory);

    expectedEx.remove();

    assertTrue(redundantServers.equals(redundantServersAfterReconnect));
    assertTrue(redundantServersAfterReconnect.size() == 2);

    assertFalse("redundantServersAfterReconnect contains " + rServer1,
        redundantServersAfterReconnect.contains(rServer1));
    assertFalse("redundantServersAfterReconnect contains " + rServer2,
        redundantServersAfterReconnect.contains(rServer2));
  }

  /**
   * This test checks a problem found in bug 39332 1. Durable client disconnects 2. Durable client
   * comes back, creates a create to server connection but not a queue connection 3. Durable client
   * disconnects again 4. Durable client connects 5. Eventually, all of the durable clients
   * connections are closed because the durable expiration timer task created in step 1 is never
   * cancelled.
   */
  @Test
  public void testBug39332() {
    // create client cache and Send clientReady message
    createCacheClient(2, 20);
    HashSet redundantServers = new HashSet(pool.getRedundantNames());
    redundantServers.add(pool.getPrimaryName());

    instance.determineAndVerfiyRedundantServers(redundantServers);
    instance.determineAndVerfiyNonRedundantServers(redundantServers);

    LogWriterUtils.getLogWriter()
        .info("TEST - Durable client initialially has servers " + redundantServers);

    LogWriterUtils.getLogWriter().info("TEST - Closing durable client for the first time");
    // Stop the durable client
    closeCache(true);

    LogWriterUtils.getLogWriter().info("TEST - Durable client closed for the first time");

    // Wait for server to cleanup client resources
    // temporary fix for bug 38345.
    Wait.pause(2000);

    LogWriterUtils.getLogWriter().info("TEST - Creating the durable client with one fewer servers");
    // We recreate the durable client, but this
    // Time we won't have it create any queues
    createCacheClient(2, 20, false);

    HashSet redundantServers2 = new HashSet(pool.getRedundantNames());
    redundantServers2.add(pool.getPrimaryName());
    LogWriterUtils.getLogWriter()
        .info("TEST - Durable client created again, now with servers " + redundantServers2);
    Host host = Host.getHost(0);
    // Make sure we create client to server connections to all of the servers
    pool.acquireConnection(
        new ServerLocation(NetworkUtils.getServerHostName(host), PORT1.intValue()));
    pool.acquireConnection(
        new ServerLocation(NetworkUtils.getServerHostName(host), PORT2.intValue()));
    pool.acquireConnection(
        new ServerLocation(NetworkUtils.getServerHostName(host), PORT3.intValue()));
    pool.acquireConnection(
        new ServerLocation(NetworkUtils.getServerHostName(host), PORT4.intValue()));

    LogWriterUtils.getLogWriter().info("TEST - All pool connections are now aquired");

    closeCache(true);

    LogWriterUtils.getLogWriter().info("TEST - closed durable client for the second time");

    // Wait for server to cleanup client resources
    // temporary fix for bug 38345.
    Wait.pause(2000);

    LogWriterUtils.getLogWriter().info("TEST - creating durable client for the third time");
    // Now we should connect to all of the servers we were originally connected to
    createCacheClient(2, 20);

    HashSet redundantServersAfterReconnect = new HashSet(pool.getRedundantNames());
    redundantServersAfterReconnect.add(pool.getPrimaryName());

    LogWriterUtils.getLogWriter()
        .info("TEST - durable client created for the third time, now with servers "
            + redundantServersAfterReconnect);

    instance.determineAndVerfiyRedundantServers(redundantServersAfterReconnect);
    instance.determineAndVerfiyNonRedundantServers(redundantServersAfterReconnect);

    assertEquals(redundantServers, redundantServersAfterReconnect);

    // Now we wait to make sure the durable client expiration task isn't fired.
    Wait.pause(25000);

    LogWriterUtils.getLogWriter()
        .info("TEST - Finished waiting for durable client expiration task");

    redundantServersAfterReconnect = new HashSet(pool.getRedundantNames());
    redundantServersAfterReconnect.add(pool.getPrimaryName());

    instance.determineAndVerfiyRedundantServers(redundantServersAfterReconnect);
    instance.determineAndVerfiyNonRedundantServers(redundantServersAfterReconnect);

    assertEquals(redundantServers, redundantServersAfterReconnect);
  }

  private static void verifyRedundantServers(Set redundantServers,
      Set redundantServersAfterReconnect) {
    try {
      Iterator iter = redundantServers.iterator();
      while (iter.hasNext()) {
        Object endpointName = iter.next();
        assertTrue(redundantServersAfterReconnect.contains(endpointName));
      }
    } catch (Exception e) {
      Assert.fail("test failed due to", e);
    }
  }

  private static void verifyNoDurableClientOnServer() {
    try {
      checkNumberOfClientProxies(0);
    } catch (Exception e) {
      Assert.fail("test failed due to", e);
    }
  }

  private static Integer createServerCache() throws Exception {
    Properties props = new Properties();
    new DurableClientReconnectDUnitTest().createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    Region r = cache.createRegion(REGION_NAME, attrs);
    assertNotNull(r);
    CacheServer server1 = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  private void determineAndVerfiyRedundantServers(Collection redundantServers) {
    if (redundantServers.contains(SERVER1)) {
      server1.invoke(() -> DurableClientReconnectDUnitTest.verifyDurableClientOnServer());
    }
    if (redundantServers.contains(SERVER2)) {
      server2.invoke(() -> DurableClientReconnectDUnitTest.verifyDurableClientOnServer());
    }
    if (redundantServers.contains(SERVER3)) {
      server3.invoke(() -> DurableClientReconnectDUnitTest.verifyDurableClientOnServer());
    }
    if (redundantServers.contains(SERVER4)) {
      server4.invoke(() -> DurableClientReconnectDUnitTest.verifyDurableClientOnServer());
    }
  }

  private void determineAndVerfiyNonRedundantServers(Collection redundantServers) {
    if (!redundantServers.contains(SERVER1)) {
      server1.invoke(() -> DurableClientReconnectDUnitTest.verifyNoDurableClientOnServer());
    }
    if (!redundantServers.contains(SERVER2)) {
      server2.invoke(() -> DurableClientReconnectDUnitTest.verifyNoDurableClientOnServer());
    }
    if (!redundantServers.contains(SERVER3)) {
      server3.invoke(() -> DurableClientReconnectDUnitTest.verifyNoDurableClientOnServer());
    }
    if (!redundantServers.contains(SERVER4)) {
      server4.invoke(() -> DurableClientReconnectDUnitTest.verifyNoDurableClientOnServer());
    }
  }

  private void closeServer(String server) {
    if (server.equals(SERVER1)) {
      server1.invoke(() -> DurableClientReconnectDUnitTest.closeCache());
    }
    if (server.equals(SERVER2)) {
      server2.invoke(() -> DurableClientReconnectDUnitTest.closeCache());
    }
    if (server.equals(SERVER3)) {
      server3.invoke(() -> DurableClientReconnectDUnitTest.closeCache());
    }
    if (server.equals(SERVER4)) {
      server4.invoke(() -> DurableClientReconnectDUnitTest.closeCache());
    }
  }

  private static void verifyDurableClientOnServer() {
    try {
      checkNumberOfClientProxies(1);
      CacheClientProxy proxy = getClientProxy();
      assertNotNull(proxy);
      // Verify that it is durable and its properties are correct
      assertTrue(proxy.isDurable());
      assertEquals("DurableClientReconnectDUnitTest_client", proxy.getDurableId());
      // assertIndexDetailsEquals(60, proxy.getDurableTimeout());
    } catch (Exception e) {
      Assert.fail("test failed due to", e);
    }
  }

  private static CacheClientProxy getClientProxy() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator i = notifier.getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = (CacheClientProxy) i.next();
    }
    return proxy;
  }

  private static void checkNumberOfClientProxies(final int expected) {
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return expected == getNumberOfClientProxies();
      }

      @Override
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 15 * 1000, 200, true);
  }

  private static int getNumberOfClientProxies() {
    return getBridgeServer().getAcceptor().getCacheClientNotifier().getClientProxies().size();
  }

  private static CacheServerImpl getBridgeServer() {
    CacheServerImpl bridgeServer = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bridgeServer);
    return bridgeServer;
  }

  private void createCache(Properties props) {
    try {
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  private void createCacheClient() {
    createCacheClient(1);
  }

  private PoolFactory getPoolFactory() {
    Host host = Host.getHost(0);
    PoolFactory factory = PoolManager.createFactory()
        .addServer(NetworkUtils.getServerHostName(host), PORT1.intValue())
        .addServer(NetworkUtils.getServerHostName(host), PORT2.intValue())
        .addServer(NetworkUtils.getServerHostName(host), PORT3.intValue())
        .addServer(NetworkUtils.getServerHostName(host), PORT4.intValue());
    return factory;
  }

  private void createCacheClient(int redundancyLevel) {
    createCacheClient(redundancyLevel, 60);
  }

  private void createCacheClient(int redundancyLevel, final int durableClientTimeout) {
    createCacheClient(redundancyLevel, durableClientTimeout, true);
  }

  private void createCacheClient(int redundancyLevel, final int durableClientTimeout,
      boolean queueEnabled) {
    try {
      final String durableClientId = "DurableClientReconnectDUnitTest_client";
      Properties props =
          getClientDistributedSystemProperties(durableClientId, durableClientTimeout);
      instance = new DurableClientReconnectDUnitTest();
      instance.createCache(props);
      // Host host = Host.getHost(0);
      PoolImpl p = (PoolImpl) getPoolFactory().setSubscriptionEnabled(queueEnabled)
          .setReadTimeout(10000)
          // .setRetryInterval(2000)
          .setSubscriptionRedundancy(redundancyLevel).create("DurableClientReconnectDUnitTestPool");

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setPoolName(p.getName());

      RegionAttributes attrs = factory.create();
      Region r = cache.createRegion(REGION_NAME, attrs);
      assertNotNull(r);

      pool = p;
      conn = pool.acquireConnection();
      assertNotNull(conn);

      cache.readyForEvents();

    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  private void createCacheClientAndConnectToSingleServer(String host, int redundancyLevel) {
    try {
      final String durableClientId = "DurableClientReconnectDUnitTest_client";
      final int durableClientTimeout = 60; // keep the client alive for 60 seconds
      Properties props =
          getClientDistributedSystemProperties(durableClientId, durableClientTimeout);
      instance = new DurableClientReconnectDUnitTest();
      instance.createCache(props);
      PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, PORT1.intValue())
          .setSubscriptionEnabled(true).setReadTimeout(10000)
          // .setRetryInterval(2000)
          .setSubscriptionRedundancy(redundancyLevel).create("DurableClientReconnectDUnitTestPool");

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setPoolName(p.getName());

      RegionAttributes attrs = factory.create();
      Region r = cache.createRegion(REGION_NAME, attrs);
      assertNotNull(r);

      pool = p;
      conn = pool.acquireConnection();
      assertNotNull(conn);

      cache.readyForEvents();

    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  private void createCacheClientAndConnectToSingleServerWithZeroConnPerServer(String host,
      int redundancyLevel) {
    try {
      final String durableClientId = "DurableClientReconnectDUnitTest_client";
      final int durableClientTimeout = 60; // keep the client alive for 60 seconds
      Properties props =
          getClientDistributedSystemProperties(durableClientId, durableClientTimeout);
      instance = new DurableClientReconnectDUnitTest();
      instance.createCache(props);
      PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, PORT1.intValue())
          .setSubscriptionEnabled(true).setReadTimeout(10000)
          // .setRetryInterval(2000)
          .setMinConnections(0).setSubscriptionRedundancy(redundancyLevel)
          .create("DurableClientReconnectDUnitTestPool");

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setPoolName(p.getName());

      RegionAttributes attrs = factory.create();
      Region r = cache.createRegion(REGION_NAME, attrs);
      assertNotNull(r);

      pool = p;
      conn = pool.acquireConnection();
      assertNotNull(conn);

      cache.readyForEvents();

    } catch (Exception e) {
      Assert.fail("test failed due to ", e);
    }
  }

  private Properties getClientDistributedSystemProperties(String durableClientId,
      int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return properties;
  }

  @Override
  public final void preTearDown() throws Exception {
    // close the clients first
    closeCache();

    // then close the servers
    server1.invoke(() -> DurableClientReconnectDUnitTest.closeCache());
    server2.invoke(() -> DurableClientReconnectDUnitTest.closeCache());
    server3.invoke(() -> DurableClientReconnectDUnitTest.closeCache());
    server4.invoke(() -> DurableClientReconnectDUnitTest.closeCache());
  }

  private void closeCache(boolean keepAlive) {
    if (cache != null && !cache.isClosed()) {
      cache.close(keepAlive);
      cache.getDistributedSystem().disconnect();
    }
  }

  private static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}

