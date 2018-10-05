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

import static org.apache.geode.cache.CacheFactory.getAnyInstance;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Test to verify Startup. and failover during startup.
 */
@Category({ClientSubscriptionTest.class})
public class HAStartupAndFailoverDUnitTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME =
      HAStartupAndFailoverDUnitTest.class.getSimpleName() + "_region";

  protected static Cache cache = null;

  VM server1 = null;
  VM server2 = null;
  VM server3 = null;

  protected static PoolImpl pool = null;
  private static Connection conn = null;

  private static Integer PORT1;
  private static Integer PORT2;
  private static Integer PORT3;


  // To verify the primary identification on client side toggeled after notification on client side
  protected static boolean identifiedPrimary = false;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);

    IgnoredException.addIgnoredException("java.io.IOException");
    IgnoredException.addIgnoredException("SocketException");

    // start servers first
    PORT1 = ((Integer) server1.invoke(() -> HAStartupAndFailoverDUnitTest.createServerCache()));
    PORT2 = ((Integer) server2.invoke(() -> HAStartupAndFailoverDUnitTest.createServerCache()));
    PORT3 = ((Integer) server3.invoke(() -> HAStartupAndFailoverDUnitTest.createServerCache()));
    CacheServerTestUtil.disableShufflingOfEndpoints();
  }

  /**
   * Stops primary server one by one to ensure new primary is selected
   */
  @Test
  public void testPrimaryFailover() throws Exception {

    createClientCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    // primary
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());

    // secondaries
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());

    setClientServerObserver();

    server1.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());

    waitForPrimaryIdentification();
    // primary
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
    unSetClientServerObserver();
    // secondary
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());

    setClientServerObserver();
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    // primary
    waitForPrimaryIdentification();
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
    unSetClientServerObserver();
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    // All servers are dead at this point , no primary in the system.
    verifyDeadAndLiveServers(3, 0);

    // now start one of the servers
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.startServer());
    // make sure that the server3 which was started recenty was marked live.
    verifyDeadAndLiveServers(2, 1);

    // verify that is it primary , and dispatche is running
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
  }



  /**
   * verify that when an exeption occurs during making primary , new primary will be selected
   *
   */
  @Test
  public void testExceptionWhileMakingPrimary() throws Exception {

    createClientCacheWithIncorrectPrimary(this.getName(),
        NetworkUtils.getServerHostName(server1.getHost()));
    // failed primary due to incorect host name of the server

    // new primary
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
    // secondary
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());

    setClientServerObserver();

    // stop new primary
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());

    waitForPrimaryIdentification();

    // newly selectd primary
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());

    unSetClientServerObserver();
  }

  /**
   * verify that when an exeption occurs during making primary to two EPs , new primary will be
   * selected
   *
   */
  @Test
  public void testTwoPrimaryFailedOneAfterTheAnother() throws Exception {

    createClientCacheWithLargeRetryInterval(this.getName(),
        NetworkUtils.getServerHostName(server1.getHost()));
    // primary
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());

    // secondaries
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    setClientServerObserver();

    server1.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    // stop ProbablePrimary
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    // processException();


    waitForPrimaryIdentification();
    // new primary
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
    unSetClientServerObserver();

  }

  /**
   * verify that Primary Should Be Null And EPList Should Be Empty When All Servers Are Dead
   */
  @Test
  public void testPrimaryShouldBeNullAndEPListShouldBeEmptyWhenAllServersAreDead()
      throws Exception {
    createClientCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    verifyPrimaryShouldNotBeNullAndEPListShouldNotBeEmpty();
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    verifyDeadAndLiveServers(3, 0);
    verifyPrimaryShouldBeNullAndEPListShouldBeEmpty();
  }

  /**
   * Tests failover initialization by cacheClientUpdater Thread on failure in Primary Server
   */
  @Test
  public void testCacheClientUpdatersInitiatesFailoverOnPrimaryFailure() throws Exception {
    createClientCacheWithLargeRetryInterval(this.getName(),
        NetworkUtils.getServerHostName(server1.getHost()));
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    setClientServerObserver();
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    waitForPrimaryIdentification();
    unSetClientServerObserver();
    verifyDeadAndLiveServers(1, 2);
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
  }

  /**
   * Tests failover initialization by cacheClientUpdater Thread on failure on Secondary server
   */
  @Test
  public void testCacheClientUpdaterInitiatesFailoverOnSecondaryFailure() throws Exception {
    createClientCacheWithLargeRetryInterval(this.getName(),
        NetworkUtils.getServerHostName(server1.getHost()));
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    verifyDeadAndLiveServers(1, 2);
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());

  }


  /**
   * Tests failover initialization by cacheClientUpdater Thread failure on both Primary and
   * Secondary server
   */
  @Test
  public void testCacheClientUpdaterInitiatesFailoverOnBothPrimaryAndSecondaryFailure()
      throws Exception {

    createClientCacheWithLargeRetryInterval(this.getName(),
        NetworkUtils.getServerHostName(server1.getHost()));
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    verifyDeadAndLiveServers(2, 1);
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());


  }

  /**
   * Tests failover initialization by cacheClientUpdater Thread
   */
  @Test
  public void testCacheClientUpdaterInitiatesFailoverOnBothPrimaryAndSecondaryFailureWithServerMonitors()
      throws Exception {

    createClientCache(this.getName(), NetworkUtils.getServerHostName(server1.getHost()));
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsNotAlive());
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    verifyDeadAndLiveServers(2, 1);
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.verifyDispatcherIsAlive());
  }

  /**
   * Tests failover initialization by cache operation Threads on secondary
   */
  @Test
  public void testInitiateFailoverByCacheOperationThreads_Secondary() throws Exception {
    // Stop the 3rd server to guarantee the client put will go to the first server
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    // create a client with no client updater thread
    // so that only cache operation can detect a server failure and should initiate failover
    createClientCacheWithLargeRetryIntervalAndWithoutCallbackConnection(this.getName(),
        NetworkUtils.getServerHostName(server1.getHost()));
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.stopServer());
    put();
    verifyDeadAndLiveServers(1, 1);
  }

  public static void put() {
    try {
      Region r1 = cache.getRegion("/" + REGION_NAME);
      r1.put("key-1", "server-value-1");
      r1.put("key-2", "server-value-2");
      r1.put("key-3", "server-value-3");
    } catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  public static void verifyDeadAndLiveServers(final int expectedDeadServers,
      final int expectedLiveServers) {
    await()
        .untilAsserted(() -> assertEquals(expectedLiveServers, pool.getConnectedServerCount()));
  }

  public static void setClientServerObserver() {
    PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void afterPrimaryIdentificationFromBackup(ServerLocation primaryEndpoint) {
        synchronized (HAStartupAndFailoverDUnitTest.class) {
          HAStartupAndFailoverDUnitTest.identifiedPrimary = true;
          HAStartupAndFailoverDUnitTest.class.notifyAll();
        }
      }
    });
  }

  public static void unSetClientServerObserver() {
    synchronized (HAStartupAndFailoverDUnitTest.class) {
      PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
      HAStartupAndFailoverDUnitTest.identifiedPrimary = false;
      ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter());
    }
  }

  public static void stopServer() {
    try {
      assertEquals("Expected exactly one BridgeServer", 1, cache.getCacheServers().size());
      CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
      assertNotNull(bs);
      bs.stop();
    } catch (Exception ex) {
      Assert.fail("while setting stopServer", ex);
    }
  }

  public static void verifyPrimaryShouldNotBeNullAndEPListShouldNotBeEmpty() {
    try {
      assertNotNull(" Primary endpoint should not be null", pool.getPrimaryName());
      assertTrue("Endpoint List should not be Empty as all server are live",
          pool.getConnectedServerCount() > 0);
    } catch (Exception e) {
      Assert.fail("failed while verifyPrimaryShouldNotBeNullAndEPListShouldNotBeEmpty()", e);
    }
  }

  public static void verifyPrimaryShouldBeNullAndEPListShouldBeEmpty() {
    try {
      assertNull("Primary endpoint should be null as all server are dead", pool.getPrimaryName());
      assertEquals("Endpoint List should be Empty as all server are dead", 0,
          pool.getConnectedServerCount());
      fail("NoSubscriptionServersAvailableException is expected");
    } catch (NoSubscriptionServersAvailableException e) {
      // pass
    } catch (Exception e) {
      Assert.fail("failed while verifyPrimaryShouldBeNullAndEPListShouldBeEmpty()", e);
    }
  }

  public static void startServer() {
    try {
      Cache c = CacheFactory.getAnyInstance();
      assertEquals("Expected exactly one BridgeServer", 1, c.getCacheServers().size());
      CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
      assertNotNull(bs);
      bs.start();
    } catch (Exception ex) {
      fail("while startServer()  " + ex);
    }
  }

  public static void waitForPrimaryIdentification() {
    assertNotNull(cache);
    if (!identifiedPrimary) {
      synchronized (HAStartupAndFailoverDUnitTest.class) {
        if (!identifiedPrimary) {
          final int MAX_WAIT = 60 * 1000;
          try {
            HAStartupAndFailoverDUnitTest.class.wait(MAX_WAIT);
          } catch (InterruptedException e) {
            fail("Test failed due to InterruptedException in waitForPrimaryIdentification()");
          }
          if (!identifiedPrimary) {
            fail("timed out after waiting " + MAX_WAIT + " millisecs"
                + " for primary to be identified");
          }
        }
      }
    }
  }

  public static void verifyDispatcherIsAlive() {
    try {
      WaitCriterion wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return cache.getCacheServers().size() == 1;
        }

        public String description() {
          return excuse;
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);

      CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
      wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return ccn.getClientProxies().size() > 0;
        }

        public String description() {
          return excuse;
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);

      Collection<CacheClientProxy> proxies = ccn.getClientProxies();
      Iterator<CacheClientProxy> iter_prox = proxies.iterator();

      if (iter_prox.hasNext()) {
        final CacheClientProxy proxy = iter_prox.next();
        wc = new WaitCriterion() {
          String excuse;

          public boolean done() {
            return proxy._messageDispatcher.isAlive();
          }

          public String description() {
            return excuse;
          }
        };
        GeodeAwaitility.await().untilAsserted(wc);
      }

    } catch (Exception ex) {
      fail("while setting verifyDispatcherIsAlive  " + ex);
    }
  }

  public static void verifyDispatcherIsNotAlive() {
    try {
      Cache c = getAnyInstance();
      // assertIndexDetailsEquals("More than one BridgeServer", 1,
      // c.getCacheServers().size());
      WaitCriterion wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return cache.getCacheServers().size() == 1;
        }

        public String description() {
          return excuse;
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);

      CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
      wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return ccn.getClientProxies().size() > 0;
        }

        public String description() {
          return excuse;
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);

      Iterator iter_prox = ccn.getClientProxies().iterator();
      if (iter_prox.hasNext()) {
        CacheClientProxy proxy = (CacheClientProxy) iter_prox.next();
        assertFalse("Dispatcher on secondary should not be alive",
            proxy._messageDispatcher.isAlive());
      }
    } catch (Exception ex) {
      fail("while setting verifyDispatcherIsNotAlive  " + ex);
    }
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String testName, String host) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAStartupAndFailoverDUnitTest().createCache(props);
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, PORT1.intValue())
        .addServer(host, PORT2.intValue()).addServer(host, PORT3.intValue())
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(10000)
        // .setRetryInterval(2000)
        .create("HAStartupAndFailoverDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);
  }

  public static void createClientCacheWithLargeRetryInterval(String testName, String host)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAStartupAndFailoverDUnitTest().createCache(props);
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, PORT1.intValue())
        .addServer(host, PORT2.intValue()).addServer(host, PORT3.intValue())
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(10000)
        // .setRetryInterval(2000000)
        .create("HAStartupAndFailoverDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);
  }

  public static void createClientCacheWithLargeRetryIntervalAndWithoutCallbackConnection(
      String testName, String host) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAStartupAndFailoverDUnitTest().createCache(props);

    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl p;
    try {
      p = (PoolImpl) PoolManager.createFactory().addServer(host, PORT1.intValue())
          .addServer(host, PORT2.intValue()).addServer(host, PORT3.intValue()).setPingInterval(500)
          // .setRetryInterval(200000)
          .create("HAStartupAndFailoverDUnitTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    pool = p;
    // since the default minConnections is 1 we currently have a connection to
    // server1 (vm_0). Now we create a connection to server2 (vm_1)
    conn = pool.acquireConnection(new ServerLocation(Host.getHost(0).getHostName(), PORT2));

    // assert that the conn is to server2 since the tests assume that this is so
    assertNotNull(conn);
    assertTrue(conn.getEndpoint().getLocation().getPort() == PORT2);
  }

  public static void createClientCacheWithIncorrectPrimary(String testName, String host)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAStartupAndFailoverDUnitTest().createCache(props);
    final int INCORRECT_PORT = 1;
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, INCORRECT_PORT)
        .addServer(host, PORT2.intValue()).addServer(host, PORT3.intValue())
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(10000)
        // .setRetryInterval(10000)
        .create("HAStartupAndFailoverDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);
  }

  public static Integer createServerCache() throws Exception {
    new HAStartupAndFailoverDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableBridgeConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    CacheServer server1 = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    // ensures updates to be sent instead of invalidations
    server1.setNotifyBySubscription(true);
    server1.setMaximumTimeBetweenPings(180000);
    server1.start();
    return new Integer(server1.getPort());
  }

  @Override
  public final void preTearDown() throws Exception {
    // close the clients first
    closeCache();

    // then close the servers
    server1.invoke(() -> HAStartupAndFailoverDUnitTest.closeCache());
    server2.invoke(() -> HAStartupAndFailoverDUnitTest.closeCache());
    server3.invoke(() -> HAStartupAndFailoverDUnitTest.closeCache());
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
      HAStartupAndFailoverDUnitTest.identifiedPrimary = false;
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
