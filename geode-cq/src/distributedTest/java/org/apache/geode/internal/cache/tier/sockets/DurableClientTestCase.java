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

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.cq.CqQueryImpl;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Class <code>DurableClientTestCase</code> tests durable client functionality.
 *
 * @since GemFire 5.2
 */
@Category({ClientSubscriptionTest.class})
public class DurableClientTestCase extends JUnit4DistributedTestCase {

  private static volatile boolean isPrimaryRecovered = false;

  protected VM server1VM;
  protected VM server2VM;
  protected VM durableClientVM;
  protected VM publisherClientVM;
  protected String regionName;

  @Override
  public final void postSetUp() throws Exception {
    this.server1VM = VM.getVM(0);
    this.server2VM = VM.getVM(1);
    this.durableClientVM = VM.getVM(2);
    this.publisherClientVM = VM.getVM(3);
    this.regionName = getName() + "_region";
    // Clients see this when the servers disconnect
    IgnoredException.addIgnoredException("Could not find any server");
    System.out.println("\n\n[setup] START TEST " + getClass().getSimpleName() + "."
        + getTestMethodName() + "\n\n");
    postSetUpDurableClientTestCase();
  }

  protected void postSetUpDurableClientTestCase() throws Exception {}

  @Override
  public final void preTearDown() throws Exception {
    preTearDownDurableClientTestCase();

    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  protected void preTearDownDurableClientTestCase() throws Exception {}

  /**
   * Test that starting a durable client is correctly processed by the server.
   */
  @Test
  public void testSimpleDurableClient() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId)));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
      }
    });

    // Stop the durable client
    this.disconnectDurableClient();

    // Verify the durable client is present on the server for closeCache=false case.
    this.verifySimpleDurableClient();

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    this.closeDurableClient();

  }

  /**
   * Test that starting a durable client is correctly processed by the server. In this test we will
   * set gemfire.SPECIAL_DURABLE property to true and will see durableID appended by poolname or not
   */
  @Test
  public void testSimpleDurableClient2() {
    final Properties jp = new Properties();
    jp.setProperty(DistributionConfig.GEMFIRE_PREFIX + "SPECIAL_DURABLE", "true");

    try {
      // Start a server
      int serverPort = this.server1VM
          .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

      // Start a durable client that is not kept alive on the server when it
      // stops normally
      final String durableClientId = getName() + "_client";

      this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
          getClientPool(NetworkUtils.getServerHostName(), serverPort,
              true),
          regionName, getClientDistributedSystemProperties(durableClientId), Boolean.FALSE,
          jp));

      // Send clientReady message
      this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
        public void run2() throws CacheException {
          CacheServerTestUtil.getClientCache().readyForEvents();
        }
      });

      // Verify durable client on server
      this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
        public void run2() throws CacheException {
          // Find the proxy
          checkNumberOfClientProxies(1);
          CacheClientProxy proxy = getClientProxy();
          assertNotNull(proxy);

          // Verify that it is durable and its properties are correct
          assertTrue(proxy.isDurable());
          assertNotSame(durableClientId, proxy.getDurableId());

          /*
           * new durable id will be like this durableClientId _gem_ //separator client pool name
           */
          String dId = durableClientId + "_gem_" + "CacheServerTestUtil";

          assertEquals(dId, proxy.getDurableId());
          assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
              proxy.getDurableTimeout());
          // assertIndexDetailsEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_KEEP_ALIVE,
          // proxy.getDurableKeepAlive());
        }
      });

      // Stop the durable client
      this.disconnectDurableClient();

      // Verify the durable client is present on the server for closeCache=false case.
      this.verifySimpleDurableClient();

      // Stop the server
      this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      this.closeDurableClient();
    } finally {

      this.durableClientVM.invoke(() -> CacheServerTestUtil.unsetJavaSystemProperties(jp));
    }

  }

  public void closeDurableClient() {}

  public void disconnectDurableClient() {
    this.disconnectDurableClient(false);
  }

  public void disconnectDurableClient(boolean keepAlive) {
    printClientProxyState("Before");
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(keepAlive));
    Wait.pause(1000);
    printClientProxyState("after");
  }

  private void printClientProxyState(String st) {
    CacheSerializableRunnable s =
        new CacheSerializableRunnable("Logging CCCP and ServerConnection state") {
          @Override
          public void run2() throws CacheException {
            // TODO Auto-generated method stub
            CacheServerTestUtil.getCache().getLogger()
                .info(st + " CCP states: " + getAllClientProxyState());
            CacheServerTestUtil.getCache().getLogger().info(st + " CHM states: "
                + printMap(ClientHealthMonitor._instance.getConnectedClients(null)));
          }
        };
    server1VM.invoke(s);
  }

  private static String printMap(Map<String, Object[]> m) {
    Iterator<Map.Entry<String, Object[]>> itr = m.entrySet().iterator();
    StringBuffer sb = new StringBuffer();
    sb.append("size = ").append(m.size()).append(" ");
    while (itr.hasNext()) {
      sb.append("{");
      Map.Entry entry = itr.next();
      sb.append(entry.getKey());
      sb.append(", ");
      printMapValue(entry.getValue(), sb);
      sb.append("}");
    }
    return sb.toString();
  }

  private static void printMapValue(Object value, StringBuffer sb) {
    if (value.getClass().isArray()) {

      sb.append("{");
      sb.append(Arrays.toString((Object[]) value));
      sb.append("}");
    } else {
      sb.append(value);
    }
  }

  public void verifySimpleDurableClient() {
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
        CacheClientProxy proxy = getClientProxy();
        assertNull(proxy);
      }
    });
  }

  /**
   * Test that starting, stopping then restarting a durable client is correctly processed by the
   * server.
   */
  @Test
  public void testStartStopStartDurableClient() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
        // assertIndexDetailsEquals(durableClientKeepAlive, proxy.getDurableKeepAlive());
      }
    });

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Re-start the durable client
    this.restartDurableClient(new Object[] {
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)});

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
      }
    });

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that starting, stopping then restarting a durable client is correctly processed by the
   * server. This is a test of bug 39630
   */
  @Test
  public void test39630() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)));

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
        // assertIndexDetailsEquals(durableClientKeepAlive, proxy.getDurableKeepAlive());
      }
    });

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Verify the durable client still exists on the server, and the socket is closed
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        assertNotNull(proxy._socket);
        long end = System.currentTimeMillis() + 60000;

        while (!proxy._socket.isClosed()) {
          if (System.currentTimeMillis() > end) {
            break;
          }
        }
        assertTrue(proxy._socket.isClosed());
      }
    });

    // Re-start the durable client (this is necessary so the
    // netDown test will set the appropriate system properties.
    this.restartDurableClient(new Object[] {
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)});

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  public void restartDurableClient(Object[] args) {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", args);

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });
  }

  /**
   * Test that disconnecting a durable client for longer than the timeout period is correctly
   * processed by the server.
   */
  @Test
  public void testStartStopTimeoutDurableClient() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 5; // keep the client alive for 5 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
      }
    });

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Verify it no longer exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
        CacheClientProxy proxy = getClientProxy();
        assertNull(proxy);
      }
    });

    this.restartDurableClient(new Object[] {
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)});

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that a durable client correctly receives updates after it reconnects.
   */
  @Test
  public void testDurableClientPrimaryUpdate() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 120; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort,
            false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 1;
    publishEntries(numberOfEntries);

    // Verify the durable client received the updates
    this.verifyListenerUpdates(numberOfEntries);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Make sure the proxy is actually paused, not dispatching
    this.server1VM.invoke(new CacheSerializableRunnable("Wait for paused") {
      public void run2() throws CacheException {
        // If we wait too long, the durable queue will be gone, because
        // the timeout is 120 seconds.
        Awaitility.waitAtMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
          CacheClientProxy proxy = getClientProxy();
          return proxy != null && proxy.isPaused();
        });
      }
    });

    // Publish some more entries
    publishEntries(numberOfEntries);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }

    // Verify the durable client's queue contains the entries
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // If we wait too long, the durable queue will be gone, because
        // the timeout is 120 seconds.
        Awaitility.waitAtMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
          CacheClientProxy proxy = getClientProxy();
          if (proxy == null) {
            return false;
          }
          // Verify the queue size
          int sz = proxy.getQueueSize();
          return numberOfEntries == sz;
        });
      }
    });

    // Verify that disconnected client does not receive any events.
    this.verifyListenerUpdatesDisconnected(numberOfEntries);

    // Re-start the durable client
    this.restartDurableClient(new Object[] {
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE});

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });

    // Verify the durable client received the updates held for it on the server
    this.verifyListenerUpdates(numberOfEntries, numberOfEntries);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the durable client VM
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  protected void publishEntries(final int numberOfEntries) {
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries") {
      public void run2() throws CacheException {
        // Get the region
        Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 0; i < numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
  }

  /**
   * Test that a durable client correctly receives updates after it reconnects.
   */
  @Test
  public void testStartStopStartDurableClientUpdate() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort,
            false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 1;
    publishEntries(numberOfEntries);

    // Verify the durable client received the updates
    this.verifyListenerUpdates(numberOfEntries);

    // ARB: Wait for queue ack to arrive at server.
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        final CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        Awaitility.waitAtMost(1, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
            .until(proxy::isPaused);

        assertTrue(proxy.isPaused());
      }
    });

    // Publish some more entries
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish more entries") {
      public void run2() throws CacheException {
        // Get the region
        Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 0; i < numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }

    // Verify the durable client's queue contains the entries
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify the queue size
        assertEquals(numberOfEntries, proxy.getQueueSize());
      }
    });

    // Verify that disconnected client does not receive any events.
    this.verifyListenerUpdatesDisconnected(numberOfEntries);

    // Re-start the durable client
    this.restartDurableClient(new Object[] {
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE});

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });

    // Verify the durable client received the updates held for it on the server
    this.verifyListenerUpdates(numberOfEntries, numberOfEntries);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the durable client VM
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  public void verifyListenerUpdates(int numEntries) {
    this.verifyListenerUpdatesEntries(numEntries, 0);
  }

  public void verifyListenerUpdates(int numEntries, int numEntriesBeforeDisconnect) {
    this.verifyListenerUpdatesEntries(numEntries, 0);
  }

  public void verifyListenerUpdatesEntries(final int numEntries,
      final int numEntriesBeforeDisconnect) {
    this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlListener listener =
            (CacheServerTestUtil.ControlListener) region.getAttributes().getCacheListeners()[0];
        listener.waitWhileNotEnoughEvents(60 * 1000, numEntries + numEntriesBeforeDisconnect);
        assertEquals(numEntries + numEntriesBeforeDisconnect, listener.events.size());
      }
    });
  }

  public void verifyListenerUpdatesDisconnected(int numberOfEntries) {
    // ARB: do nothing.
  }

  public void verifySimpleDurableClientMultipleServers() {
    // Verify the durable client is no longer on server1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
        CacheClientProxy proxy = getClientProxy();
        assertNull(proxy);
      }
    });

    // Verify the durable client is no longer on server2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
        CacheClientProxy proxy = getClientProxy();
        assertNull(proxy);
      }
    });
  }

  /**
   * Test whether a durable client reconnects properly to a server that is stopped and restarted.
   */
  @Test
  public void testDurableClientConnectServerStopStart() {
    // Start a server
    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
      }
    });

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Re-start the server
    this.server1VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        serverPort));

    // Pause 10 seconds to allow client to reconnect to server
    // no need for the explicit pause since checkNumberOfClientProxies
    // will wait up to 15 seconds
    // pause(10000);

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
      }
    });

    // Start a publisher
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort,
            false),
        regionName));

    // Publish some messages
    // Publish some entries
    final int numberOfEntries = 10;
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 0; i < numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    // Verify the durable client received the updates
    verifyDurableClientEvents(this.durableClientVM, numberOfEntries);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  protected void verifyDurableClientEvents(VM durableClientVm, final int numberOfEntries) {
    verifyDurableClientEvents(durableClientVm, numberOfEntries, -1);
  }

  protected void verifyDurableClientEvents(VM durableClientVm, final int numberOfEntries,
      final int eventType) {
    verifyDurableClientEvents(durableClientVm, numberOfEntries, eventType, numberOfEntries);
  }

  protected void verifyNoDurableClientEvents(VM durableClientVm, final int numberOfEntries,
      final int eventType) {
    verifyDurableClientEvents(durableClientVm, numberOfEntries, eventType, 0);
  }

  private void verifyDurableClientEvents(VM durableClientVm, final int numberOfEntries,
      final int eventType, final int expectedNumberOfEvents) {
    durableClientVm.invoke(new CacheSerializableRunnable("Verify updates") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlListener listener =
            (CacheServerTestUtil.ControlListener) region.getAttributes().getCacheListeners()[0];
        listener.waitWhileNotEnoughEvents(30000, numberOfEntries, eventType);
        assertEquals(expectedNumberOfEvents, listener.getEvents(eventType).size());
      }
    });
  }

  @Test
  public void testDurableNonHAFailover() {
    durableFailover(0);
    durableFailoverAfterReconnect(0);
  }

  @Test
  public void testDurableHAFailover() {
    // Clients see this when the servers disconnect
    IgnoredException.addIgnoredException("Could not find any server");
    durableFailover(1);
    durableFailoverAfterReconnect(1);
  }

  /**
   * Test a durable client with 2 servers where the client fails over from one to another server
   * with a publisher/feeder performing operations and the client verifying updates received.
   * Redundancy level is set to 1 for this test case.
   */
  public void durableFailover(int redundancyLevel) {

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int server1Port = ports[0];

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Start a durable client
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    Pool clientPool;
    if (redundancyLevel == 1) {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true);
    } else {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true, 0);
    }

    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Re-start server2
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        server2Port));

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port,
            server2Port, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 1;
    publishEntries(numberOfEntries);

    // Verify the durable client received the updates
    this.verifyListenerUpdates(numberOfEntries);

    try {
      java.lang.Thread.sleep(5000);
    } catch (java.lang.InterruptedException ex) {
      fail("interrupted");
    }


    // Stop the durable client
    this.disconnectDurableClient(true);

    // Publish updates during client downtime
    publishEntries(numberOfEntries);


    // Re-start the durable client that is kept alive on the server
    this.restartDurableClient(new Object[] {clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Publish second round of updates
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries before failover") {
      public void run2() throws CacheException {
        // Get the region
        Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 1; i < numberOfEntries + 1; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    // Verify the durable client received the updates before failover
    this.verifyListenerUpdates(numberOfEntries + 1, numberOfEntries);

    try {
      java.lang.Thread.sleep(1000);
    } catch (java.lang.InterruptedException ex) {
      fail("interrupted");
    }

    setPrimaryRecoveryCheck();

    // Stop server 1 - publisher will put 10 entries during shutdown/primary identification
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    this.durableClientVM.invoke(new CacheSerializableRunnable("Get") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        assertNull(region.getEntry("0"));
      }
    });

    checkPrimaryRecovery();
    // Publish second round of updates after failover
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries after failover") {
      public void run2() throws CacheException {
        // Get the region
        Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 2; i < numberOfEntries + 2; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    // Verify the durable client received the updates after failover
    this.verifyListenerUpdates(numberOfEntries + 2, numberOfEntries);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }


  public void durableFailoverAfterReconnect(int redundancyLevel) {
    // Start server 1
    Integer[] ports = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, true));
    final int server1Port = ports[0];

    ports = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, true));
    int server2Port = ports[0];

    // Start a durable client
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    Pool clientPool;
    if (redundancyLevel == 1) {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true);
    } else {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true, 0);
    }

    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port,
            server2Port, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 1;
    publishEntries(numberOfEntries);

    try {
      java.lang.Thread.sleep(10000);
    } catch (java.lang.InterruptedException ex) {
      fail("interrupted");
    }

    // Verify the durable client received the updates
    this.verifyListenerUpdates(numberOfEntries);

    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
      }
    });

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Stop server 1 - publisher will put 10 entries during shutdown/primary identification
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Publish updates during client downtime
    publishEntries(numberOfEntries);

    // Re-start the durable client that is kept alive on the server
    this.restartDurableClient(new Object[] {clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Publish second round of updates
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries before failover") {
      public void run2() throws CacheException {
        // Get the region
        Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 1; i < numberOfEntries + 1; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    // Verify the durable client received the updates before failover
    if (redundancyLevel == 1) {
      this.verifyListenerUpdates(numberOfEntries + 1, numberOfEntries);
    } else {
      this.verifyListenerUpdates(numberOfEntries, numberOfEntries);
    }

    this.durableClientVM.invoke(new CacheSerializableRunnable("Get") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        assertNull(region.getEntry("0"));
      }
    });

    // Publish second round of updates after failover
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries after failover") {
      public void run2() throws CacheException {
        // Get the region
        Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 2; i < numberOfEntries + 2; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    // Verify the durable client received the updates after failover
    if (redundancyLevel == 1) {
      this.verifyListenerUpdates(numberOfEntries + 2, numberOfEntries);
    } else {
      this.verifyListenerUpdates(numberOfEntries + 1, numberOfEntries);
    }

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }


  // First we will have the client wait before trying to reconnect
  // Then the drain will lock and begins to drain
  // The client will then be able to continue, and get rejected
  // Then we proceed to drain and release all locks
  // The client will then reconnect
  public class RejectClientReconnectTestHook implements CacheClientProxy.TestHook {
    final CountDownLatch reconnectLatch = new CountDownLatch(1);
    final CountDownLatch continueDrain = new CountDownLatch(1);
    volatile boolean clientWasRejected = false;
    CountDownLatch clientConnected = new CountDownLatch(1);

    public void doTestHook(String spot) {
      try {
        switch (spot) {
          case "CLIENT_PRE_RECONNECT":
            if (!reconnectLatch.await(60, TimeUnit.SECONDS)) {
              fail("reonnect latch was never released.");
            }
            break;
          case "DRAIN_IN_PROGRESS_BEFORE_DRAIN_LOCK_CHECK":
            // let client try to reconnect
            reconnectLatch.countDown();
            // we wait until the client is rejected
            if (!continueDrain.await(120, TimeUnit.SECONDS)) {
              fail("Latch was never released.");
            }
            break;
          case "CLIENT_REJECTED_DUE_TO_CQ_BEING_DRAINED":
            clientWasRejected = true;
            continueDrain.countDown();
            break;
          default:
            break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    public boolean wasClientRejected() {
      return clientWasRejected;
    }
  }

  /*
   * This hook will cause the close cq to throw an exception due to a client in the middle of
   * activating sequence - server will pause before draining client will begin to reconnect and then
   * wait to continue server will be unblocked, and rejected client will the be unlocked after
   * server is rejected and continue
   */
  public class CqExceptionDueToActivatingClientTestHook implements CacheClientProxy.TestHook {
    final CountDownLatch unblockDrain = new CountDownLatch(1);
    final CountDownLatch unblockClient = new CountDownLatch(1);
    final CountDownLatch finish = new CountDownLatch(1);

    public void doTestHook(String spot) {
      if (spot.equals("PRE_DRAIN_IN_PROGRESS")) {
        try {
          // Unblock any client waiting to reconnect
          unblockClient.countDown();
          // Wait until client is reconnecting
          if (!unblockDrain.await(120, TimeUnit.SECONDS)) {
            fail("client never got far enough reconnected to unlatch lock.");
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }
      if (spot.equals("PRE_RELEASE_DRAIN_LOCK")) {
        // Client is reconnecting but still holds the drain lock
        // let the test continue to try to close a cq
        unblockDrain.countDown();
        // wait until the server has finished attempting to close the cq
        try {
          if (!finish.await(30, TimeUnit.SECONDS)) {
            fail("Test did not complete, server never finished attempting to close cq");
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }
      if (spot.equals("DRAIN_COMPLETE")) {
        finish.countDown();
      }
    }
  }


  protected CqQuery createCq(String cqName, String cqQuery, boolean durable)
      throws CqException, CqExistsException {
    QueryService qs = CacheServerTestUtil.getCache().getQueryService();
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqListener[] cqListeners = {new CacheServerTestUtil.ControlCqListener()};
    cqf.initCqListeners(cqListeners);
    CqAttributes cqa = cqf.create();
    return qs.newCq(cqName, cqQuery, cqa, durable);

  }

  protected Pool getClientPool(String host, int serverPort, boolean establishCallbackConnection) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, serverPort).setSubscriptionEnabled(establishCallbackConnection)
        .setSubscriptionAckInterval(1);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }


  protected Pool getClientPool(String host, int server1Port, int server2Port,
      boolean establishCallbackConnection) {
    return getClientPool(host, server1Port, server2Port, establishCallbackConnection, 1);
  }


  protected Properties getClientDistributedSystemProperties(String durableClientId) {
    return getClientDistributedSystemProperties(durableClientId,
        DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);
  }

  protected Properties getClientDistributedSystemPropertiesNonDurable(String durableClientId) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    return properties;
  }


  protected Properties getClientDistributedSystemProperties(String durableClientId,
      int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return properties;
  }

  protected static CacheClientProxy getClientProxy() {
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

  protected static String getAllClientProxyState() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator<CacheClientProxy> i = notifier.getClientProxies().iterator();
    StringBuilder sb = new StringBuilder();
    while (i.hasNext()) {
      sb.append(" [");
      sb.append(i.next().getState());
      sb.append(" ]");
    }
    return sb.toString();
  }

  protected static void checkNumberOfClientProxies(final int expected) {
    Awaitility.waitAtMost(50, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> expected == getNumberOfClientProxies());
  }

  protected static void checkProxyIsAlive(final CacheClientProxy proxy) {
    Awaitility.waitAtMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(proxy::isAlive);
  }

  protected static int getNumberOfClientProxies() {
    return getBridgeServer().getAcceptor().getCacheClientNotifier().getClientProxies().size();
  }

  protected static CacheServerImpl getBridgeServer() {
    CacheServerImpl bridgeServer =
        (CacheServerImpl) CacheServerTestUtil.getCache().getCacheServers().iterator().next();
    assertNotNull(bridgeServer);
    return bridgeServer;
  }


  protected Pool getClientPool(String host, int server1Port, int server2Port,
      boolean establishCallbackConnection, int redundancyLevel) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port).addServer(host, server2Port)
        .setSubscriptionEnabled(establishCallbackConnection)
        .setSubscriptionRedundancy(redundancyLevel).setSubscriptionAckInterval(1);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }

  /**
   * Returns the durable client proxy's HARegionQueue region name. This method is accessed via
   * reflection on a server VM.
   *
   * @return the durable client proxy's HARegionQueue region name
   */
  protected static String getHARegionQueueName() {
    checkNumberOfClientProxies(1);
    CacheClientProxy proxy = getClientProxy();
    assertNotNull(proxy);
    return proxy.getHARegionName();
  }

  public static void verifyReceivedMarkerAck(final CacheClientProxy proxy) {
    Awaitility.waitAtMost(3, TimeUnit.MINUTES).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> checkForAck(proxy));
  }

  /**
   * This is an incredibly ugly test to see if we got an ack from the client. the dispatched
   * messages map is <b> static<b> map, which has region queue names for keys and MapWrappers (an
   * protected class) as values. All this is testing is to see that this queue has an entry in the
   * dispatchedMessages map, which means it got at least one periodic ack.
   *
   * @return true if there was an ack
   */
  protected static boolean checkForAck(CacheClientProxy proxy) {
    // pause(5000);
    return HARegionQueue.isTestMarkerMessageReceived();
  }

  protected static void setTestFlagToVerifyActForMarker(Boolean flag) {
    HARegionQueue.setUsedByTest(flag);
  }

  public static void setBridgeObeserverForAfterPrimaryRecovered() {
    DurableClientTestCase.isPrimaryRecovered = false;
    PoolImpl.AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG = true;
    ClientServerObserver bo =
        ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
          public void afterPrimaryRecovered(ServerLocation location) {
            DurableClientTestCase.isPrimaryRecovered = true;
            PoolImpl.AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG = false;
          }
        });
  }

  public void setPrimaryRecoveryCheck() {
    this.durableClientVM.invoke(new CacheSerializableRunnable("Set observer") {
      public void run2() {
        setBridgeObeserverForAfterPrimaryRecovered();
      }
    });
  }

  public void checkPrimaryRecovery() {
    this.durableClientVM.invoke(new CacheSerializableRunnable("Check observer") {
      public void run2() {
        Awaitility.waitAtMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
            .until(() -> DurableClientTestCase.isPrimaryRecovered);
      }
    });
  }


  protected void sendClientReady(VM vm) {
    // Send clientReady message
    vm.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });
  }

  protected void registerInterest(VM vm, final String regionName, final boolean durable) {
    vm.invoke(new CacheSerializableRunnable("Register interest on region : " + regionName) {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, durable);
      }
    });
  }

  protected void createCq(VM vm, final String cqName, final String cqQuery, final boolean durable) {
    vm.invoke(new CacheSerializableRunnable("Register cq " + cqName) {
      public void run2() throws CacheException {

        try {
          createCq(cqName, cqQuery, durable).execute();
        } catch (CqExistsException | CqException | RegionNotFoundException e) {
          throw new CacheException(e) {};
        }

      }
    });
  }

  protected void publishEntries(VM vm, final String regionName, final int numEntries) {
    vm.invoke(new CacheSerializableRunnable("publish " + numEntries + " entries") {
      public void run2() throws CacheException {
        // Get the region
        Region<String, Portfolio> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 0; i < numEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, new Portfolio(i));
        }
      }
    });
  }

  /*
   * Due to the way removal from ha region queue is implemented a dummy cq or interest needs to be
   * created and a dummy value used so that none of the actual cqs will be triggered and yet an
   * event will flush the queue
   */
  protected void flushEntries(VM server, VM client, final String regionName) {
    // This wait is to make sure that all acks have been responded to...
    // We can add a stat later on the cache client proxy stats that checks
    // ack counts
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    registerInterest(client, regionName, false);
    server.invoke(new CacheSerializableRunnable("flush entries") {
      public void run2() throws CacheException {
        CqService service = ((InternalCache) CacheServerTestUtil.getCache()).getCqService();
        // Get the region
        Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);
        region.put("LAST", "ENTRY");
      }
    });
  }


  protected void checkCqStatOnServer(VM server, final String durableClientId, final String cqName,
      final int expectedNumber) {
    server.invoke(new CacheSerializableRunnable(
        "Check ha queued cq stats for durable client " + durableClientId + " cq: " + cqName) {
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
        final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();
        CqService cqService = ((InternalCache) CacheServerTestUtil.getCache()).getCqService();
        cqService.start();
        final CqQueryImpl cqQuery = (CqQueryImpl) cqService.getClientCqFromServer(proxyId, cqName);

        // Wait until we get the expected number of events or until 10 seconds are up
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
            .until(() -> cqQuery.getVsdStats().getNumHAQueuedEvents() == expectedNumber);

        assertEquals(expectedNumber, cqQuery.getVsdStats().getNumHAQueuedEvents());
      }
    });
  }

  /*
   * Remaining is the number of events that could still be in the queue due to timing issues with
   * acks and receiving them after remove from ha queue region has been called.
   */
  protected void checkHAQueueSize(VM server, final String durableClientId, final int expectedNumber,
      final int remaining) {
    server.invoke(new CacheSerializableRunnable(
        "Check ha queued size for durable client " + durableClientId) {
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
        final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();

        // Wait until we get the expected number of events or until 10 seconds are up
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
            .until(() -> clientProxy.getQueueSizeStat() == expectedNumber
                || clientProxy.getQueueSizeStat() == remaining);

        assertTrue(clientProxy.getQueueSizeStat() == expectedNumber
            || clientProxy.getQueueSizeStat() == remaining);
      }
    });
  }

  protected void checkNumDurableCqs(VM server, final String durableClientId,
      final int expectedNumber) {
    server.invoke(new CacheSerializableRunnable(
        "check number of durable cqs on server for durable client: " + durableClientId) {
      public void run2() throws CacheException {
        try {
          final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
          final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);
          ClientProxyMembershipID proxyId = clientProxy.getProxyID();
          CqService cqService = ((InternalCache) CacheServerTestUtil.getCache()).getCqService();
          cqService.start();
          List<String> cqNames = cqService.getAllDurableClientCqs(proxyId);
          assertEquals(expectedNumber, cqNames.size());
        } catch (Exception e) {
          throw new CacheException(e) {};
        }
      }
    });
  }

  /*
   * @param numEventsToWaitFor most times will be the same as numEvents, but there are times where
   * we want to wait for an event we know is not coming just to be sure an event actually isnt
   * received
   *
   */
  protected void checkCqListenerEvents(VM vm, final String cqName, final int numEvents,
      final int numEventsToWaitFor, final int secondsToWait) {
    vm.invoke(new CacheSerializableRunnable("Verify events for cq: " + cqName) {
      public void run2() throws CacheException {
        QueryService qs = CacheServerTestUtil.getCache().getQueryService();
        CqQuery cq = qs.getCq(cqName);
        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlCqListener listener =
            (CacheServerTestUtil.ControlCqListener) cq.getCqAttributes().getCqListener();
        listener.waitWhileNotEnoughEvents(secondsToWait * 1000, numEventsToWaitFor);
        assertEquals(numEvents, listener.events.size());
      }
    });
  }

  protected void checkInterestEvents(VM vm, final String regionName, final int numEvents) {
    vm.invoke(new CacheSerializableRunnable("Verify interest events") {
      public void run2() throws CacheException {
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);

        CacheServerTestUtil.ControlListener clistener =
            (CacheServerTestUtil.ControlListener) region.getAttributes().getCacheListeners()[0];
        clistener.waitWhileNotEnoughEvents(30000, numEvents);
        assertEquals(numEvents, clistener.events.size());
      }
    });
  }

  protected void startDurableClient(VM vm, String durableClientId, int serverPort1,
      String regionName, int durableTimeoutInSeconds) {
    vm.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort1, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableTimeoutInSeconds),
        Boolean.TRUE));
  }

  protected void startDurableClient(VM vm, String durableClientId, int serverPort1,
      String regionName) {
    vm.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort1, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));
  }

  protected void startDurableClient(VM vm, String durableClientId, int serverPort1, int serverPort2,
      String regionName) {
    vm.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort1, serverPort2, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));
  }

  protected void startClient(VM vm, int serverPort1, String regionName) {
    vm.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort1, false),
        regionName));
  }

  protected void startClient(VM vm, int serverPort1, int serverPort2, String regionName) {
    vm.invoke(() -> CacheServerTestUtil
        .createCacheClient(getClientPool(NetworkUtils.getServerHostName(), serverPort1,
            serverPort2, false), regionName));
  }

  protected void verifyDurableClientOnServer(VM server, final String durableClientId) {
    server.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });
  }

  protected void checkPrimaryUpdater(VM vm) {
    vm.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {

        Awaitility.waitAtMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
            .until(() -> CacheServerTestUtil.getPool().isPrimaryUpdaterAlive());

        assertTrue(CacheServerTestUtil.getPool().isPrimaryUpdaterAlive());
      }
    });
  }

  protected void closeCache(VM vm) {
    vm.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }
}
