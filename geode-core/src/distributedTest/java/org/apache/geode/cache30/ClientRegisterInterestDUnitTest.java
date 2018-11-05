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
package org.apache.geode.cache30;

import static org.apache.geode.cache.Scope.LOCAL;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;

import java.io.IOException;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.SubscriptionNotEnabledException;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Tests the client register interest
 *
 * @since GemFire 4.2.3
 */
@Category({ClientSubscriptionTest.class})
public class ClientRegisterInterestDUnitTest extends ClientServerTestCase {

  protected static int bridgeServerPort;

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS(); // cleans up cache server and client and lonerDS
  }

  /**
   * Tests for Bug 35381 Calling register interest if establishCallbackConnection is not set causes
   * cache server NPE.
   */
  @Test
  public void testBug35381() throws Exception {
    final Host host = Host.getHost(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[1]; // 1 server in this test

    final int whichVM = 0;
    final VM vm = Host.getHost(0).getVM(whichVM);
    vm.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("[testBug35381] Create BridgeServer");
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region = createRegion(name, factory.create());
        assertNotNull(region);
        assertNotNull(getRootRegion().getSubregion(name));
        region.put("KEY-1", "VAL-1");

        try {
          bridgeServerPort = startBridgeServer(0);
        } catch (IOException e) {
          LogWriterUtils.getLogWriter().error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException ", e);
        }

        assertTrue(bridgeServerPort != 0);

        LogWriterUtils.getLogWriter().info("[testBug35381] port=" + bridgeServerPort);
        LogWriterUtils.getLogWriter().info("[testBug35381] serverMemberId=" + getMemberId());
      }
    });
    ports[whichVM] = vm.invoke(() -> ClientRegisterInterestDUnitTest.getBridgeServerPort());
    assertTrue(ports[whichVM] != 0);

    LogWriterUtils.getLogWriter().info("[testBug35381] create bridge client");
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    getSystem(config);
    getCache();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    LogWriterUtils.getLogWriter().info("[testBug35381] creating connection pool");
    boolean establishCallbackConnection = false; // SOURCE OF BUG 35381
    ClientServerTestCase.configureConnectionPool(factory, NetworkUtils.getServerHostName(host),
        ports, establishCallbackConnection, -1, -1, null);
    Region region = createRegion(name, factory.create());
    assertNotNull(getRootRegion().getSubregion(name));
    try {
      region.registerInterest("KEY-1");
      fail(
          "registerInterest failed to throw SubscriptionNotEnabledException with establishCallbackConnection set to false");
    } catch (SubscriptionNotEnabledException expected) {
    }
  }

  private static int getBridgeServerPort() {
    return bridgeServerPort;
  }

  /**
   * Tests failover of register interest from client point of view. Related bugs include:
   *
   * <p>
   * Bug 35654 "failed re-registration may never be detected and thus may never re-re-register"
   *
   * <p>
   * Bug 35639 "registerInterest re-registration happens everytime a healthy server is detected"
   *
   * <p>
   * Bug 35655 "a single failed re-registration causes all other pending re-registrations to be
   * cancelled"
   */
  @Ignore("TODO")
  @Test
  public void testRegisterInterestFailover() throws Exception {
    // controller is bridge client

    final Host host = getHost(0);
    final String name = this.getUniqueName();
    final String regionName1 = name + "-1";
    final String regionName2 = name + "-2";
    final String regionName3 = name + "-3";
    final String key1 = "KEY-" + regionName1 + "-1";
    final String key2 = "KEY-" + regionName1 + "-2";
    final String key3 = "KEY-" + regionName1 + "-3";
    final int[] ports = new int[3]; // 3 servers in this test

    // create first cache server with region for client...
    final int firstServerIdx = 0;
    final VM firstServerVM = getHost(0).getVM(firstServerIdx);
    firstServerVM.invoke(new CacheSerializableRunnable("Create first cache server") {
      public void run2() throws CacheException {
        getLogWriter()
            .info("[testRegisterInterestFailover] Create first cache server");
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(LOCAL);
        Region region1 = createRootRegion(regionName1, factory.create());
        Region region2 = createRootRegion(regionName2, factory.create());
        Region region3 = createRootRegion(regionName3, factory.create());
        region1.put(key1, "VAL-1");
        region2.put(key2, "VAL-1");
        region3.put(key3, "VAL-1");

        try {
          bridgeServerPort = startBridgeServer(0);
        } catch (IOException e) {
          getLogWriter().error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException ", e);
        }

        assertTrue(bridgeServerPort != 0);

        getLogWriter()
            .info("[testRegisterInterestFailover] " + "firstServer port=" + bridgeServerPort);
        getLogWriter()
            .info("[testRegisterInterestFailover] " + "firstServer memberId=" + getMemberId());
      }
    });

    // create second cache server missing region for client...
    final int secondServerIdx = 1;
    final VM secondServerVM = getHost(0).getVM(secondServerIdx);
    secondServerVM.invoke(new CacheSerializableRunnable("Create second cache server") {
      public void run2() throws CacheException {
        getLogWriter()
            .info("[testRegisterInterestFailover] Create second cache server");
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(LOCAL);
        Region region1 = createRootRegion(regionName1, factory.create());
        Region region3 = createRootRegion(regionName3, factory.create());
        region1.put(key1, "VAL-2");
        region3.put(key3, "VAL-2");

        try {
          bridgeServerPort = startBridgeServer(0);
        } catch (IOException e) {
          getLogWriter().error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException ", e);
        }

        assertTrue(bridgeServerPort != 0);

        getLogWriter()
            .info("[testRegisterInterestFailover] " + "secondServer port=" + bridgeServerPort);
        getLogWriter()
            .info("[testRegisterInterestFailover] " + "secondServer memberId=" + getMemberId());
      }
    });

    // get the cache server ports...
    ports[firstServerIdx] =
        firstServerVM.invoke(() -> getBridgeServerPort());
    assertTrue(ports[firstServerIdx] != 0);
    ports[secondServerIdx] =
        secondServerVM.invoke(() -> getBridgeServerPort());
    assertTrue(ports[secondServerIdx] != 0);
    assertTrue(ports[firstServerIdx] != ports[secondServerIdx]);

    // stop second and third servers
    secondServerVM.invoke(new CacheSerializableRunnable("Stop second cache server") {
      public void run2() throws CacheException {
        stopBridgeServers(getCache());
      }
    });

    // create the bridge client
    getLogWriter().info("[testBug35654] create bridge client");
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    getSystem(config);
    getCache();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(LOCAL);

    getLogWriter().info("[testRegisterInterestFailover] creating connection pool");
    boolean establishCallbackConnection = true;
    final PoolImpl p = (PoolImpl) configureConnectionPool(factory,
        getServerHostName(host), ports, establishCallbackConnection, -1, -1, null);

    final Region region1 = createRootRegion(regionName1, factory.create());
    final Region region2 = createRootRegion(regionName2, factory.create());
    final Region region3 = createRootRegion(regionName3, factory.create());

    assertTrue(region1.getInterestList().isEmpty());
    assertTrue(region2.getInterestList().isEmpty());
    assertTrue(region3.getInterestList().isEmpty());

    region1.registerInterest(key1);
    region2.registerInterest(key2);
    region3.registerInterest(key3);

    assertTrue(region1.getInterestList().contains(key1));
    assertTrue(region2.getInterestList().contains(key2));
    assertTrue(region3.getInterestList().contains(key3));

    assertTrue(region1.getInterestListRegex().isEmpty());
    assertTrue(region2.getInterestListRegex().isEmpty());
    assertTrue(region3.getInterestListRegex().isEmpty());

    // get ConnectionProxy and wait until connected to first server
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return p.getPrimaryPort() != -1;
      }

      public String description() {
        return "primary port remained invalid";
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    assertEquals(ports[firstServerIdx], p.getPrimaryPort());

    // assert intial values
    assertEquals("VAL-1", region1.get(key1));
    assertEquals("VAL-1", region2.get(key2));
    assertEquals("VAL-1", region3.get(key3));

    // do puts on server1 and make sure values come thru for all 3 registrations
    firstServerVM.invoke(new CacheSerializableRunnable("Puts from first cache server") {
      public void run2() throws CacheException {
        Region region1 = getCache().getRegion(regionName1);
        region1.put(key1, "VAL-1-1");
        Region region2 = getCache().getRegion(regionName2);
        region2.put(key2, "VAL-1-1");
        Region region3 = getCache().getRegion(regionName3);
        region3.put(key3, "VAL-1-1");
      }
    });

    ev = new WaitCriterion() {
      public boolean done() {
        if (!"VAL-1-1".equals(region1.get(key1)) || !"VAL-1-1".equals(region2.get(key2))
            || !"VAL-1-1".equals(region3.get(key3)))
          return false;
        return true;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    assertEquals("VAL-1-1", region1.get(key1));
    assertEquals("VAL-1-1", region2.get(key2));
    assertEquals("VAL-1-1", region3.get(key3));

    // force failover to server 2
    secondServerVM.invoke(new CacheSerializableRunnable("Start second cache server") {
      public void run2() throws CacheException {
        try {
          startBridgeServer(ports[secondServerIdx]);
        } catch (IOException e) {
          getLogWriter().error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException ", e);
        }
      }
    });

    firstServerVM.invoke(new CacheSerializableRunnable("Stop first cache server") {
      public void run2() throws CacheException {
        stopBridgeServers(getCache());
      }
    });

    // wait for failover to second server
    ev = new WaitCriterion() {
      public boolean done() {
        return ports[secondServerIdx] == p.getPrimaryPort();
      }

      public String description() {
        return "primary port never became " + ports[secondServerIdx];
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);

    try {
      assertEquals(null, region2.get(key2));
      fail("CacheLoaderException expected");
    } catch (CacheLoaderException e) {
    }

    // region2 registration should be gone now
    // do puts on server2 and make sure values come thru for only 2 registrations
    secondServerVM.invoke(new CacheSerializableRunnable("Puts from second cache server") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(LOCAL);
        createRootRegion(regionName2, factory.create());
      }
    });

    // assert that there is no actively registered interest on region2
    assertTrue(region2.getInterestList().isEmpty());
    assertTrue(region2.getInterestListRegex().isEmpty());

    region2.put(key2, "VAL-0");

    secondServerVM.invoke(new CacheSerializableRunnable("Put from second cache server") {
      public void run2() throws CacheException {
        Region region1 = getCache().getRegion(regionName1);
        region1.put(key1, "VAL-2-2");
        Region region2 = getCache().getRegion(regionName2);
        region2.put(key2, "VAL-2-1");
        Region region3 = getCache().getRegion(regionName3);
        region3.put(key3, "VAL-2-2");
      }
    });

    // wait for updates to come thru
    ev = new WaitCriterion() {
      public boolean done() {
        if (!"VAL-2-2".equals(region1.get(key1)) || !"VAL-2-2".equals(region3.get(key3)))
          return false;
        return true;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    assertEquals("VAL-2-2", region1.get(key1));
    assertEquals("VAL-0", region2.get(key2));
    assertEquals("VAL-2-2", region3.get(key3));

    // assert again that there is no actively registered interest on region2
    assertTrue(region2.getInterestList().isEmpty());

    // register interest again on region2 and make
    region2.registerInterest(key2);
    assertEquals("VAL-2-1", region2.get(key2));

    secondServerVM.invoke(new CacheSerializableRunnable("Put from second cache server") {
      public void run2() throws CacheException {
        Region region1 = getCache().getRegion(regionName1);
        region1.put(key1, "VAL-2-3");
        Region region2 = getCache().getRegion(regionName2);
        region2.put(key2, "VAL-2-2");
        Region region3 = getCache().getRegion(regionName3);
        region3.put(key3, "VAL-2-3");
      }
    });

    // wait for updates to come thru
    ev = new WaitCriterion() {
      public boolean done() {
        if (!"VAL-2-3".equals(region1.get(key1)) || !"VAL-2-2".equals(region2.get(key2))
            || !"VAL-2-3".equals(region3.get(key3)))
          return false;
        return true;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    assertEquals("VAL-2-3", region1.get(key1));
    assertEquals("VAL-2-2", region2.get(key2));
    assertEquals("VAL-2-3", region3.get(key3));

    // assert public methods report actively registered interest on region2
    assertTrue(region2.getInterestList().contains(key2));
  }



  @Test
  public void rejectAttemptToRegisterInterestInLonerSystem() throws Exception {
    final String name = this.getUniqueName();
    final String regionName1 = name + "-1";

    // create first cache server with region for client...
    final int firstServerIdx = 1;

    final VM firstServerVM = Host.getHost(0).getVM(firstServerIdx);
    firstServerVM.invoke(new CacheSerializableRunnable("Create first cache server") {
      public void run2() throws CacheException {
        Cache cache = new CacheFactory().set("mcast-port", "0").create();

        try {
          CacheServer bridge = cache.addCacheServer();
          bridge.setPort(0);
          bridge.setMaxThreads(getMaxThreads());
          bridge.start();
          bridgeServerPort = bridge.getPort();
        } catch (IOException e) {
          LogWriterUtils.getLogWriter().error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException ", e);
        }
        assertTrue(bridgeServerPort != 0);

        Region region1 = cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName1);
      }
    });

    // get the cache server ports...
    int port = firstServerVM.invoke(() -> ClientRegisterInterestDUnitTest.getBridgeServerPort());

    try {
      ClientCache clientCache =
          new ClientCacheFactory().addPoolServer(firstServerVM.getHost().getHostName(), port)
              .setPoolSubscriptionEnabled(true).create();
      Region region = clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(regionName1);
      region.registerInterestRegex(".*");
      fail();
    } catch (ServerOperationException e) {
      // expected
      if (!e.getRootCause().getMessage().equals(
          "Should not register interest for a partitioned region when mcast-port is 0 and no locator is present")) {
        fail();
      }
    }
  }

}
