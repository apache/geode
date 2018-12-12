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
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.ha.HAContainerRegion;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class CacheClientNotifierDUnitTest extends WANTestBase {

  private static final int NUM_KEYS = 10;

  private int createCacheServerWithCSC(VM vm, final boolean withCSC, final int capacity,
      final String policy, final String diskStoreName) {
    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    SerializableRunnable createCacheServer = new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        CacheServerImpl server = (CacheServerImpl) cache.addCacheServer();
        server.setPort(serverPort);
        if (withCSC) {
          if (diskStoreName != null) {
            DiskStore ds = cache.findDiskStore(diskStoreName);
            if (ds == null) {
              ds = cache.createDiskStoreFactory().create(diskStoreName);
            }
          }
          ClientSubscriptionConfig csc = server.getClientSubscriptionConfig();
          csc.setCapacity(capacity);
          csc.setEvictionPolicy(policy);
          csc.setDiskStoreName(diskStoreName);
          server.setHostnameForClients("localhost");
          // server.setGroups(new String[]{"serv"});
        }
        try {
          server.start();
        } catch (IOException e) {
          org.apache.geode.test.dunit.Assert.fail("Failed to start server ", e);
        }
      }
    };
    vm.invoke(createCacheServer);
    return serverPort;
  }

  private void checkCacheServer(VM vm, final int serverPort, final boolean withCSC,
      final int capacity) {
    SerializableRunnable checkCacheServer = new SerializableRunnable() {

      @Override
      public void run() throws Exception {
        List<CacheServer> cacheServers =
            ((GemFireCacheImpl) cache).getCacheServersAndGatewayReceiver();
        CacheServerImpl server = null;
        for (CacheServer cs : cacheServers) {
          if (cs.getPort() == serverPort) {
            server = (CacheServerImpl) cs;
            break;
          }
        }
        assertNotNull(server);
        CacheClientNotifier ccn = server.getAcceptor().getCacheClientNotifier();
        HAContainerRegion haContainer = (HAContainerRegion) ccn.getHaContainer();
        if (server.getAcceptor().isGatewayReceiver()) {
          assertNull(haContainer);
          return;
        }
        Region internalRegion = haContainer.getMapForTest();
        RegionAttributes ra = internalRegion.getAttributes();
        EvictionAttributes ea = ra.getEvictionAttributes();
        if (withCSC) {
          assertNotNull(ea);
          assertEquals(capacity, ea.getMaximum());
          assertEquals(EvictionAction.OVERFLOW_TO_DISK, ea.getAction());
        } else {
          assertNull(ea);
        }
      }
    };
    vm.invoke(checkCacheServer);
  }

  public static void closeACacheServer(final int serverPort) {
    List<CacheServer> cacheServers = cache.getCacheServers();
    CacheServerImpl server = null;
    for (CacheServer cs : cacheServers) {
      if (cs.getPort() == serverPort) {
        server = (CacheServerImpl) cs;
        break;
      }
    }
    assertNotNull(server);
    server.stop();
  }

  private void verifyRegionSize(VM vm, final int expect) {
    SerializableRunnable verifyRegionSize = new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        final Region region = cache.getRegion(getTestMethodName() + "_PR");

        await().untilAsserted(() -> assertEquals(expect, region.size()));
      }
    };
    vm.invoke(verifyRegionSize);
  }

  /**
   * The test will start several cache servers, including gateway receivers. Shutdown them and
   * verify the CacheClientNotifier for each server is correct
   */
  @Test
  public void testNormalClient2MultipleCacheServer() throws Exception {
    doMultipleCacheServer(false);
  }

  public void doMultipleCacheServer(boolean durable) throws Exception {
    /* test scenario: */
    /* create 1 GatewaySender on vm5 */
    /* create 1 GatewayReceiver on vm2 */
    /* create 2 cache servers on vm2, one with overflow. */
    /* verify if the cache server2 still has the overflow attributes */
    /* create 1 cache client1 on vm3 to register interest on cache server1 */
    /* create 1 cache client2 on vm4 to register interest on cache server1 */
    /* do some puts to GatewaySender on vm5 */

    // start locators
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // create receiver and cache servers will be at ny
    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    int receiverPort = vm2.invoke(() -> WANTestBase.createReceiver());
    checkCacheServer(vm2, receiverPort, false, 0);

    // create PR for receiver
    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        null, 1, 100, isOffHeap()));

    // create cache server1 with overflow
    int serverPort = createCacheServerWithCSC(vm2, true, 3, "entry", "DEFAULT");
    checkCacheServer(vm2, serverPort, true, 3);

    // create cache server 2
    final int serverPort2 = createCacheServerWithCSC(vm2, false, 0, null, null);
    // Currently, only the first cache server's overflow attributes will take effect
    // It will be enhanced in GEODE-1102
    checkCacheServer(vm2, serverPort2, true, 3);
    LogService.getLogger().info("receiverPort=" + receiverPort + ",serverPort=" + serverPort
        + ",serverPort2=" + serverPort2);

    vm3.invoke(() -> createClientWithLocator(nyPort, "localhost", getTestMethodName() + "_PR",
        "123", durable));
    vm4.invoke(() -> createClientWithLocator(nyPort, "localhost", getTestMethodName() + "_PR",
        "124", durable));

    // create sender at ln
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.startSender("ln"));
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", NUM_KEYS));

    /* verify */
    verifyRegionSize(vm5, NUM_KEYS);
    verifyRegionSize(vm2, NUM_KEYS);
    verifyRegionSize(vm4, NUM_KEYS);
    verifyRegionSize(vm3, NUM_KEYS);

    // close a cache server, then re-test
    vm2.invoke(() -> closeACacheServer(serverPort2));

    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", NUM_KEYS * 2));

    /* verify */
    verifyRegionSize(vm5, NUM_KEYS * 2);
    verifyRegionSize(vm2, NUM_KEYS * 2);
    verifyRegionSize(vm4, NUM_KEYS * 2);
    verifyRegionSize(vm3, NUM_KEYS * 2);

    disconnectAllFromDS();
  }

  public static void createClientWithLocator(int port0, String host, String regionName,
      String clientId, boolean isDurable) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    if (isDurable) {
      props.setProperty(DURABLE_CLIENT_ID, clientId);
      props.setProperty(DURABLE_CLIENT_TIMEOUT, "" + 200);
    }

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setSocketBufferSize(1000)
          .setMinConnections(6).setMaxConnections(10).setRetryAttempts(3).create(regionName);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(p.getName());
    factory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(regionName, attrs);
    region.registerInterest("ALL_KEYS");
    assertNotNull(region);
    if (isDurable) {
      cache.readyForEvents();
    }
    LogWriterUtils.getLogWriter()
        .info("Distributed Region " + regionName + " created Successfully :" + region.toString()
            + " in a " + (isDurable ? "durable" : "") + " client");
  }
}
