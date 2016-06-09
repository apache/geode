/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.wan;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.ha.HAContainerRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class CacheClientNotifierDUnitTest extends WANTestBase {

  private static final int NUM_KEYS = 10;
  
  private int createCacheServerWithCSC(VM vm, final boolean withCSC, final int capacity,
      final String policy, final String diskStoreName) {
    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    SerializableRunnable createCacheServer = new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        CacheServerImpl server = (CacheServerImpl)cache.addCacheServer();
        server.setPort(serverPort);
        if (withCSC) {
          if (diskStoreName != null) {
            DiskStore ds = cache.findDiskStore(diskStoreName);
            if(ds == null) {
              ds = cache.createDiskStoreFactory().create(diskStoreName);
            }
          }
          ClientSubscriptionConfig csc = server.getClientSubscriptionConfig();
          csc.setCapacity(capacity);
          csc.setEvictionPolicy(policy);
          csc.setDiskStoreName(diskStoreName);
          server.setHostnameForClients("localhost");
          //server.setGroups(new String[]{"serv"});
        }
        try {
          server.start();
        } catch (IOException e) {
          com.gemstone.gemfire.test.dunit.Assert.fail("Failed to start server ", e);
        }
      }
    };
    vm.invoke(createCacheServer);
    return serverPort;
  }

  private void checkCacheServer(VM vm, final int serverPort, final boolean withCSC, final int capacity) {
    SerializableRunnable checkCacheServer = new SerializableRunnable() {

      @Override
      public void run() throws Exception {
        List<CacheServer> cacheServers = ((GemFireCacheImpl)cache).getCacheServersAndGatewayReceiver();
        CacheServerImpl server = null;
        for (CacheServer cs:cacheServers) {
          if (cs.getPort() == serverPort) {
            server = (CacheServerImpl)cs;
            break;
          }
        }
        assertNotNull(server);
        CacheClientNotifier ccn = server.getAcceptor().getCacheClientNotifier();
        HAContainerRegion haContainer = (HAContainerRegion)ccn.getHaContainer();
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
    for (CacheServer cs:cacheServers) {
      if (cs.getPort() == serverPort) {
        server = (CacheServerImpl)cs;
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

        Wait.waitForCriterion(new WaitCriterion() {
          public boolean done() {
            return region.size() == expect; 
          }
          public String description() {
            return null;
          }
        }, 60000, 100, false);
        assertEquals(expect, region.size());
      }
    };
    vm.invoke(verifyRegionSize);
  }
  
  /**
   * The test will start several cache servers, including gateway receivers.
   * Shutdown them and verify the CacheClientNotifier for each server is correct
   */
  @Category(FlakyTest.class) // GEODE-1183: random ports, failure to start threads, eats exceptions, time sensitive
  @Test
  public void testNormalClient2MultipleCacheServer() throws Exception {
    doMultipleCacheServer(false);
  }

  public void doMultipleCacheServer(boolean durable) throws Exception {
    /* test senario: */
    /* create 1 GatewaySender on vm0 */
    /* create 1 GatewayReceiver on vm1 */
    /* create 2 cache servers on vm1, one with overflow. */
    /* verify if the cache server2 still has the overflow attributes */
    /* create 1 cache client1 on vm2 to register interest on cache server1 */
    /* create 1 cache client2 on vm3 to register interest on cache server1 */
    /* do some puts to GatewaySender on vm0 */
    
    // create sender at ln
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    
    // create recever and cache servers will be at ny
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));
    vm1.invoke(() -> WANTestBase.createCache( nyPort ));
    int receiverPort = vm1.invoke(() -> WANTestBase.createReceiver());
    checkCacheServer(vm1, receiverPort, false, 0);
    
    // create PR for receiver
    vm1.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    
    // create cache server1 with overflow
    int serverPort = createCacheServerWithCSC(vm1, true, 3, "entry", "DEFAULT");
    checkCacheServer(vm1, serverPort, true, 3);
    
    // create cache server 2
    final int serverPort2 = createCacheServerWithCSC(vm1, false, 0, null, null);
    // Currently, only the first cache server's overflow attributes will take effect
    // It will be enhanced in GEODE-1102
    checkCacheServer(vm1, serverPort2, true, 3);
    LogService.getLogger().info("receiverPort="+receiverPort+",serverPort="+serverPort+",serverPort2="+serverPort2);
    
    vm2.invoke(() -> createClientWithLocator(nyPort, "localhost", getTestMethodName() + "_PR", "123", durable));
    vm3.invoke(() -> createClientWithLocator(nyPort, "localhost", getTestMethodName() + "_PR", "124", durable));

    vm0.invoke(() -> WANTestBase.createCache( lnPort ));
    vm0.invoke(() -> WANTestBase.createSender( "ln", 2, false, 100, 400, false, false, null, true ));
    vm0.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm0.invoke(() -> WANTestBase.startSender( "ln" ));
    vm0.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", NUM_KEYS ));
    
    /* verify */
    verifyRegionSize(vm0, NUM_KEYS);
    verifyRegionSize(vm1, NUM_KEYS);
    verifyRegionSize(vm3, NUM_KEYS);
    verifyRegionSize(vm2, NUM_KEYS);

    // close a cache server, then re-test
    vm1.invoke(() -> closeACacheServer(serverPort2));

    vm0.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", NUM_KEYS*2 ));

    /* verify */
    verifyRegionSize(vm0, NUM_KEYS*2);
    verifyRegionSize(vm1, NUM_KEYS*2);
    verifyRegionSize(vm3, NUM_KEYS*2);
    verifyRegionSize(vm2, NUM_KEYS*2);
    
    disconnectAllFromDS();
  }

  public static void createClientWithLocator(int port0,String host,
      String regionName, String clientId, boolean isDurable) {
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
      p = PoolManager.createFactory().addLocator(host, port0)
          .setPingInterval(250).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(regionName);
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
    LogWriterUtils.getLogWriter().info(
        "Distributed Region " + regionName + " created Successfully :"
            + region.toString() + " in a "+(isDurable?"durable":"")+" client");
  }
}
