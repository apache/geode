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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.ha.HAContainerRegion;
import org.apache.geode.test.junit.categories.FlakyTest;
import com.jayway.awaitility.Awaitility;

@Category(DistributedTest.class)
public class Simple2CacheServerDUnitTest extends WANTestBase {
  private static final int NUM_KEYS = 10;
  static int afterPrimaryCount = 0;
  static int afterProxyReinitialized = 0;

  public Simple2CacheServerDUnitTest() {
    super();
  }

  @Test
  public void testNormalClient2MultipleCacheServer() throws Exception {
    doMultipleCacheServer(false);
  }

  public void doMultipleCacheServer(boolean durable) throws Exception {
    Integer lnPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    vm1.invoke(() -> WANTestBase.createCache(lnPort));
    vm1.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        null, 1, 100, isOffHeap()));
    int serverPort = vm1.invoke(() -> WANTestBase.createCacheServer());
    int serverPort2 = vm1.invoke(() -> WANTestBase.createCacheServer());

    if (durable) {
      vm1.invoke(() -> setCacheClientProxyTestHook());
    } else {
      vm2.invoke(() -> setClientServerObserver());
    }
    vm2.invoke(() -> CacheClientNotifierDUnitTest.createClientWithLocator(lnPort, "localhost",
        getTestMethodName() + "_PR", "123", durable));

    vm0.invoke(() -> WANTestBase.createCache(lnPort));
    vm0.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        null, 1, 100, isOffHeap()));
    int serverPort3 = vm0.invoke(() -> WANTestBase.createCacheServer());

    if (durable) {
      vm1.invoke(() -> checkResultAndUnsetCacheClientProxyTestHook());
    } else {
      vm2.invoke(() -> checkResultAndUnsetClientServerObserver());
    }
    Awaitility.waitAtMost(20, TimeUnit.SECONDS).until(() -> {
      return checkProxyIsPrimary(vm0) || checkProxyIsPrimary(vm1);
    });

    // close the current primary cache server, then re-test
    int serverPortAtVM1 = vm1.invoke(() -> findCacheServerForPrimaryProxy());
    if (serverPortAtVM1 != 0) {
      vm1.invoke(() -> CacheClientNotifierDUnitTest.closeACacheServer(serverPortAtVM1));
      LogService.getLogger().info("Closed cache server on vm1:" + serverPortAtVM1);
      Awaitility.waitAtMost(20, TimeUnit.SECONDS).until(() -> {
        return checkProxyIsPrimary(vm0) || checkProxyIsPrimary(vm1);
      });
    } else {
      vm0.invoke(() -> CacheClientNotifierDUnitTest.closeACacheServer(serverPort3));
      LogService.getLogger().info("Closed cache server on vm0:" + serverPort3);
      assertTrue(checkProxyIsPrimary(vm1));
    }
    disconnectAllFromDS();
  }

  private static int findCacheServerForPrimaryProxy() {
    List<CacheServer> cacheServers = ((GemFireCacheImpl) cache).getCacheServers();
    CacheServerImpl server = null;
    for (CacheServer cs : cacheServers) {
      server = (CacheServerImpl) cs;
      long acceptorId = server.getAcceptor().getAcceptorId();
      for (CacheClientProxy proxy : CacheClientNotifier.getInstance().getClientProxies()) {
        if (proxy.isPrimary() == false) {
          continue;
        }
        if (proxy.getAcceptorId() == acceptorId) {
          LogService.getLogger()
              .info("Found cache server " + server + " for the primary proxy " + proxy);
          return server.getPort();
        }
      }
    }
    return 0;
  }

  public static void setClientServerObserver() {
    PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void afterPrimaryIdentificationFromBackup(ServerLocation primaryEndpoint) {
        LogService.getLogger().info("After primary is set");
        afterPrimaryCount++;
      }
    });
  }

  public static void checkResultAndUnsetClientServerObserver() {
    PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
    // setPrimary only happened once
    assertEquals(1, afterPrimaryCount);
    afterPrimaryCount = 0;
  }

  public static void setCacheClientProxyTestHook() {
    CacheClientProxy.testHook = new CacheClientProxy.TestHook() {
      @Override
      public void doTestHook(String spot) {
        if (spot.equals("CLIENT_RECONNECTED")) {
          afterProxyReinitialized++;
        }
      }
    };
  }

  public static void checkResultAndUnsetCacheClientProxyTestHook() {
    // Reinitialize only happened once
    CacheClientProxy.testHook = null;
    assertEquals(1, afterProxyReinitialized);
    afterProxyReinitialized = 0;
  }

  private boolean checkProxyIsPrimary(VM vm) {
    SerializableCallable checkProxyIsPrimary = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        final CacheClientNotifier ccn = CacheClientNotifier.getInstance();
        Awaitility.waitAtMost(20, TimeUnit.SECONDS).until(() -> {
          return (ccn.getClientProxies().size() == 1);
        });

        Iterator iter_prox = ccn.getClientProxies().iterator();
        CacheClientProxy proxy = (CacheClientProxy) iter_prox.next();
        return proxy.isPrimary();
      }
    };
    return (Boolean) vm.invoke(checkProxyIsPrimary);
  }
}
