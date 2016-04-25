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

import java.util.Iterator;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;

public class Simple2CacheServerDUnitTest extends WANTestBase {
  private static final int NUM_KEYS = 10;
  static int afterPrimaryCount = 0;
  static int afterProxyReinitialized = 0;
  
  public Simple2CacheServerDUnitTest(String name) {
    super(name);
  }
  
  // GEODE-1183: random ports, failure to start threads, eats exceptions, time sensitive
  public void testDurableClient2MultipleCacheServer() throws Exception {
    doMultipleCacheServer(true);
  }

  public void testNormalClient2MultipleCacheServer() throws Exception {
    doMultipleCacheServer(false);
  }
  
  public void doMultipleCacheServer(boolean durable) throws Exception {
    Integer lnPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    vm1.invoke(() -> WANTestBase.createCache( lnPort ));
    vm1.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    int serverPort = vm1.invoke(() -> WANTestBase.createCacheServer());
    int serverPort2 = vm1.invoke(() -> WANTestBase.createCacheServer());

    if (durable) {
      vm1.invoke(() -> setCacheClientProxyTestHook());
    } else {
      vm2.invoke(() -> setClientServerObserver());
    }
    vm2.invoke(() -> CacheClientNotifierDUnitTest.createClientWithLocator(lnPort, "localhost", getTestMethodName() + "_PR" , "123", durable));

    vm0.invoke(() -> WANTestBase.createCache( lnPort ));
    vm0.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    int serverPort3 = vm0.invoke(() -> WANTestBase.createCacheServer());
    
    if (durable) {
      vm1.invoke(() -> checkResultAndUnsetCacheClientProxyTestHook());
    } else {
      vm2.invoke(() -> checkResultAndUnsetClientServerObserver());
    }
    
    boolean vm0_proxy = checkProxyIsPrimary(vm0);
    boolean vm1_proxy = checkProxyIsPrimary(vm1);
    assertTrue(vm1_proxy || vm0_proxy);
    
    // close the current primary cache server, then re-test
    vm1.invoke(()-> CacheClientNotifierDUnitTest.closeACacheServer(serverPort2));
    vm0_proxy = checkProxyIsPrimary(vm0);
    vm1_proxy = checkProxyIsPrimary(vm1);
    assertTrue(vm1_proxy || vm0_proxy);
    
    disconnectAllFromDS();
  }

  public static void setClientServerObserver()
  {
    PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
    ClientServerObserverHolder
    .setInstance(new ClientServerObserverAdapter() {
      public void afterPrimaryIdentificationFromBackup(ServerLocation primaryEndpoint)
      {
        LogService.getLogger().info("After primary is set");
        afterPrimaryCount++;
      }
    });
  }

  public static void checkResultAndUnsetClientServerObserver()
  {
    PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
    // setPrimary only happened once
    assertEquals(1, afterPrimaryCount);
    afterPrimaryCount = 0;
  }

  public static void setCacheClientProxyTestHook()
  {
    CacheClientProxy.testHook = new CacheClientProxy.TestHook() {
      @Override
      public void doTestHook(String spot) {
        if (spot.equals("CLIENT_RECONNECTED")) {
          afterProxyReinitialized++;
        }
      }
    };
  }

  public static void checkResultAndUnsetCacheClientProxyTestHook()
  {
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
        
        Wait.waitForCriterion(new WaitCriterion() {
          public boolean done() {
            return ccn.getClientProxies().size() == 1; 
          }
          public String description() {
            return null;
          }
        }, 20000, 100, false);
        assertEquals(1, ccn.getClientProxies().size());

        Iterator iter_prox = ccn.getClientProxies().iterator();
        assertEquals(1, ccn.getClientProxies().size());
        CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
        return proxy.isPrimary();
      }
    };
    return (Boolean)vm.invoke(checkProxyIsPrimary);
  }
}
