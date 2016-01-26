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
package com.gemstone.gemfire.cache30;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.HashMap;

import junit.framework.AssertionFailedError;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.TombstoneService;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * concurrency-control tests for client/server
 * 
 * @author bruce
 *
 */
public class ClientServerCCEDUnitTest extends CacheTestCase {
  public static LocalRegion TestRegion;
  
  public void setup() {
    // for bug #50683 we need a short queue-removal-message processing interval
    HARegionQueue.setMessageSyncInterval(5);
  }
  
  public void tearDown2() {
    disconnectAllFromDS();
    HARegionQueue.setMessageSyncInterval(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
  }

  public ClientServerCCEDUnitTest(String name) {
    super(name);
  }

  public void testClientServerRRTombstoneGC() {
    clientServerTombstoneGCTest(getUniqueName(), true);
  }
  
  public void testClientServerPRTombstoneGC() {
    clientServerTombstoneGCTest(getUniqueName(), false);
  }
  
  public void testPutAllInNonCCEClient() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName() + "Region";
    
    int port = createServerRegion(vm0, name, true);
    createClientRegion(vm1, name, port, false);
    doPutAllInClient(vm1);
  }
  
  
  /**
   * test that distributed GC messages are sent to clients and properly processed
   * @param replicatedRegion whether to use a RR or PR in the servers
   */
  private void clientServerTombstoneGCTest(String uniqueName, boolean replicatedRegion) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final String name = uniqueName + "Region";


    createServerRegion(vm0, name, replicatedRegion);
    int port = createServerRegion(vm1, name, replicatedRegion);
    createClientRegion(vm2, name, port, true);
    createClientRegion(vm3, name, port, true);
    createEntries(vm2);
    destroyEntries(vm3);
    unregisterInterest(vm3);
    forceGC(vm0);
    if (!replicatedRegion) {
      //other bucket might be in vm1
      forceGC(vm1);
    }
    checkClientReceivedGC(vm2);
    checkClientDoesNotReceiveGC(vm3);
  }
  
  /**
   * for bug #40791 we pull tombstones into clients on get(), getAll() and
   * registerInterest() to protect the client cache from stray putAll
   * events sitting in backup queues on the server 
   */
  public void testClientRIGetsTombstonesRR() throws Exception {
    clientRIGetsTombstoneTest(getUniqueName(),true);
  }
  
  public void testClientRIGetsTombstonesPR() throws Exception {
    clientRIGetsTombstoneTest(getUniqueName(),false);
  }

  /**
   * test that clients receive tombstones in register-interest results
   */
  private void clientRIGetsTombstoneTest(String uniqueName, boolean replicatedRegion) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String name = uniqueName + "Region";


    createServerRegion(vm0, name, replicatedRegion);
    int port = createServerRegion(vm1, name, replicatedRegion);
    createEntries(vm0);
    destroyEntries(vm0);
    
    getLogWriter().info("***************** register interest on all keys");
    createClientRegion(vm2, name, port, true);
    registerInterest(vm2);
    ensureAllTombstonesPresent(vm2);
    
    getLogWriter().info("***************** clear cache and register interest on one key, Object0");
    clearLocalCache(vm2);
    registerInterestOneKey(vm2, "Object0");
    List<String> keys = new ArrayList(1);
    keys.add("Object0");
    ensureAllTombstonesPresent(vm2, keys);

    getLogWriter().info("***************** clear cache and register interest on four keys");
    clearLocalCache(vm2);
    keys = new ArrayList(4);
    for (int i=0; i<4; i++) {
      keys.add("Object"+i);
    }
    registerInterest(vm2, keys);
    ensureAllTombstonesPresent(vm2, keys);

    getLogWriter().info("***************** clear cache and register interest with regex on four keys");
    clearLocalCache(vm2);
    registerInterestRegex(vm2, "Object[0-3]");
    ensureAllTombstonesPresent(vm2, keys);

    getLogWriter().info("***************** fetch entries with getAll()");
    clearLocalCache(vm2);
    getAll(vm2);
    ensureAllTombstonesPresent(vm2);
  }
  
  public void testClientRIGetsInvalidEntriesRR() throws Exception {
    clientRIGetsInvalidEntriesTest(getUniqueName(),true);
  }
  
  public void testClientRIGetsInvalidEntriesPR() throws Exception {
    clientRIGetsInvalidEntriesTest(getUniqueName(),false);
  }

  private void clientRIGetsInvalidEntriesTest(String uniqueName, boolean replicatedRegion) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String name = uniqueName + "Region";


    createServerRegion(vm0, name, replicatedRegion);
    int port = createServerRegion(vm1, name, replicatedRegion);
    createEntries(vm0);
    invalidateEntries(vm0);
    
    getLogWriter().info("***************** register interest on all keys");
    createClientRegion(vm2, name, port, true);
    registerInterest(vm2);
    ensureAllInvalidsPresent(vm2);
    
    getLogWriter().info("***************** clear cache and register interest on one key, Object0");
    clearLocalCache(vm2);
    registerInterestOneKey(vm2, "Object0");
    List<String> keys = new ArrayList(1);
    keys.add("Object0");
    ensureAllInvalidsPresent(vm2, keys);

    getLogWriter().info("***************** clear cache and register interest on four keys");
    clearLocalCache(vm2);
    keys = new ArrayList(4);
    for (int i=0; i<4; i++) {
      keys.add("Object"+i);
    }
    registerInterest(vm2, keys);
    ensureAllInvalidsPresent(vm2, keys);

    getLogWriter().info("***************** clear cache and register interest with regex on four keys");
    clearLocalCache(vm2);
    registerInterestRegex(vm2, "Object[0-3]");
    ensureAllInvalidsPresent(vm2, keys);

    getLogWriter().info("***************** fetch entries with getAll()");
    clearLocalCache(vm2);
    getAll(vm2);
    ensureAllInvalidsPresent(vm2);
  }

  
  private void registerInterest(VM vm) {
    vm.invoke(new SerializableRunnable("register interest in all keys") {
      public void run() {
        TestRegion.registerInterestRegex(".*");
      }
    });
  }
  
  private void unregisterInterest(VM vm) {
    vm.invoke(new SerializableRunnable("unregister interest in all keys") {
      public void run() {
//        TestRegion.dumpBackingMap();
        TestRegion.unregisterInterestRegex(".*");
//        TestRegion.dumpBackingMap();
      }
    });
  }
  
  private void registerInterest(VM vm, final List keys) {
    vm.invoke(new SerializableRunnable("register interest in key list") {
      public void run() {
        TestRegion.registerInterest(keys);
      }
    });
  }
  
  private void registerInterestOneKey(VM vm, final String key) {
    vm.invoke(new SerializableRunnable("register interest in " + key) {
      public void run() {
        TestRegion.registerInterest(key);
      }
    });
  }
  
  private void registerInterestRegex(VM vm, final String pattern) {
    vm.invoke(new SerializableRunnable("register interest in key list") {
      public void run() {
        TestRegion.registerInterestRegex(pattern);
      }
    });
  }
  
  private void ensureAllTombstonesPresent(VM vm) {
    vm.invoke(new SerializableCallable("check all are tombstones") {
      public Object call() {
        for (int i=0; i<10; i++) {
          assertTrue("expected a tombstone for Object"+i, TestRegion.containsTombstone("Object"+i));
        }
        return null;
      }
    });
  }
  
  private void ensureAllTombstonesPresent(VM vm, final List keys) {
    vm.invoke(new SerializableCallable("check tombstones in list") {
      public Object call() {
        for (Object key: keys) {
          assertTrue("expected to find a tombstone for "+key, TestRegion.containsTombstone(key));
        }
        return null;
      }
    });
  }
  
  private void ensureAllInvalidsPresent(VM vm) {
    vm.invoke(new SerializableCallable("check all are tombstones") {
      public Object call() {
        for (int i=0; i<10; i++) {
          assertTrue("expected to find an entry for Object"+i, TestRegion.containsKey("Object"+i));
          assertTrue("expected to find entry invalid for Object"+i, !TestRegion.containsValue("Object"+i));
        }
        return null;
      }
    });
  }
  
  private void ensureAllInvalidsPresent(VM vm, final List keys) {
    vm.invoke(new SerializableCallable("check tombstones in list") {
      public Object call() {
        for (Object key: keys) {
          assertTrue("expected to find an entry for "+key, TestRegion.containsKey(key));
          assertTrue("expected to find entry invalid for "+key, !TestRegion.containsValue(key));
        }
        return null;
      }
    });
  }

  /* do a getAll of all keys */
  private void getAll(VM vm) {
    vm.invoke(new SerializableRunnable("getAll for all keys") {
      public void run() {
        Set<String> keys = new HashSet();
        for (int i=0; i<10; i++) {
          keys.add("Object"+i);
        }
        Map result = TestRegion.getAll(keys);
        for (int i=0; i<10; i++) {
          assertNull("expected no result for Object"+i, result.get("Object"+i));
        }
      }
    });
  }

  /* this should remove all entries from the region, including tombstones */
  private void clearLocalCache(VM vm) {
    vm.invoke(new SerializableRunnable("clear local cache") {
      public void run() {
        TestRegion.localClear();
      }
    });
  }

  //  private void closeCache(VM vm) {

  public void testClientServerRRQueueCleanup() {  // see bug #50879 if this fails
    clientServerTombstoneMessageTest(true);
  }
  
  public void testClientServerPRQueueCleanup() {  // see bug #50879 if this fails
    clientServerTombstoneMessageTest(false);
  }

  /**
   * test that distributed GC messages are properly cleaned out of durable
   * client HA queues
   */
  private void clientServerTombstoneMessageTest(boolean replicatedRegion) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final String name = this.getUniqueName() + "Region";


    int port1 = createServerRegion(vm0, name, replicatedRegion);
    int port2 = createServerRegion(vm1, name, replicatedRegion);
    createDurableClientRegion(vm2, name, port1, port2, true);
    createDurableClientRegion(vm3, name, port1, port2, true);
    createEntries(vm2);
    destroyEntries(vm3);
    forceGC(vm0);
    if (!replicatedRegion) {
      //other bucket might be in vm1
      forceGC(vm1);
    }
    pause(5000); // better chance that WaitCriteria will succeed 1st time if we pause a bit
    checkClientReceivedGC(vm2);
    checkClientReceivedGC(vm3);
    checkServerQueuesEmpty(vm0);
    checkServerQueuesEmpty(vm1);
  }
  

//  private void closeCache(VM vm) {
//    vm.invoke(new SerializableCallable() {
//      public Object call() throws Exception {
//        closeCache();
//        return null;
//      }
//    });
//  }
  
  private void createEntries(VM vm) {
    vm.invoke(new SerializableCallable("create entries") {
      public Object call() {
        for (int i=0; i<10; i++) {
          TestRegion.create("Object"+i, Integer.valueOf(i));
        }
        return null;
      }
    });
  }
  

  private void destroyEntries(VM vm) {
    vm.invoke(new SerializableCallable("destroy entries") {
      public Object call() {
        for (int i=0; i<10; i++) {
          TestRegion.destroy("Object"+i, Integer.valueOf(i));
        }
        assertEquals(0, TestRegion.size());
        if (TestRegion.getDataPolicy().isReplicate()) {
          assertEquals(10, TestRegion.getTombstoneCount());
        }
        return null;
      }
    });
  }
  
  private void doPutAllInClient(VM vm) {
    vm.invoke(new SerializableRunnable("do putAll") {
      public void run() {
        Map map = new HashMap();
        for (int i=1000; i<1100; i++) {
          map.put("object_"+i, i);
        }
        try {
          TestRegion.putAll(map);
          for (int i=1000; i<1100; i++) {
            assertTrue("expected key object_"+i+" to be in the cache but it isn't", TestRegion.containsKey("object_"+i));
          }
        } catch (NullPointerException e) {
          fail("caught NPE", e);
        }
      }
    });
  }
  

  private void invalidateEntries(VM vm) {
    vm.invoke(new SerializableCallable("invalidate entries") {
      public Object call() {
        for (int i=0; i<10; i++) {
          TestRegion.invalidate("Object"+i, Integer.valueOf(i));
        }
        assertEquals(10, TestRegion.size());
        return null;
      }
    });
  }
  

  private void forceGC(VM vm) {
    vm.invoke(new SerializableCallable("force GC") {
      public Object call() throws Exception {
        TestRegion.getCache().getTombstoneService().forceBatchExpirationForTests(10);
        return null;
      }
    });
  }
  
  private void checkClientReceivedGC(VM vm) {
    vm.invoke(new SerializableCallable("check that GC happened") {
      public Object call() throws Exception {
        WaitCriterion wc = new WaitCriterion() {
          
          @Override
          public boolean done() {
            getLogWriter().info("tombstone count = " + TestRegion.getTombstoneCount());
            getLogWriter().info("region size = " + TestRegion.size());
            return TestRegion.getTombstoneCount() == 0 && TestRegion.size() == 0;
          }
          
          @Override
          public String description() {
            return "waiting for garbage collection to occur";
          }
        };
        waitForCriterion(wc, 60000, 2000, true);
        return null;
      }
    });
  }
        
  private void checkServerQueuesEmpty(VM vm) {
    vm.invoke(new SerializableCallable("check that client queues are properly cleared of old ClientTombstone messages") {

      public Object call() throws Exception {
        WaitCriterion wc = new WaitCriterion() {
//          boolean firstTime = true;
          
          @Override
          public boolean done() {
            CacheClientNotifier singleton = CacheClientNotifier.getInstance();
            Collection<CacheClientProxy> proxies = singleton.getClientProxies();
//            boolean first = firstTime;
//            firstTime = false;
            for (CacheClientProxy proxy: proxies) {
              if (!proxy.isPrimary()) {  // bug #50683 only applies to backup queues
                int size = proxy.getQueueSize();
                if (size > 0) {
//                  if (first) {
//                    ((LocalRegion)proxy.getHARegion()).dumpBackingMap();
//                  }
                  getLogWriter().info("queue size ("+size+") is still > 0 for " + proxy.getProxyID()); 
                  return false;
                }
              }
            }
            // also ensure that server regions have been cleaned up
            int regionEntryCount = TestRegion.getRegionMap().size();
            if (regionEntryCount > 0) {
              getLogWriter().info("TestRegion has unexpected entries - all should have been GC'd but we have " + regionEntryCount);
              TestRegion.dumpBackingMap();
              return false;
            }
            return true;
          }
          
          @Override
          public String description() {
            return "waiting for queue removal messages to clear client queues";
          }
        };
        waitForCriterion(wc, 60000, 2000, true);
        return null;
      }
    });
  }


  private void checkClientDoesNotReceiveGC(VM vm) {
    vm.invoke(new SerializableCallable("check that GC did not happen") {
      public Object call() throws Exception {
        if (TestRegion.getTombstoneCount() == 0) {
          getLogWriter().warning("region has no tombstones");
//          TestRegion.dumpBackingMap();
          throw new AssertionFailedError("expected to find tombstones but region is empty");
        }
        return null;
      }
    });
  }
        
        
  private int createServerRegion(VM vm, final String regionName, final boolean replicatedRegion) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
//        TombstoneService.VERBOSE = true;
        AttributesFactory af = new AttributesFactory();
        if (replicatedRegion) {
          af.setScope(Scope.DISTRIBUTED_ACK);
          af.setDataPolicy(DataPolicy.REPLICATE);
        } else {
          af.setDataPolicy(DataPolicy.PARTITION);
          af.setPartitionAttributes((new PartitionAttributesFactory()).setTotalNumBuckets(2).create());
        }
        TestRegion = (LocalRegion)createRootRegion(regionName, af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  
  
  private void createClientRegion(final VM vm, final String regionName, final int port, final boolean ccEnabled) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm.getHost()), port);
        cf.setPoolSubscriptionEnabled(true);
        cf.set("log-level", getDUnitLogLevel());
        ClientCache cache = getClientCache(cf);
        ClientRegionFactory crf = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        crf.setConcurrencyChecksEnabled(ccEnabled);
        TestRegion = (LocalRegion)crf.create(regionName);
        TestRegion.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES, false, true);
        return null;
      }
    };
    vm.invoke(createRegion);
  }

  // For durable client QRM testing we need a backup queue (redundancy=1) and
  // durable attributes.  We also need to invoke readyForEvents()
  private void createDurableClientRegion(final VM vm, final String regionName,
      final int port1, final int port2, final boolean ccEnabled) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm.getHost()), port1);
        cf.addPoolServer(getServerHostName(vm.getHost()), port2);
        cf.setPoolSubscriptionEnabled(true);
        cf.setPoolSubscriptionRedundancy(1);
        // bug #50683 - secondary durable queue retains all GC messages
        cf.set("durable-client-id", ""+vm.getPid());
        cf.set("durable-client-timeout", "" + 200);
        cf.set("log-level", getDUnitLogLevel());
        ClientCache cache = getClientCache(cf);
        ClientRegionFactory crf = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        crf.setConcurrencyChecksEnabled(ccEnabled);
        TestRegion = (LocalRegion)crf.create(regionName);
        TestRegion.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES, true, true);
        cache.readyForEvents();
        return null;
      }
    };
    vm.invoke(createRegion);
  }
}
