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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.Delta;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.DestroyOp;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.Distribution;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Disconnect;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * tests for the concurrentMapOperations. there are more tests in ClientServerMiscDUnitTest
 */

public class ConcurrentMapOpsDUnitTest implements Serializable {

  private static final String REP_REG_NAME = "repRegion";
  private static final String PR_REG_NAME = "prRegion";
  private static final int MAX_ENTRIES = 113;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();
  private int locatorPort;

  @Before
  public void setup() {
    // stress testing needs this so that join attempts don't give up too soon
    locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    Invoke.invokeInEveryVM(() -> System.setProperty("p2p.joinTimeout", "120000"));
    clusterStartupRule.startLocatorVM(0,
        locatorStarterRule -> locatorStarterRule
            .withPort(locatorPort)
            .withSystemProperty(BYPASS_DISCOVERY_PROPERTY, "true")
            .withSystemProperty(GeodeGlossary.GEMFIRE_PREFIX + "member-weight", "100")
            .withSystemProperty("p2p.joinTimeout", "120000"));
  }

  private void createRegions(MemberVM vm) {
    vm.invoke(() -> {
      createReplicateRegion();
      createPartitionedRegion();
    });
  }

  private Cache getServerCache() {
    return CacheFactory.getAnyInstance();
  }

  private Region createReplicateRegion() {
    Cache cache = getServerCache();
    return cache.createRegionFactory(RegionShortcut.REPLICATE)
        .setConcurrencyChecksEnabled(true).create(REP_REG_NAME);
  }

  private Region createPartitionedRegion() {
    Cache cache = getServerCache();
    return cache.createRegionFactory(RegionShortcut.PARTITION)
        .setConcurrencyChecksEnabled(true).create(PR_REG_NAME);
  }

  private MemberVM startServerAndCreateRegions(int vmID, final int locatorPort) {
    return startServerAndCreateRegions(vmID, false, locatorPort);
  }

  private MemberVM startServerAndCreateRegions(int vmID, boolean withRedundancy,
      final int locatorPort) {
    System.err.println("locatorPort = " + locatorPort);
    MemberVM memberVM =
        clusterStartupRule.startServerVM(vmID, serverStarterRule -> serverStarterRule
            .withProperty(DISABLE_AUTO_RECONNECT, "true")
            .withConnectionToLocator(locatorPort));
    memberVM.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(REP_REG_NAME);
      if (withRedundancy) {
        cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).create(PR_REG_NAME);
      } else {
        cache.createRegionFactory(RegionShortcut.PARTITION).create(PR_REG_NAME);
      }
    });
    return memberVM;
  }

  private ClientVM createClientRegionWithRI(int vmIndex, final boolean isEmpty, int... ports)
      throws Exception {
    return createClientRegion(vmIndex, isEmpty, true, ports);
  }

  private ClientVM createClientRegion(int vmIndex, final boolean isEmpty, int... ports)
      throws Exception {
    return createClientRegion(vmIndex, isEmpty, false, ports);
  }

  private ClientVM createClientRegion(int vmIndex, final boolean isEmpty,
      final boolean registerInterest, int... ports) throws Exception {
    ClientVM clientVM =
        clusterStartupRule.startClientVM(vmIndex, new Properties(), clientCacheFactory -> {
          for (int port : ports) {
            clientCacheFactory.addPoolServer("localhost", port);
          }
          clientCacheFactory.setPoolSubscriptionEnabled(true);
          clientCacheFactory.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        });

    clientVM.invoke(() -> {
      ClientCache clientCache = getClientCache();
      ClientRegionFactory<Integer, String> clientRegionFactory =
          clientCache.createClientRegionFactory(
              isEmpty ? ClientRegionShortcut.PROXY : ClientRegionShortcut.CACHING_PROXY);
      Region<Integer, String> replicateRegion = clientRegionFactory.create(REP_REG_NAME);
      Region<Integer, String> partitionRegion = clientRegionFactory.create(PR_REG_NAME);
      if (registerInterest) {
        replicateRegion.registerInterestRegex(".*");
        partitionRegion.registerInterestRegex(".*");
      }
    });
    return clientVM;
  }

  private abstract static class AbstractConcMapOpsListener
      implements CacheListener<Integer, String> {
    @Override
    public void afterCreate(EntryEvent<Integer, String> event) {
      validate(event);
    }

    @Override
    public void afterDestroy(EntryEvent<Integer, String> event) {
      validate(event);
    }

    @Override
    public void afterInvalidate(EntryEvent<Integer, String> event) {
      validate(event);
    }

    @Override
    public void afterRegionClear(RegionEvent<Integer, String> event) {}

    @Override
    public void afterRegionCreate(RegionEvent<Integer, String> event) {}

    @Override
    public void afterRegionDestroy(RegionEvent<Integer, String> event) {}

    @Override
    public void afterRegionInvalidate(RegionEvent<Integer, String> event) {}

    @Override
    public void afterRegionLive(RegionEvent<Integer, String> event) {}

    @Override
    public void afterUpdate(EntryEvent<Integer, String> event) {
      validate(event);
    }

    @Override
    public void close() {}

    abstract void validate(EntryEvent event);
  }

  private static class NotInvokedListener extends AbstractConcMapOpsListener {
    @Override
    void validate(EntryEvent event) {
      fail("should not be called.  Event=" + event);
    }
  }

  private static class InitialCreatesListener extends AbstractConcMapOpsListener {
    AtomicInteger numCreates = new AtomicInteger();

    @Override
    void validate(EntryEvent event) {
      if (!event.getOperation().isCreate()) {
        fail("expected only create events");
      }
      numCreates.incrementAndGet();
    }
  }

  // test for bug #42164
  @Test
  public void testListenerNotInvokedForRejectedOperation() throws Exception {
    MemberVM vm1 = startServerAndCreateRegions(1, locatorPort);
    MemberVM vm2 = startServerAndCreateRegions(2, locatorPort);
    ClientVM client1 = createClientRegionWithRI(3, true, vm1.getPort());
    ClientVM client2 = createClientRegionWithRI(4, true, vm2.getPort());

    SerializableRunnable addListenerToClientForInitialCreates = new SerializableRunnable() {
      @Override
      public void run() {
        ClientCache clientCache = ClientCacheFactory.getAnyInstance();
        Region replicateRegion = clientCache.getRegion(REP_REG_NAME);
        replicateRegion.getAttributesMutator().addCacheListener(new InitialCreatesListener());
        Region partitionRegion = clientCache.getRegion(PR_REG_NAME);
        partitionRegion.getAttributesMutator().addCacheListener(new InitialCreatesListener());
      }
    };
    client1.invoke(addListenerToClientForInitialCreates);
    client2.invoke(addListenerToClientForInitialCreates);

    vm1.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region<Integer, String> redundantRegion = cache.getRegion(REP_REG_NAME);
      Region<Integer, String> partitionedRegion = cache.getRegion(PR_REG_NAME);
      for (int i = 0; i < MAX_ENTRIES; i++) {
        redundantRegion.put(i, "value" + i);
        partitionedRegion.put(i, "value" + i);
      }
    });

    SerializableRunnable waitForInitialCreates = new SerializableRunnable() {
      @Override
      public void run() {
        ClientCache clientCache = ClientCacheFactory.getAnyInstance();
        Region<Integer, String> replicateRegion = clientCache.getRegion(REP_REG_NAME);
        Region<Integer, String> partitionRegion = clientCache.getRegion(PR_REG_NAME);
        waitForCreates(replicateRegion);
        waitForCreates(partitionRegion);
      }

      private void waitForCreates(Region region) {
        CacheListener[] listeners = region.getAttributes().getCacheListeners();
        boolean listenerFound = false;
        for (CacheListener listener : listeners) {
          if (listener instanceof InitialCreatesListener) {
            listenerFound = true;
            final InitialCreatesListener initialCreatesListener = (InitialCreatesListener) listener;
            WaitCriterion wc = new WaitCriterion() {
              @Override
              public boolean done() {
                return initialCreatesListener.numCreates.get() == MAX_ENTRIES;
              }

              @Override
              public String description() {
                return "Client expected to get " + MAX_ENTRIES + " creates, but got "
                    + initialCreatesListener.numCreates.get();
              }
            };
            GeodeAwaitility.await().untilAsserted(wc);
          }
        }
        if (!listenerFound) {
          fail("Client listener should have been found");
        }
      }
    };
    client1.invoke(waitForInitialCreates);
    client2.invoke(waitForInitialCreates);

    SerializableCallable addListener = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache cache = CacheFactory.getAnyInstance();
        Region redundantRegion = cache.getRegion(REP_REG_NAME);
        redundantRegion.getAttributesMutator().addCacheListener(new NotInvokedListener());
        Region partitionRegion = cache.getRegion(PR_REG_NAME);
        partitionRegion.getAttributesMutator().addCacheListener(new NotInvokedListener());
        return null;
      }
    };
    vm1.invoke(addListener);
    vm2.invoke(addListener);
    SerializableRunnable addListenerToClient = new SerializableRunnable() {
      @Override
      public void run() {
        ClientCache clientCache = ClientCacheFactory.getAnyInstance();
        Region replicateRegion = clientCache.getRegion(REP_REG_NAME);
        replicateRegion.getAttributesMutator().addCacheListener(new NotInvokedListener());
        Region partitionRegion = clientCache.getRegion(PR_REG_NAME);
        partitionRegion.getAttributesMutator().addCacheListener(new NotInvokedListener());
      }
    };
    client1.invoke(addListenerToClient);
    client2.invoke(addListenerToClient);

    vm1.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region<Integer, String> redundantRegion = cache.getRegion(REP_REG_NAME);
      Region<Integer, String> partitionedRegion = cache.getRegion(PR_REG_NAME);
      for (int i = 0; i < MAX_ENTRIES; i++) {
        assertEquals("value" + i, redundantRegion.putIfAbsent(i, "piavalue"));
        assertEquals("value" + i, partitionedRegion.putIfAbsent(i, "piavalue"));
      }
      for (int i = 0; i < MAX_ENTRIES; i++) {
        assertFalse(redundantRegion.replace(i, "value", "replace1Value"));
        assertFalse(partitionedRegion.replace(i, "value", "replace1Value"));
      }
      for (int i = MAX_ENTRIES + 1; i < MAX_ENTRIES * 2; i++) {
        assertNull(redundantRegion.replace(i, "replace2value" + i));
        assertNull(partitionedRegion.replace(i, "replace2value" + i));
      }
      for (int i = MAX_ENTRIES + 1; i < MAX_ENTRIES * 2; i++) {
        assertFalse(redundantRegion.remove(i, "removeValue" + i));
        assertFalse(partitionedRegion.remove(i, "removeValue" + i));
      }
    });
  }

  @Test
  public void testBug42162() throws Exception {
    dotestConcOps(false);
  }

  @Test
  public void testBug42162EmptyClient() throws Exception {
    dotestConcOps(true);
  }

  private void dotestConcOps(final boolean emptyClient) throws Exception {
    MemberVM serverVM = startServerAndCreateRegions(1, locatorPort);

    ClientVM clientVM = createClientRegion(2, emptyClient, serverVM.getPort());
    clientVM.invoke(() -> {
      ClientCache clientCache = ClientCacheFactory.getAnyInstance();
      Region<String, String> redundantRegion = clientCache.getRegion(REP_REG_NAME);
      Region<String, String> partitionedRegion = clientCache.getRegion(PR_REG_NAME);
      redundantRegion.registerInterestRegex(".*");
      partitionedRegion.registerInterestRegex(".*");
    });

    serverVM.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region<String, String> redundantRegion = cache.getRegion(REP_REG_NAME);
      Region<String, String> partitionedRegion = cache.getRegion(PR_REG_NAME);
      redundantRegion.put("key0", "value");
      partitionedRegion.put("key0", "value");
      assertNull(redundantRegion.putIfAbsent("keyForNull", null));
      assertNull(partitionedRegion.putIfAbsent("keyForNull", null));
      assertEquals("value", redundantRegion.putIfAbsent("key0", null));
      assertEquals("value", partitionedRegion.putIfAbsent("key0", null));
      assertTrue(redundantRegion.containsKey("keyForNull"));
      assertTrue(partitionedRegion.containsKey("keyForNull"));
      assertFalse(redundantRegion.containsValueForKey("keyForNull"));
      assertFalse(partitionedRegion.containsValueForKey("keyForNull"));
      redundantRegion.put("key0", "value");
      partitionedRegion.put("key0", "value");
    });
    clientVM.invoke(() -> {
      ClientCache clientCache = ClientCacheFactory.getAnyInstance();
      final Region replicateRegion = clientCache.getRegion(REP_REG_NAME);
      final Region partitionRegion = clientCache.getRegion(PR_REG_NAME);
      WaitCriterion wc = new WaitCriterion() {
        AssertionError e = null;

        @Override
        public boolean done() {
          try {
            if (!emptyClient) {
              assertTrue(replicateRegion.containsKey("key0"));
              assertTrue(partitionRegion.containsKey("key0"));
              assertTrue(replicateRegion.containsKey("keyForNull"));
              assertTrue(partitionRegion.containsKey("keyForNull"));
              assertFalse(replicateRegion.containsValueForKey("keyForNull"));
              assertFalse(partitionRegion.containsValueForKey("keyForNull"));
            }
            assertEquals("value", replicateRegion.putIfAbsent("key0", null));
            assertEquals("value", partitionRegion.putIfAbsent("key0", null));
            assertNull(replicateRegion.putIfAbsent("keyForNull", null));
            assertNull(partitionRegion.putIfAbsent("keyForNull", null));
            assertNull(replicateRegion.putIfAbsent("clientNullKey", null));
            assertNull(partitionRegion.putIfAbsent("clientNullKey", null));
          } catch (AssertionError ex) {
            replicateRegion.getCache().getLogger().fine("SWAP:caught ", ex);
            e = ex;
            return false;
          }
          return true;
        }

        @Override
        public String description() {
          return "timeout " + e;
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);
    });

    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      Region<String, String> redundantRegion = cache.getRegion(REP_REG_NAME);
      Region<String, String> partitionedRegion = cache.getRegion(PR_REG_NAME);
      assertTrue(redundantRegion.containsKey("clientNullKey"));
      assertTrue(partitionedRegion.containsKey("clientNullKey"));
      assertFalse(redundantRegion.containsValueForKey("clientNullKey"));
      assertFalse(partitionedRegion.containsValueForKey("clientNullKey"));
      assertNotNull(redundantRegion.replace("key0", "value2"));
      assertNotNull(partitionedRegion.replace("key0", "value2"));
      assertTrue(redundantRegion.replace("keyForNull", null, "newValue"));
      assertTrue(partitionedRegion.replace("keyForNull", null, "newValue"));
    });

    clientVM.invoke(() -> {
      ClientCache clientCache = getClientCache();
      final Region replicateRegion = clientCache.getRegion(REP_REG_NAME);
      final Region partitionRegion = clientCache.getRegion(PR_REG_NAME);
      WaitCriterion wc = new WaitCriterion() {
        AssertionError e = null;

        @Override
        public boolean done() {
          try {
            assertEquals("value2", replicateRegion.putIfAbsent("key0", null));
            assertEquals("value2", partitionRegion.putIfAbsent("key0", null));
            assertEquals("newValue", replicateRegion.putIfAbsent("keyForNull", null));
            assertEquals("newValue", partitionRegion.putIfAbsent("keyForNull", null));
            // replace from client
            assertEquals("value2", replicateRegion.replace("key0", "value"));
            assertEquals("value2", partitionRegion.replace("key0", "value"));
            assertNull(replicateRegion.replace("NoKeyOnServer", "value"));
            assertNull(replicateRegion.replace("NoKeyOnServer", "value"));
            assertTrue(replicateRegion.replace("clientNullKey", null, "newValue"));
            assertTrue(partitionRegion.replace("clientNullKey", null, "newValue"));
          } catch (AssertionError ex) {
            e = ex;
            return false;
          }
          return true;
        }

        @Override
        public String description() {
          return "timeout " + e.getMessage();
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);
    });

    serverVM.invoke(() -> {

      Cache cache = getServerCache();
      Region<String, String> redundantRegion = cache.getRegion(REP_REG_NAME);
      Region<String, String> partitionedRegion = cache.getRegion(PR_REG_NAME);

      assertEquals("newValue", redundantRegion.get("clientNullKey"));
      assertEquals("newValue", partitionedRegion.get("clientNullKey"));
    });

  }

  private ClientCache getClientCache() {
    return ClientCacheFactory.getAnyInstance();
  }

  @Test
  public void testNullValueFromNonEmptyClients() throws Exception {
    MemberVM serverVM = startServerAndCreateRegions(1, locatorPort);

    ClientVM clientVM = createClientRegion(2, true, serverVM.getPort());

    serverVM.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region redundantRegion = cache.getRegion(REP_REG_NAME);
      Region partitionedRegion = cache.getRegion(PR_REG_NAME);
      redundantRegion.create("createKey", null);
      partitionedRegion.create("createKey", null);
      assertNull(redundantRegion.putIfAbsent("putAbsentKey", null));
      assertNull(partitionedRegion.putIfAbsent("putAbsentKey", null));
    });

    clientVM.invoke(() -> {
      ClientCache clientCache = getClientCache();
      final Region replicateRegion = clientCache.getRegion(REP_REG_NAME);
      final Region partitionRegion = clientCache.getRegion(PR_REG_NAME);
      assertEquals(replicateRegion.get("createKey"), replicateRegion.get("putAbsentKey"));
      assertEquals(partitionRegion.get("createKey"), partitionRegion.get("putAbsentKey"));
      assertFalse(replicateRegion.containsKey("createKey"));
      assertFalse(partitionRegion.containsKey("createKey"));
      assertEquals(replicateRegion.containsKey("createKey"),
          replicateRegion.containsKey("putAbsentKey"));
      assertEquals(partitionRegion.containsKey("createKey"),
          partitionRegion.containsKey("putAbsentKey"));
    });
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    doPutIfAbsentWork(false);
  }

  @Test
  public void testPutIfAbsentClientServer() throws Exception {
    doPutIfAbsentWork(true);
  }

  private void doPutIfAbsentWork(final boolean testWithClientServer) throws Exception {
    MemberVM serverVM;
    VM vm2;
    if (testWithClientServer) {
      serverVM = startServerAndCreateRegions(1, locatorPort);
      ClientVM clientVM = createClientRegion(2, false, serverVM.getPort());
      vm2 = clientVM.getVM();
    } else {
      serverVM = clusterStartupRule.startServerVM(1, locatorPort);
      MemberVM serverVM2 = clusterStartupRule.startServerVM(2, locatorPort);
      vm2 = serverVM2.getVM();
      createRegions(serverVM);
      createRegions(serverVM2);
    }
    serverVM.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region redundantRegion = cache.getRegion(REP_REG_NAME);
      Region partitionedRegion = cache.getRegion(PR_REG_NAME);
      assertNull(redundantRegion.putIfAbsent("key0", "value"));
      assertNull(partitionedRegion.putIfAbsent("key0", "value"));
      assertNull(redundantRegion.putIfAbsent("keyForClient", "value"));
      assertNull(partitionedRegion.putIfAbsent("keyForClient", "value"));
      assertEquals("value", redundantRegion.putIfAbsent("key0", "value2"));
      assertEquals("value", partitionedRegion.putIfAbsent("key0", "value2"));
    });

    vm2.invoke(() -> {
      GemFireCache cache = testWithClientServer ? getClientCache() : getServerCache();
      final Region replicateRegion = cache.getRegion(REP_REG_NAME);
      final Region partitionRegion = cache.getRegion(PR_REG_NAME);
      assertEquals("value", replicateRegion.putIfAbsent("key0", "value2"));
      assertEquals("value", partitionRegion.putIfAbsent("key0", "value2"));
      if (testWithClientServer) {
        replicateRegion.get("key0");
        partitionRegion.get("key0");
      }
      assertTrue(replicateRegion.containsKey("key0"));
      assertTrue(partitionRegion.containsKey("key0"));
      assertTrue(replicateRegion.containsValueForKey("key0"));
      assertTrue(partitionRegion.containsValueForKey("key0"));
    });
  }

  @Test
  public void testRemove() throws Exception {
    doRemoveWork(false);
  }

  @Test
  public void testRemoveClientServer() throws Exception {
    doRemoveWork(true);
  }

  private void doRemoveWork(final boolean testWithClientServer) throws Exception {
    Host host = Host.getHost(0);
    MemberVM serverVM;
    VM vm2;

    if (testWithClientServer) {
      serverVM = startServerAndCreateRegions(1, locatorPort);
      ClientVM clientVM = createClientRegion(2, true, serverVM.getPort());
      vm2 = clientVM.getVM();
    } else {
      serverVM = clusterStartupRule.startServerVM(1, locatorPort);
      MemberVM serverVM2 = clusterStartupRule.startServerVM(2, locatorPort);
      vm2 = serverVM2.getVM();
      createRegions(serverVM);
      createRegions(serverVM2);
    }
    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      Region replicantRegion = cache.getRegion(REP_REG_NAME);
      Region partitionRegion = cache.getRegion(PR_REG_NAME);
      assertNull(replicantRegion.putIfAbsent("key0", "value"));
      assertNull(partitionRegion.putIfAbsent("key0", "value"));
      assertNull(replicantRegion.putIfAbsent("keyForClient", "value"));
      assertNull(partitionRegion.putIfAbsent("keyForClient", "value"));
      assertFalse(replicantRegion.remove("nonExistentkey", "value"));
      assertFalse(partitionRegion.remove("nonExistentkey", "value"));
      assertFalse(replicantRegion.remove("key0", "newValue"));
      assertFalse(partitionRegion.remove("key0", "newValue"));
      assertTrue(replicantRegion.remove("key0", "value"));
      assertTrue(partitionRegion.remove("key0", "value"));
      assertFalse(replicantRegion.containsKey("key0"));
      assertFalse(partitionRegion.containsKey("key0"));
    });

    vm2.invoke(() -> {
      GemFireCache cache = testWithClientServer ? getClientCache() : getServerCache();
      final Region replicateRegion = cache.getRegion(REP_REG_NAME);
      final Region partitionRegion = cache.getRegion(PR_REG_NAME);
      assertFalse(replicateRegion.remove("nonExistentkey", "value"));
      assertFalse(partitionRegion.remove("nonExistentkey", "value"));
      assertFalse(replicateRegion.remove("keyForClient", "newValue"));
      assertFalse(partitionRegion.remove("keyForClient", "newValue"));
      assertTrue(replicateRegion.remove("keyForClient", "value"));
      assertTrue(partitionRegion.remove("keyForClient", "value"));
    });

    serverVM.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region replicateRegion = cache.getRegion(REP_REG_NAME);
      Region partitionedRegion = cache.getRegion(PR_REG_NAME);
      assertFalse(replicateRegion.containsKey("keyForClient"));
      assertFalse(partitionedRegion.containsKey("keyForClient"));
    });
  }

  @Test
  public void testReplaceUsingClientServer() throws Exception {
    doReplaceWork(true);
  }

  @Test
  public void testReplace() throws Exception {
    doReplaceWork(false);
  }

  private void doReplaceWork(final boolean testAsClientServer) throws Exception {
    MemberVM serverVM;
    VM vm2;
    if (testAsClientServer) {
      serverVM = startServerAndCreateRegions(1, locatorPort);
      ClientVM clientVM = createClientRegion(2, true, serverVM.getPort());
      vm2 = clientVM.getVM();
    } else {
      serverVM = clusterStartupRule.startServerVM(1, locatorPort);
      MemberVM serverVM2 = clusterStartupRule.startServerVM(2, locatorPort);
      vm2 = serverVM2.getVM();
      createRegions(serverVM);
      createRegions(serverVM2);
    }
    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      Region redundantRegion = cache.getRegion(REP_REG_NAME);
      Region partitionedRegion = cache.getRegion(PR_REG_NAME);
      assertNull(redundantRegion.putIfAbsent("key0", "value"));
      assertNull(partitionedRegion.putIfAbsent("key0", "value"));
      assertNull(redundantRegion.putIfAbsent("keyForClient", "value"));
      assertNull(partitionedRegion.putIfAbsent("keyForClient", "value"));
      assertNull(redundantRegion.replace("nonExistentkey", "value"));
      assertNull(partitionedRegion.replace("nonExistentkey", "value"));
      assertEquals("value", redundantRegion.replace("key0", "value2"));
      assertEquals("value", partitionedRegion.replace("key0", "value2"));
      assertFalse(redundantRegion.replace("key0", "value", "newValue"));
      assertFalse(partitionedRegion.replace("key0", "value", "newValue"));
      assertTrue(redundantRegion.replace("key0", "value2", "newValue"));
      assertTrue(partitionedRegion.replace("key0", "value2", "newValue"));
    });

    vm2.invoke(() -> {
      GemFireCache cache = testAsClientServer ? getClientCache() : getServerCache();
      final Region replicateRegion = cache.getRegion(REP_REG_NAME);
      final Region partitionRegion = cache.getRegion(PR_REG_NAME);
      assertNull(replicateRegion.replace("nonExistentkey", "value"));
      assertNull(partitionRegion.replace("nonExistentkey", "value"));
      assertEquals("value", replicateRegion.replace("keyForClient", "value2"));
      assertEquals("value", partitionRegion.replace("keyForClient", "value2"));
      assertFalse(replicateRegion.replace("keyForClient", "value", "newValue"));
      assertFalse(partitionRegion.replace("keyForClient", "value", "newValue"));
      assertTrue(replicateRegion.replace("keyForClient", "value2", "newValue"));
      assertTrue(partitionRegion.replace("keyForClient", "value2", "newValue"));
    });

    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      Region redundantRegion = cache.getRegion(REP_REG_NAME);
      Region partitionedRegion = cache.getRegion(PR_REG_NAME);
      assertFalse(redundantRegion.containsKey("nonExistentkey"));
      assertFalse(partitionedRegion.containsKey("nonExistentkey"));
    });
  }

  @Test
  public void testBug42167() throws Exception {
    do42167Work(false, REP_REG_NAME);
  }

  @Test
  public void testBug42167PartitionRegion() throws Exception {
    do42167Work(false, PR_REG_NAME);
  }

  @Test
  public void testBug42167Empty() throws Exception {
    do42167Work(true, REP_REG_NAME);
  }

  @Test
  public void testBug42167EmptyPartitionRegion() throws Exception {
    do42167Work(true, PR_REG_NAME);
  }

  private void do42167Work(final boolean emptyClient, final String regionName) throws Exception {
    MemberVM serverVM = startServerAndCreateRegions(1, locatorPort);

    ClientVM clientVM = createClientRegion(2, emptyClient, serverVM.getPort());
    serverVM.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      Region region = cache.getRegion(regionName);
      region.put("key0", "value");
      region.put("key2", "value2");
    });

    clientVM.invoke(() -> {
      ClientCache clientCache = getClientCache();
      Region replicateRegion = clientCache.getRegion(regionName);
      assertEquals("value", replicateRegion.get("key0"));
      if (!emptyClient) {
        replicateRegion.localDestroy("key0");
        assertFalse(replicateRegion.containsKey("key0"));
      }
      clientCache.getLogger().fine("SWAP:doingRemove");
      assertTrue(replicateRegion.remove("key0", "value"));

      DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
      assertTrue(replicateRegion.remove("key0") == null);
      assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

      DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
      assertFalse(replicateRegion.remove("key0", "value"));
      assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

      DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
      assertTrue(replicateRegion.destroy("key0") == null);
      assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

      DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
      assertTrue(replicateRegion.remove("nonExistentKey1") == null);
      assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

      DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
      assertFalse(replicateRegion.remove("nonExistentKey2", "value"));
      assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

      DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND = false;
      assertTrue(replicateRegion.destroy("nonExistentKey3") == null);
      assertTrue(DestroyOp.TEST_HOOK_ENTRY_NOT_FOUND);

      clientCache.getLogger().fine("SWAP:doingReplace");
      assertEquals("value2", replicateRegion.replace("key2", "newValue2"));
      clientCache.getLogger().fine("SWAP:doingReplace2");
      assertEquals(null, replicateRegion.replace("key0", "newValue"));
      assertNull(replicateRegion.putIfAbsent("key4", "value4"));
    });

    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      Region region = cache.getRegion(regionName);
      assertFalse(region.containsKey("key0"));
      assertFalse(region.containsValueForKey("key0"));
      assertTrue(region.containsKey("key2"));
      assertEquals("newValue2", region.get("key2"));
      region.getCache().getLogger().fine("SWAP:doingGet");
      assertEquals("value4", region.get("key4"));
    });
  }

  @Test
  public void testBug42189() throws Exception {
    doBug42189Work(REP_REG_NAME);
  }

  @Test
  public void testBug42189PartitionRegion() throws Exception {
    doBug42189Work(PR_REG_NAME);
  }

  private void doBug42189Work(final String regionName) throws Exception {
    Host host = Host.getHost(0);
    MemberVM serverVM = startServerAndCreateRegions(1, locatorPort);

    ClientVM clientVM = createClientRegion(2, false, serverVM.getPort());
    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      final Region region = cache.getRegion(regionName);
      region.create("key0", null);
      assertNull(region.putIfAbsent("key0", "value"));
      assertTrue(region.containsKey("key0"));
      Object v = region.get("key0");
      assertNull("expected null but was " + v, v);
    });

    clientVM.invoke(() -> {
      final Region replicateRegion = getClientCache().getRegion(regionName);
      assertNull(replicateRegion.putIfAbsent("key0", "value"));
      assertTrue(replicateRegion.containsKeyOnServer("key0"));
      Object v = replicateRegion.get("key0");
      assertNull("expected null but was " + v, v);
    });

    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      final Region region = cache.getRegion(regionName);
      assertTrue(region.containsKey("key0"));
      assertFalse(region.containsValueForKey("key0"));
    });
  }

  /**
   * Replicate Region test for bug #42195: putIfAbsent from client does not put old value in local
   * cache
   */
  @Ignore("TODO")
  @Test
  public void testBug42195() throws Exception {
    doPutIfAbsentPutsKeyInLocalClientCacheWork(REP_REG_NAME);
  }

  /**
   * Partitioned Region test for bug #42195: putIfAbsent from client does not put old value in local
   * cache
   */
  @Ignore("TODO")
  @Test
  public void testBug42195PartitionRegion() throws Exception {
    doPutIfAbsentPutsKeyInLocalClientCacheWork(PR_REG_NAME);
  }

  private void doPutIfAbsentPutsKeyInLocalClientCacheWork(final String regionName)
      throws Exception {
    MemberVM serverVM = startServerAndCreateRegions(1, locatorPort);

    ClientVM clientVM = createClientRegion(2, false, serverVM.getPort());
    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      final Region region = cache.getRegion(regionName);
      assertNull(region.putIfAbsent("key0", "value"));
      assertTrue(region.containsKey("key0"));
    });

    clientVM.invoke(() -> {
      final Region replicateRegion = getClientCache().getRegion(regionName);
      assertEquals("value", replicateRegion.putIfAbsent("key0", "newValue"));
      assertTrue(replicateRegion.containsKeyOnServer("key0"));
      assertTrue(replicateRegion.containsKey("key0"));
      assertTrue(replicateRegion.containsValueForKey("key0"));
    });
  }

  @Test
  public void testReplacePutsKeyInLocalClientCache() throws Exception {
    doReplacePutsKeyInLocalClientCacheWork(REP_REG_NAME);
  }

  @Test
  public void testReplacePutsKeyInLocalClientCacheWithPartitionRegion() throws Exception {
    doReplacePutsKeyInLocalClientCacheWork(PR_REG_NAME);
  }

  private void doReplacePutsKeyInLocalClientCacheWork(final String regionName) throws Exception {
    MemberVM serverVM = startServerAndCreateRegions(1, locatorPort);

    ClientVM clientVM = createClientRegion(2, false, serverVM.getPort());
    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      final Region region = cache.getRegion(regionName);
      assertNull(region.putIfAbsent("key0", "value"));
      assertTrue(region.containsKey("key0"));
      assertNull(region.putIfAbsent("key2", "value2"));
      assertTrue(region.containsKey("key2"));
    });

    clientVM.invoke(() -> {
      final Region region = getClientCache().getRegion(regionName);
      assertEquals("value", region.replace("key0", "newValue"));
      assertTrue(region.containsKeyOnServer("key0"));
      assertTrue(region.containsKey("key0"));
      assertTrue(region.containsValueForKey("key0"));

      assertFalse(region.replace("key2", "DontReplace", "newValue"));
      assertTrue(region.replace("key2", "value2", "newValu2"));
      assertTrue(region.containsKeyOnServer("key2"));
      assertTrue(region.containsKey("key2"));
      assertTrue(region.containsValueForKey("key2"));
    });
    // bug #42221 - replace does not put entry on client when server has invalid value
    clientVM.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      final String key = "bug42221";
      region.putIfAbsent(key, null);
      assertTrue(region.containsKey(key));
      Object result = region.replace(key, "not null");
      assertEquals(null, result);
      assertTrue(region.containsKey(key));
      assertEquals(region.get(key), "not null");
      region.remove(key); // cleanup
    });

    // bug #42242 - remove(K,null) doesn't work
    clientVM.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      final String key = "bug42242";
      region.putIfAbsent(key, null);
      assertTrue(region.containsKey(key));
      assertTrue(region.containsKeyOnServer(key));
      boolean result = region.remove(key, null);
      assertTrue(result);
      assertFalse(region.containsKey(key));
      assertFalse(region.containsKeyOnServer(key));
    });

    // bug #42242b - second scenario with a replace(K,V,V) that didn't work
    clientVM.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      final String key = "bug42242b";
      region.putIfAbsent(key, null);
      assertTrue(region.containsKey(key));
      assertTrue(region.containsKeyOnServer(key));
      boolean result = region.replace(key, null, "new value");
      assertTrue(result);
      result = region.remove(key, "new value");
      assertTrue(result);
      assertFalse(region.containsKey(key));
      assertFalse(region.containsKeyOnServer(key));
    });

    // bug #42242c - remove does not work for entry that's on the server but not on the client
    final String key = "bug42242c";
    clientVM.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    serverVM.invoke(() -> {
      Cache cache = getServerCache();
      Region region = cache.getRegion(regionName);
      region.putIfAbsent(key, null);
      assertTrue(region.containsKey(key));
    });

    clientVM.invoke(() -> {
      final Region region = getClientCache().getRegion(regionName);
      WaitCriterion w = new WaitCriterion() {
        @Override
        public String description() {
          return "waiting for server operation to reach client";
        }

        @Override
        public boolean done() {
          return region.containsKey(key);
        }
      };
      GeodeAwaitility.await().untilAsserted(w);
      assertTrue(region.containsKeyOnServer(key));
      boolean result = region.remove(key, null);

      assertTrue(result);
      assertFalse(region.containsKey(key));
      assertFalse(region.containsKeyOnServer(key));
    });
  }

  @Test
  public void testWithDelta() throws Exception {
    doTestWithDeltaWork(false, REP_REG_NAME);
  }

  @Test
  public void testWithDeltaPartitionRegion() throws Exception {
    doTestWithDeltaWork(false, PR_REG_NAME);
  }

  @Test
  public void testWithDeltaClientServer() throws Exception {
    doTestWithDeltaWork(true, REP_REG_NAME);
  }

  @Test
  public void testWithDeltaPartitionRegionClientServer() throws Exception {
    doTestWithDeltaWork(true, PR_REG_NAME);
  }

  private void doTestWithDeltaWork(final boolean clientServer, final String regName)
      throws Exception {
    MemberVM serverVM;
    VM vm2;

    if (clientServer) {
      serverVM = startServerAndCreateRegions(1, locatorPort);
      vm2 = createClientRegion(2, false, serverVM.getPort()).getVM();
    } else {
      serverVM = clusterStartupRule.startServerVM(1, locatorPort);
      MemberVM serverVM2 = clusterStartupRule.startServerVM(2, locatorPort);
      vm2 = serverVM2.getVM();
      createRegions(serverVM);
      createRegions(serverVM2);
    }

    vm2.invoke(() -> {
      GemFireCache cache =
          clientServer ? ClientCacheFactory.getAnyInstance() : CacheFactory.getAnyInstance();
      Region region = cache.getRegion(regName);
      CustomerDelta customerDelta = new CustomerDelta("cust1", "addr1");
      assertNull(region.putIfAbsent("k1", customerDelta));
      CustomerDelta newCustomer = new CustomerDelta(customerDelta);
      newCustomer.setAddress("updatedAddress");
      assertEquals(customerDelta, region.putIfAbsent("k1", customerDelta));
      assertEquals(customerDelta, region.replace("k1", newCustomer));
      assertFalse(region.replace("k1", customerDelta, newCustomer));
      assertTrue(region.replace("k1", newCustomer, customerDelta));
      assertFalse(region.remove("k1", newCustomer));
      assertTrue(region.remove("k1", customerDelta));
    });
  }

  /** test putIfAbsent with failover & retry. This is bugs 42559 and 43640 */
  @Test
  public void testRetriedPutIfAbsent() throws Exception {
    doRetriedOperation(Operation.PUT_IF_ABSENT, false, locatorPort);
  }

  @Test
  public void testRetriedReplace() throws Exception {
    doRetriedOperation(Operation.REPLACE, false, locatorPort);
  }

  @Test
  public void testRetriedRemove() throws Exception {
    doRetriedOperation(Operation.REMOVE, false, locatorPort);
  }

  @Test
  public void testRetriedPutIfAbsentPartitionRegion() throws Exception {
    doRetriedOperation(Operation.PUT_IF_ABSENT, false, locatorPort);
  }

  @Test
  public void testRetriedReplacePartitionRegion() throws Exception {
    doRetriedOperation(Operation.REPLACE, false, locatorPort);
  }

  @Test
  public void testRetriedRemovePartitionRegion() throws Exception {
    doRetriedOperation(Operation.REMOVE, false, locatorPort);
  }

  private void doRetriedOperation(final Operation operation, boolean testWithPartitionRegion,
      final int locatorPort)
      throws Exception {
    MemberVM serverVM1 = startServerAndCreateRegions(1, true, locatorPort);
    MemberVM serverVM2 = startServerAndCreateRegions(2, true, locatorPort);
    final String regionName = testWithPartitionRegion ? PR_REG_NAME : REP_REG_NAME;

    IgnoredException.addIgnoredException("java.net.SocketException");

    ClientVM clientVM = createClientRegion(3, false, serverVM1.getPort(), serverVM2.getPort());

    final DistributedMember server1ID =
        serverVM1.getVM().invoke("get DM ID",
            () -> getServerCache().getDistributedSystem().getDistributedMember());
    final DistributedMember server2ID =
        serverVM2.getVM().invoke("get DM ID",
            () -> getServerCache().getDistributedSystem().getDistributedMember());

    Set<IgnoredException> exceptions = new HashSet<>();
    exceptions.add(
        IgnoredException.addIgnoredException("Membership: requesting removal", serverVM1.getVM()));
    exceptions.add(
        IgnoredException.addIgnoredException("Membership: requesting removal", serverVM2.getVM()));
    exceptions.add(IgnoredException
        .addIgnoredException(ForcedDisconnectException.class.getSimpleName(), serverVM1.getVM()));
    exceptions.add(IgnoredException
        .addIgnoredException(ForcedDisconnectException.class.getSimpleName(), serverVM2.getVM()));
    exceptions.add(IgnoredException
        .addIgnoredException(MemberDisconnectedException.class.getSimpleName(), serverVM1.getVM()));
    exceptions.add(IgnoredException
        .addIgnoredException(MemberDisconnectedException.class.getSimpleName(), serverVM2.getVM()));


    try {
      serverVM1.invoke("install crasher in server1", () -> {
        Cache cache = getServerCache();
        Region region = cache.getRegion(regionName);
        region.put("key0", "value");
        if (operation == Operation.PUT_IF_ABSENT) {
          region.destroy("key0");
        }
        // force client to use server1 for now
        // getCache().getCacheServers().get(0).stop();
        region.getAttributesMutator()
            .addCacheListener(new CustomerDelta.KillServerAdapter(cache, server2ID));
      });

      serverVM2.invoke("install crasher in server2", () -> {
        Cache cache = getServerCache();
        Region region = cache.getRegion(regionName);
        // force client to use server1 for now
        // getCache().getCacheServers().get(0).stop();
        region.getAttributesMutator()
            .addCacheListener(new CustomerDelta.KillServerAdapter(cache, server1ID));
      });

      clientVM.invoke(() -> {

        ClientCache clientCache = getClientCache();
        Region region = clientCache.getRegion(regionName);
        if (operation == Operation.PUT_IF_ABSENT) {

          assertTrue("expected putIfAbsent to succeed and return null",
              region.putIfAbsent("key0", "newvalue") == null);
        } else if (operation == Operation.REMOVE) {

          assertTrue("expected remove operation to succeed and return true",
              region.remove("key0", "value"));
        } else if (operation == Operation.REPLACE) {

          assertTrue("expected replace operation to succeed and return true",
              region.replace("key0", "value", "newvalue"));
        }
      });
    } finally {
      Disconnect.disconnectAllFromDS();
      for (IgnoredException ex : exceptions) {
        ex.remove();
      }
    }
  }

  private static class CustomerDelta implements DataSerializable, Delta {
    private String name;
    private String address;
    private boolean nameChanged;
    private boolean addressChanged;

    public CustomerDelta() {}

    public CustomerDelta(CustomerDelta o) {
      address = o.address;
      name = o.name;
    }

    public CustomerDelta(String name, String address) {
      this.name = name;
      this.address = address;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeUTF(name);
      out.writeUTF(address);
      out.writeBoolean(nameChanged);
      out.writeBoolean(addressChanged);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      name = in.readUTF();
      address = in.readUTF();
      nameChanged = in.readBoolean();
      addressChanged = in.readBoolean();
    }

    @Override
    public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
      boolean nameC = in.readBoolean();
      if (nameC) {
        name = in.readUTF();
      }
      boolean addressC = in.readBoolean();
      if (addressC) {
        address = in.readUTF();
      }
    }

    @Override
    public boolean hasDelta() {
      return nameChanged || addressChanged;
    }

    @Override
    public void toDelta(DataOutput out) throws IOException {
      if (nameChanged) {
        out.writeBoolean(nameChanged);
        out.writeUTF(name);
      }
      if (addressChanged) {
        out.writeBoolean(addressChanged);
        out.writeUTF(address);
      }
    }

    public void setName(String name) {
      nameChanged = true;
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setAddress(String address) {
      addressChanged = true;
      this.address = address;
    }

    public String getAddress() {
      return address;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof CustomerDelta)) {
        return false;
      }
      CustomerDelta other = (CustomerDelta) obj;
      return name.equals(other.name) && address.equals(other.address);
    }

    @Override
    public int hashCode() {
      return address.hashCode() + name.hashCode();
    }

    private static class KillServerAdapter extends CacheListenerAdapter {
      private Cache cache;
      private DistributedMember distributedMember;

      public KillServerAdapter(Cache cache, DistributedMember distributedMember) {
        this.cache = cache;
        this.distributedMember = distributedMember;
      }

      private void killSender(EntryEvent event) {
        if (event.isOriginRemote()) {
          Distribution distribution =
              MembershipManagerHelper.getDistribution(cache.getDistributedSystem());
          boolean requestMemberRemoval =
              distribution.requestMemberRemoval((InternalDistributedMember) distributedMember,
                  "removing for test");
          assertTrue(requestMemberRemoval);
          try {
            distribution.waitForDeparture((InternalDistributedMember) distributedMember);
          } catch (Exception e) {
            fail("failed to stop the other server for this test:" + e.getMessage());
          }
        }
      }

      @Override
      public void afterCreate(EntryEvent event) {
        cache.getLogger().info("afterCreate invoked with " + event);
        killSender(event);
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        cache.getLogger().info("afterUpdate invoked with " + event);
        killSender(event);
      }

      @Override
      public void afterDestroy(EntryEvent event) {
        cache.getLogger().info("afterDestroy invoked with " + event);
        killSender(event);
      }
    }
  }
}
