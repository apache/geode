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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import org.awaitility.Awaitility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.DistributedLockStats;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.DistributedTombstoneOperation.TombstoneMessage;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.partitioned.PRTombstoneMessage;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * concurrency-control tests for client/server
 * 
 *
 */
@Category({DistributedTest.class, ClientServerTest.class})
public class ClientServerCCEDUnitTest extends JUnit4CacheTestCase {
  public static LocalRegion TestRegion;

  @Before
  public void setup() {
    // for bug #50683 we need a short queue-removal-message processing interval
    HARegionQueue.setMessageSyncInterval(5);
    IgnoredException.addIgnoredException("java.net.ConnectException");
  }

  @Override
  public final void preTearDownCacheTestCase() {
    disconnectAllFromDS();
    HARegionQueue.setMessageSyncInterval(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
    TestRegion = null;
  }


  /**
   * GEODE-3519 servers are not locking on remove or invalidate ops initiated by clients
   * <p>
   * This test sets up two servers each with a client attached. The clients perform operations on
   * the same key in a region which, in the servers, has Scope.GLOBAL. There should be no conflation
   * and each operation should obtain a lock.
   * 
   * @throws Exception
   */
  @Test
  public void testClientEventsAreNotConflatedByGlobalRegionOnServer() throws Exception {
    VM[] serverVMs = new VM[] {Host.getHost(0).getVM(0), Host.getHost(0).getVM(1)};
    VM[] clientVMs = new VM[] {Host.getHost(0).getVM(2), Host.getHost(0).getVM(3)};
    final String name = this.getUniqueName() + "Region";

    int serverPorts[] = new int[] {createServerRegion(serverVMs[0], name, true, Scope.GLOBAL),
        createServerRegion(serverVMs[1], name, true, Scope.GLOBAL)};

    for (int i = 0; i < clientVMs.length; i++) {
      createClientRegion(clientVMs[i], name, serverPorts[i], false,
          ClientRegionShortcut.CACHING_PROXY, false);
    }

    getBlackboard().initBlackboard();

    final int numIterations = 500;

    AsyncInvocation[] asyncInvocations = new AsyncInvocation[clientVMs.length];
    for (int i = 0; i < clientVMs.length; i++) {
      final String clientGateName = "client" + i + "Ready";
      asyncInvocations[i] = clientVMs[i].invokeAsync("doOps Thread", () -> {
        doOps(name, numIterations, clientGateName);
      });
      getBlackboard().waitForGate(clientGateName, 30, SECONDS);
    }

    getBlackboard().signalGate("proceed");

    for (int i = 0; i < asyncInvocations.length; i++) {
      asyncInvocations[i].join();
    }

    for (int i = 0; i < serverVMs.length; i++) {
      serverVMs[i].invoke("verify thread", () -> {
        verifyServerState(name, numIterations);
      });
    }
  }

  private void verifyServerState(String name, int numIterations) {
    Cache cache = CacheFactory.getAnyInstance();
    DistributedRegion region = (DistributedRegion) cache.getRegion(name);
    CachePerfStats stats = region.getCachePerfStats();
    assertEquals(0, stats.getConflatedEventsCount());

    DLockService dLockService = (DLockService) region.getLockService();
    DistributedLockStats distributedLockStats = dLockService.getStats();
    assertEquals(numIterations, distributedLockStats.getLockReleasesCompleted());
  }

  private void doOps(String name, int numIterations, String clientGateName) {
    ClientCache cache = ClientCacheFactory.getAnyInstance();
    Region region = cache.getRegion(name);
    getBlackboard().signalGate(clientGateName);
    try {
      getBlackboard().waitForGate("proceed", 30, SECONDS);
    } catch (TimeoutException | InterruptedException e) {
      throw new RuntimeException("failed to start", e);
    }
    String key = "lockingKey";
    String value = "lockingValue";
    for (int j = 0; j < numIterations; j++) {
      int operation = j % 5;
      switch (operation) {
        case 0:
          region.remove(key);
          break;
        case 1:
          region.putIfAbsent(key, value);
          break;
        case 2:
          region.invalidate(key);
          break;
        case 3:
          region.replace(key, value);
          break;
        case 4:
          region.replace(key, value, value);
          break;
        // case 5:
        // remove(k,v) can't be included in this test as it checks the old value
        // against what is in the local cache before sending the operation to the server
        // region.remove(key, value);
        // break;
        default:
          throw new RuntimeException("" + j + " % 5 == " + operation + "?");
      }
    }
  }

  @Test
  public void testClientDoesNotExpireEntryPrematurely() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName() + "Region";
    final String key = "testKey";

    int port = createServerRegion(vm0, name, true);

    vm0.invoke(new SerializableCallable("create old entry") {
      public Object call() throws Exception {
        LocalRegion r = (LocalRegion) basicGetCache().getRegion(name);
        r.put(key, "value");
        AbstractRegionEntry entry = (AbstractRegionEntry) r.basicGetEntry(key);
        // set an old timestamp in the entry - thirty minutes ago
        entry.getVersionStamp().setVersionTimeStamp(System.currentTimeMillis() - 1800000L);
        return null;
      }
    });

    createClientRegion(vm1, name, port, true, ClientRegionShortcut.CACHING_PROXY, false);

    vm1.invoke(new SerializableCallable("fetch entry and validate") {
      public Object call() throws Exception {
        final Long[] expirationTimeMillis = new Long[1];
        int expirationSeconds = 15;

        LocalRegion r = (LocalRegion) basicGetCache().getRegion(name);
        AttributesMutator mutator = r.getAttributesMutator();
        mutator.setEntryIdleTimeout(
            new ExpirationAttributes(expirationSeconds, ExpirationAction.LOCAL_DESTROY));
        mutator.addCacheListener(new CacheListenerAdapter() {
          @Override
          public void afterDestroy(EntryEvent event) {
            expirationTimeMillis[0] = System.currentTimeMillis();
          }
        });

        // fetch the entry from the server and make sure it doesn't expire early
        if (!r.containsKey(key)) {
          r.get(key);
        }

        final long expirationTime = System.currentTimeMillis() + (expirationSeconds * 1000);

        Awaitility.await("waiting for object to expire").atMost(expirationSeconds * 2, SECONDS)
            .until(() -> {
              return expirationTimeMillis[0] != null;
            });

        disconnectFromDS();

        assertTrue(
            "entry expired " + (expirationTime - expirationTimeMillis[0]) + " milliseconds early",
            expirationTimeMillis[0] >= expirationTime);

        return null;
      }
    });

    vm0.invoke(new SerializableRunnable() {
      public void run() {
        disconnectFromDS();
      }
    });

  }

  public ClientServerCCEDUnitTest() {
    super();
  }

  @Test
  public void testClientServerRRTombstoneGC() {
    clientServerTombstoneGCTest(getUniqueName(), true);
  }

  @Test
  public void testClientServerPRTombstoneGC() {
    clientServerTombstoneGCTest(getUniqueName(), false);
  }

  @Test
  public void testPutAllInNonCCEClient() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String name = this.getUniqueName() + "Region";

    int port = createServerRegion(vm0, name, true);
    createClientRegion(vm1, name, port, false, ClientRegionShortcut.CACHING_PROXY);
    doPutAllInClient(vm1);
  }


  /**
   * test that distributed GC messages are sent to clients and properly processed
   * 
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
    createClientRegion(vm2, name, port, true, ClientRegionShortcut.CACHING_PROXY);
    createClientRegion(vm3, name, port, true, ClientRegionShortcut.CACHING_PROXY);
    createEntries(vm2);
    destroyEntries(vm3);
    unregisterInterest(vm3);
    forceGC(vm0);
    if (!replicatedRegion) {
      // other bucket might be in vm1
      forceGC(vm1);
    }
    checkClientReceivedGC(vm2);
    checkClientDoesNotReceiveGC(vm3);
  }

  @Test
  public void testTombstoneMessageSentToReplicatesAreNotProcessedInLine() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final String name = "Region";

    createServerRegion(vm0, name, true);
    createEntries(vm0);
    createServerRegion(vm1, name, true);

    try {
      vm1.invoke(() -> {
        DistributionMessageObserver.setInstance(new PRTombstoneMessageObserver());
      });
      destroyEntries(vm0);
      forceGC(vm0);

      vm1.invoke(() -> {
        PRTombstoneMessageObserver mo =
            (PRTombstoneMessageObserver) DistributionMessageObserver.getInstance();
        Awaitility.await().atMost(60, SECONDS).until(() -> {
          return mo.tsMessageProcessed >= 1;
        });
        assertTrue("Tombstone GC message is not expected.", mo.thName.contains(
            LocalizedStrings.DistributionManager_POOLED_MESSAGE_PROCESSOR.toLocalizedString()));
      });

    } finally {
      vm1.invoke(() -> {
        DistributionMessageObserver.setInstance(null);
      });
    }
  }

  @Test
  public void testTombstoneGcMessagesAreOnlySentToPRNodesWithInterestRegistration() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    final String name = "Region";

    createServerRegion(vm0, name, false);
    // Create all the buckets on this vm.
    createEntries(vm0);

    createServerRegion(vm1, name, false);

    int port = createServerRegion(vm2, name, false);

    // Create client and register interest on one server.
    createClientRegion(vm3, name, port, true, ClientRegionShortcut.CACHING_PROXY);

    try {
      vm1.invoke(() -> {
        DistributionMessageObserver.setInstance(new PRTombstoneMessageObserver());
      });
      vm2.invoke(() -> {
        DistributionMessageObserver.setInstance(new PRTombstoneMessageObserver());
      });

      destroyEntries(vm0);
      forceGC(vm0);

      // vm2 should receive tombstone GC messages
      vm2.invoke(() -> {
        PRTombstoneMessageObserver mo =
            (PRTombstoneMessageObserver) DistributionMessageObserver.getInstance();
        // Should receive tombstone message for each bucket.
        Awaitility.await().atMost(60, SECONDS).until(() -> {
          return mo.prTsMessageProcessed >= 2;
        });
        assertEquals("Tombstone GC message is expected.", 2, mo.prTsMessageProcessed);
      });

      // Since there is no interest registered, vm1 should not receive any tombstone GC messages
      vm1.invoke(() -> {
        PRTombstoneMessageObserver mo =
            (PRTombstoneMessageObserver) DistributionMessageObserver.getInstance();
        assertEquals("Tombstone GC message is not expected.", 0, mo.prTsMessageProcessed);
      });
    } finally {
      vm1.invoke(() -> {
        DistributionMessageObserver.setInstance(null);
      });
      vm2.invoke(() -> {
        DistributionMessageObserver.setInstance(null);
      });
    }
  }

  private class PRTombstoneMessageObserver extends DistributionMessageObserver {
    public int tsMessageProcessed = 0;
    public int prTsMessageProcessed = 0;
    public String thName;

    @Override
    public void afterProcessMessage(DistributionManager dm, DistributionMessage message) {
      thName = Thread.currentThread().getName();

      if (message instanceof TombstoneMessage) {
        tsMessageProcessed++;
      }

      if (message instanceof PRTombstoneMessage) {
        prTsMessageProcessed++;
      }
    }
  }

  /**
   * for bug #40791 we pull tombstones into clients on get(), getAll() and registerInterest() to
   * protect the client cache from stray putAll events sitting in backup queues on the server
   */
  @Test
  public void testClientRIGetsTombstonesRR() throws Exception {
    clientRIGetsTombstoneTest(getUniqueName(), true);
  }

  @Test
  public void testClientRIGetsTombstonesPR() throws Exception {
    clientRIGetsTombstoneTest(getUniqueName(), false);
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

    LogWriterUtils.getLogWriter().info("***************** register interest on all keys");
    createClientRegion(vm2, name, port, true, ClientRegionShortcut.CACHING_PROXY);
    registerInterest(vm2);
    ensureAllTombstonesPresent(vm2);

    LogWriterUtils.getLogWriter()
        .info("***************** clear cache and register interest on one key, Object0");
    clearLocalCache(vm2);
    registerInterestOneKey(vm2, "Object0");
    List<String> keys = new ArrayList(1);
    keys.add("Object0");
    ensureAllTombstonesPresent(vm2, keys);

    LogWriterUtils.getLogWriter()
        .info("***************** clear cache and register interest on four keys");
    clearLocalCache(vm2);
    keys = new ArrayList(4);
    for (int i = 0; i < 4; i++) {
      keys.add("Object" + i);
    }
    registerInterest(vm2, keys);
    ensureAllTombstonesPresent(vm2, keys);

    LogWriterUtils.getLogWriter()
        .info("***************** clear cache and register interest with regex on four keys");
    clearLocalCache(vm2);
    registerInterestRegex(vm2, "Object[0-3]");
    ensureAllTombstonesPresent(vm2, keys);

    LogWriterUtils.getLogWriter().info("***************** fetch entries with getAll()");
    clearLocalCache(vm2);
    getAll(vm2);
    ensureAllTombstonesPresent(vm2);
  }

  @Test
  public void testClientRIGetsInvalidEntriesRR() throws Exception {
    clientRIGetsInvalidEntriesTest(getUniqueName(), true);
  }

  @Test
  public void testClientRIGetsInvalidEntriesPR() throws Exception {
    clientRIGetsInvalidEntriesTest(getUniqueName(), false);
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

    LogWriterUtils.getLogWriter().info("***************** register interest on all keys");
    createClientRegion(vm2, name, port, true, ClientRegionShortcut.CACHING_PROXY);
    registerInterest(vm2);
    ensureAllInvalidsPresent(vm2);

    LogWriterUtils.getLogWriter()
        .info("***************** clear cache and register interest on one key, Object0");
    clearLocalCache(vm2);
    registerInterestOneKey(vm2, "Object0");
    List<String> keys = new ArrayList(1);
    keys.add("Object0");
    ensureAllInvalidsPresent(vm2, keys);

    LogWriterUtils.getLogWriter()
        .info("***************** clear cache and register interest on four keys");
    clearLocalCache(vm2);
    keys = new ArrayList(4);
    for (int i = 0; i < 4; i++) {
      keys.add("Object" + i);
    }
    registerInterest(vm2, keys);
    ensureAllInvalidsPresent(vm2, keys);

    LogWriterUtils.getLogWriter()
        .info("***************** clear cache and register interest with regex on four keys");
    clearLocalCache(vm2);
    registerInterestRegex(vm2, "Object[0-3]");
    ensureAllInvalidsPresent(vm2, keys);

    LogWriterUtils.getLogWriter().info("***************** fetch entries with getAll()");
    clearLocalCache(vm2);
    getAll(vm2);
    ensureAllInvalidsPresent(vm2);
  }

  @Test
  public void testClientCacheListenerDoesNotSeeTombstones() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final String name = getUniqueName() + "Region";


    createServerRegion(vm0, name, true);
    int port = createServerRegion(vm1, name, true);
    createEntries(vm0);
    destroyEntries(vm0);


    LogWriterUtils.getLogWriter().info("***************** register interest on all keys");
    createClientRegion(vm2, name, port, true, ClientRegionShortcut.PROXY);
    vm2.invoke(
        () -> TestRegion.getAttributesMutator().addCacheListener(new RecordingCacheListener()));

    getAll(vm2);

    vm2.invoke(() -> {
      RecordingCacheListener listener = (RecordingCacheListener) TestRegion.getCacheListener();
      assertEquals(Collections.emptyList(), listener.events);
    });
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
        // TestRegion.dumpBackingMap();
        TestRegion.unregisterInterestRegex(".*");
        // TestRegion.dumpBackingMap();
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
        for (int i = 0; i < 10; i++) {
          assertTrue("expected a tombstone for Object" + i,
              TestRegion.containsTombstone("Object" + i));
        }
        return null;
      }
    });
  }

  private void ensureAllTombstonesPresent(VM vm, final List keys) {
    vm.invoke(new SerializableCallable("check tombstones in list") {
      public Object call() {
        for (Object key : keys) {
          assertTrue("expected to find a tombstone for " + key, TestRegion.containsTombstone(key));
        }
        return null;
      }
    });
  }

  private void ensureAllInvalidsPresent(VM vm) {
    vm.invoke(new SerializableCallable("check all are tombstones") {
      public Object call() {
        for (int i = 0; i < 10; i++) {
          assertTrue("expected to find an entry for Object" + i,
              TestRegion.containsKey("Object" + i));
          assertTrue("expected to find entry invalid for Object" + i,
              !TestRegion.containsValue("Object" + i));
        }
        return null;
      }
    });
  }

  private void ensureAllInvalidsPresent(VM vm, final List keys) {
    vm.invoke(new SerializableCallable("check tombstones in list") {
      public Object call() {
        for (Object key : keys) {
          assertTrue("expected to find an entry for " + key, TestRegion.containsKey(key));
          assertTrue("expected to find entry invalid for " + key, !TestRegion.containsValue(key));
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
        for (int i = 0; i < 10; i++) {
          keys.add("Object" + i);
        }
        Map result = TestRegion.getAll(keys);
        for (int i = 0; i < 10; i++) {
          assertNull("expected no result for Object" + i, result.get("Object" + i));
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

  // private void closeCache(VM vm) {

  @Test
  public void testClientServerRRQueueCleanup() { // see bug #50879 if this fails
    clientServerTombstoneMessageTest(true);
  }

  @Test
  public void testClientServerPRQueueCleanup() { // see bug #50879 if this fails
    clientServerTombstoneMessageTest(false);
  }

  /**
   * test that distributed GC messages are properly cleaned out of durable client HA queues
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
      // other bucket might be in vm1
      forceGC(vm1);
    }
    Wait.pause(5000); // better chance that WaitCriteria will succeed 1st time if we pause a bit
    checkClientReceivedGC(vm2);
    checkClientReceivedGC(vm3);
    checkServerQueuesEmpty(vm0);
    checkServerQueuesEmpty(vm1);
  }


  // private void closeCache(VM vm) {
  // vm.invoke(new SerializableCallable() {
  // public Object call() throws Exception {
  // closeCache();
  // return null;
  // }
  // });
  // }

  private void createEntries(VM vm) {
    vm.invoke(new SerializableCallable("create entries") {
      public Object call() {
        for (int i = 0; i < 10; i++) {
          TestRegion.create("Object" + i, Integer.valueOf(i));
        }
        return null;
      }
    });
  }


  private void destroyEntries(VM vm) {
    vm.invoke(new SerializableCallable("destroy entries") {
      public Object call() {
        for (int i = 0; i < 10; i++) {
          TestRegion.destroy("Object" + i, Integer.valueOf(i));
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
        for (int i = 1000; i < 1100; i++) {
          map.put("object_" + i, i);
        }
        try {
          TestRegion.putAll(map);
          for (int i = 1000; i < 1100; i++) {
            assertTrue("expected key object_" + i + " to be in the cache but it isn't",
                TestRegion.containsKey("object_" + i));
          }
        } catch (NullPointerException e) {
          Assert.fail("caught NPE", e);
        }
      }
    });
  }


  private void invalidateEntries(VM vm) {
    vm.invoke(new SerializableCallable("invalidate entries") {
      public Object call() {
        for (int i = 0; i < 10; i++) {
          TestRegion.invalidate("Object" + i, Integer.valueOf(i));
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
            LogWriterUtils.getLogWriter()
                .info("tombstone count = " + TestRegion.getTombstoneCount());
            LogWriterUtils.getLogWriter().info("region size = " + TestRegion.size());
            return TestRegion.getTombstoneCount() == 0 && TestRegion.size() == 0;
          }

          @Override
          public String description() {
            return "waiting for garbage collection to occur";
          }
        };
        Wait.waitForCriterion(wc, 60000, 2000, true);
        return null;
      }
    });
  }

  private void checkServerQueuesEmpty(VM vm) {
    vm.invoke(new SerializableCallable(
        "check that client queues are properly cleared of old ClientTombstone messages") {

      public Object call() throws Exception {
        WaitCriterion wc = new WaitCriterion() {
          // boolean firstTime = true;

          @Override
          public boolean done() {
            CacheClientNotifier singleton = CacheClientNotifier.getInstance();
            Collection<CacheClientProxy> proxies = singleton.getClientProxies();
            // boolean first = firstTime;
            // firstTime = false;
            for (CacheClientProxy proxy : proxies) {
              if (!proxy.isPrimary()) { // bug #50683 only applies to backup queues
                int size = proxy.getQueueSize();
                if (size > 0) {
                  // if (first) {
                  // ((LocalRegion)proxy.getHARegion()).dumpBackingMap();
                  // }
                  LogWriterUtils.getLogWriter()
                      .info("queue size (" + size + ") is still > 0 for " + proxy.getProxyID());
                  return false;
                }
              }
            }
            // also ensure that server regions have been cleaned up
            int regionEntryCount = TestRegion.getRegionMap().size();
            if (regionEntryCount > 0) {
              LogWriterUtils.getLogWriter()
                  .info("TestRegion has unexpected entries - all should have been GC'd but we have "
                      + regionEntryCount);
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
        Wait.waitForCriterion(wc, 60000, 2000, true);
        return null;
      }
    });
  }


  private void checkClientDoesNotReceiveGC(VM vm) {
    vm.invoke(new SerializableCallable("check that GC did not happen") {
      public Object call() throws Exception {
        if (TestRegion.getTombstoneCount() == 0) {
          LogWriterUtils.getLogWriter().warning("region has no tombstones");
          // TestRegion.dumpBackingMap();
          throw new AssertionError("expected to find tombstones but region is empty");
        }
        return null;
      }
    });
  }


  private int createServerRegion(VM vm, final String regionName, final boolean replicatedRegion) {
    return createServerRegion(vm, regionName, replicatedRegion, Scope.DISTRIBUTED_ACK);
  }

  private int createServerRegion(VM vm, final String regionName, final boolean replicatedRegion,
      Scope regionScope) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        // TombstoneService.VERBOSE = true;
        AttributesFactory af = new AttributesFactory();
        if (replicatedRegion) {
          af.setScope(regionScope);
          af.setDataPolicy(DataPolicy.REPLICATE);
        } else {
          af.setDataPolicy(DataPolicy.PARTITION);
          af.setPartitionAttributes(
              (new PartitionAttributesFactory()).setTotalNumBuckets(2).create());
        }
        TestRegion = (LocalRegion) createRootRegion(regionName, af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }


  private void createClientRegion(final VM vm, final String regionName, final int port,
      final boolean ccEnabled, final ClientRegionShortcut clientRegionShortcut) {
    createClientRegion(vm, regionName, port, ccEnabled, clientRegionShortcut, true);
  }

  private void createClientRegion(final VM vm, final String regionName, final int port,
      final boolean ccEnabled, final ClientRegionShortcut clientRegionShortcut,
      final boolean registerInterest) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port);
        if (registerInterest) {
          cf.setPoolSubscriptionEnabled(true);
        }
        cf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        ClientCache cache = getClientCache(cf);
        ClientRegionFactory crf = cache.createClientRegionFactory(clientRegionShortcut);
        crf.setConcurrencyChecksEnabled(ccEnabled);
        crf.setStatisticsEnabled(true);
        TestRegion = (LocalRegion) crf.create(regionName);
        if (registerInterest) {
          TestRegion.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES, false, true);
        }
        return null;
      }
    };
    vm.invoke(createRegion);
  }

  // For durable client QRM testing we need a backup queue (redundancy=1) and
  // durable attributes. We also need to invoke readyForEvents()
  private void createDurableClientRegion(final VM vm, final String regionName, final int port1,
      final int port2, final boolean ccEnabled) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port1);
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port2);
        cf.setPoolSubscriptionEnabled(true);
        cf.setPoolSubscriptionRedundancy(1);
        // bug #50683 - secondary durable queue retains all GC messages
        cf.set(DURABLE_CLIENT_ID, "" + vm.getId());
        cf.set(DURABLE_CLIENT_TIMEOUT, "" + 200);
        cf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        ClientCache cache = getClientCache(cf);
        ClientRegionFactory crf =
            cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        crf.setConcurrencyChecksEnabled(ccEnabled);
        TestRegion = (LocalRegion) crf.create(regionName);
        TestRegion.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES, true, true);
        cache.readyForEvents();
        return null;
      }
    };
    vm.invoke(createRegion);
  }

  private static class RecordingCacheListener extends CacheListenerAdapter {
    List<EntryEvent> events = new ArrayList<EntryEvent>();

    @Override
    public void afterCreate(final EntryEvent event) {
      events.add(event);
    }

    @Override
    public void afterDestroy(final EntryEvent event) {
      events.add(event);
    }

    @Override
    public void afterInvalidate(final EntryEvent event) {
      events.add(event);
    }

    @Override
    public void afterUpdate(final EntryEvent event) {
      events.add(event);
    }
  }

}
