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
package org.apache.geode.internal.cache.event;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Tests <code>EventTracker</code> management.
 *
 * @since GemFire 6.5
 */

public class EventTrackerDUnitTest extends JUnit4CacheTestCase {

  /** The port on which the <code>CacheServer</code> was started in this VM */
  private static int cacheServerPort;

  /** The <code>Cache</code>'s <code>ExpiryTask</code>'s ping interval */
  private static final String MESSAGE_TRACKING_TIMEOUT = "5000";

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * Tests <code>EventTracker</code> is created and destroyed when a <code>Region</code> is created
   * and destroyed.
   */
  @Test
  public void testEventTrackerCreateDestroy() throws CacheException {
    // Verify the Cache's ExpiryTask contains no EventTrackers
    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    EventTrackerExpiryTask expiryTask = cache.getEventTrackerTask();
    assertNotNull(expiryTask);

    // We start with 3 event trackers:
    // one for the PDX registry region
    // one for ManagementConstants.MONITORING_REGION
    // one for ManagementConstants.NOTIFICATION_REGION
    final int EXPECTED_TRACKERS = 3;
    assertEquals(EXPECTED_TRACKERS, expiryTask.getNumberOfTrackers());

    // Create a distributed Region
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    LocalRegion region = (LocalRegion) createRegion(getName(), factory.create());

    // Verify an EventTracker is created and is empty
    EventTracker eventTracker = region.getEventTracker();
    assertNotNull(eventTracker);
    Map eventState = region.getEventState();
    assertNotNull(eventState);
    assertEquals(0, eventState.size());

    // Verify it and the root region's EventTracker are added to the Cache's ExpiryTask's trackers
    assertEquals(EXPECTED_TRACKERS + 2, expiryTask.getNumberOfTrackers());

    // Destroy the Region
    region.destroyRegion();

    // Verify the EventTracker is removed from the Cache's ExpiryTask's trackers
    assertEquals(EXPECTED_TRACKERS + 1, expiryTask.getNumberOfTrackers());
  }

  /**
   * Tests adding threads to an <code>EventTracker</code>.
   */
  @Test
  public void testEventTrackerAddThreadIdentifier() throws CacheException {
    Host host = Host.getHost(0);
    VM serverVM = host.getVM(0);
    VM clientVM = host.getVM(1);
    final String regionName = getName();

    // Create Region in the server and verify tracker is created
    serverVM.invoke(new CacheSerializableRunnable("Create server") {
      public void run2() throws CacheException {
        // Create a distributed Region
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        LocalRegion region = (LocalRegion) createRegion(regionName, factory.create());

        // Verify an EventTracker is created
        EventTracker eventTracker = region.getEventTracker();
        assertNotNull(eventTracker);
        try {
          startCacheServer();
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Verify tracker in server contains no entries
    serverVM.invoke(new CacheSerializableRunnable("Do puts") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(regionName);
        Map eventState = region.getEventState();
        assertEquals(0, eventState.size());
      }
    });

    // Create Create Region in the client
    final int port = serverVM.invoke(() -> EventTrackerDUnitTest.getCacheServerPort());
    final String hostName = NetworkUtils.getServerHostName(host);
    clientVM.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        getCache();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, hostName, port, -1, false, -1, -1,
            null);
        createRegion(regionName, factory.create());
      }
    });

    // Do puts in the client
    clientVM.invoke(new CacheSerializableRunnable("Do puts") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < 10; i++) {
          region.put(i, i);
        }
      }
    });

    // Verify tracker in server contains an entry for client thread
    serverVM.invoke(new CacheSerializableRunnable("Do puts") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(regionName);
        Map eventState = region.getEventState();
        assertEquals(1, eventState.size());
      }
    });
  }

  /**
   * Tests adding events to and removing events from an <code>EventTracker</code>.
   */
  @Test
  public void testEventTrackerAddRemoveThreadIdentifier() throws CacheException {
    Host host = Host.getHost(0);
    VM serverVM = host.getVM(0);
    VM clientVM = host.getVM(1);
    final String regionName = getName();

    // Create Region in the server and verify tracker is created
    serverVM.invoke(new CacheSerializableRunnable("Create server") {
      public void run2() throws CacheException {
        // Set the message tracking timeout
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "messageTrackingTimeout",
            MESSAGE_TRACKING_TIMEOUT);

        // Create a distributed Region
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        LocalRegion region = (LocalRegion) createRegion(regionName, factory.create());

        // Verify an EventTracker is created
        EventTracker eventTracker = region.getEventTracker();
        assertNotNull(eventTracker);
        try {
          startCacheServer();
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Verify tracker in server contains no entries
    serverVM.invoke(new CacheSerializableRunnable("Do puts") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(regionName);
        Map eventState = region.getEventState();
        assertEquals(0, eventState.size());
      }
    });

    // Create Create Region in the client
    final int port = serverVM.invoke(() -> EventTrackerDUnitTest.getCacheServerPort());
    final String hostName = NetworkUtils.getServerHostName(host);
    clientVM.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        getCache();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, hostName, port, -1, false, -1, -1,
            null);
        createRegion(regionName, factory.create());
      }
    });

    // Do puts in the client
    clientVM.invoke(new CacheSerializableRunnable("Do puts") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < 10; i++) {
          region.put(i, i);
        }
      }
    });

    // Verify tracker in server
    serverVM.invoke(new CacheSerializableRunnable("Do puts") {
      public void run2() throws CacheException {
        // First verify it contains an entry
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(regionName);
        Map eventState = region.getEventState();
        assertEquals(1, eventState.size());

        // Pause for the message tracking timeout
        int waitTime = Integer.parseInt(MESSAGE_TRACKING_TIMEOUT) * 3;
        Wait.pause(waitTime);

        // Verify the server no longer contains an entry
        eventState = region.getEventState();
        assertEquals(0, eventState.size());
      }
    });
  }

  /**
   * Test to make sure we don't leak put all events in the event tracker after multiple putAlls
   */
  @Test
  public void testPutAllHoldersInEventTracker() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createRegion = new SerializableRunnable("createRegion") {

      public void run() {
        Cache cache = getCache();
        RegionFactory<Object, Object> rf =
            cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(3);
        rf.setPartitionAttributes(paf.create());
        rf.setConcurrencyChecksEnabled(true);
        rf.create("partitioned");

        rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
        rf.setConcurrencyChecksEnabled(true);
        rf.create("replicate");
        try {
          startCacheServer();
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    };

    vm0.invoke(createRegion);
    vm1.invoke(createRegion);

    // Create Create Region in the client
    final int port = vm0.invoke(() -> EventTrackerDUnitTest.getCacheServerPort());
    final String hostName = NetworkUtils.getServerHostName(host);
    vm2.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        getCache();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, hostName, port, -1, false, -1, -1,
            null);
        createRootRegion("partitioned", factory.create());
        createRootRegion("replicate", factory.create());
      }
    });

    doTwoPutAlls(vm2, "partitioned");
    doTwoPutAlls(vm2, "replicate");

    // Make sure that the event tracker for each bucket only records the last
    // event.
    checkBucketEventTracker(vm0, 0, 3);
    checkBucketEventTracker(vm1, 0, 3);
    checkBucketEventTracker(vm0, 1, 3);
    checkBucketEventTracker(vm1, 1, 3);
    checkBucketEventTracker(vm0, 2, 3);
    checkBucketEventTracker(vm1, 2, 3);

    checkReplicateEventTracker(vm0, 9);
    checkReplicateEventTracker(vm1, 9);
  }

  private void doTwoPutAlls(VM vm, final String regionName) {
    SerializableRunnable createData = new SerializableRunnable("putAlls") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        Map putAllMap = new HashMap();
        for (int i = 0; i < 9; i++) {
          putAllMap.put(i, i);
        }
        region.putAll(putAllMap);

        putAllMap.clear();
        for (int i = 10; i < 19; i++) {
          putAllMap.put(i, i);
        }
        region.putAll(putAllMap);
      }
    };

    vm.invoke(createData);
  }

  private SerializableRunnable checkReplicateEventTracker(VM vm, final int expectedEntryCount) {
    SerializableRunnable checkEventTracker = new SerializableRunnable("checkEventTracker") {

      public void run() {
        Cache cache = getCache();
        DistributedRegion region = (DistributedRegion) cache.getRegion("replicate");
        checkEventTracker(region, expectedEntryCount);
      }
    };
    vm.invoke(checkEventTracker);
    return checkEventTracker;
  }

  private SerializableRunnable checkBucketEventTracker(VM vm, final int bucketNumber,
      final int expectedEntryCount) {
    SerializableRunnable checkEventTracker = new SerializableRunnable("checkEventTracker") {

      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("partitioned");
        BucketRegion br = region.getBucketRegion(bucketNumber);

        checkEventTracker(br, expectedEntryCount);
      }
    };
    vm.invoke(checkEventTracker);
    return checkEventTracker;
  }

  private void checkEventTracker(LocalRegion region, int numberOfEvents) {
    EventTracker tracker = region.getEventTracker();
    ConcurrentMap<ThreadIdentifier, BulkOperationHolder> memberToTags =
        tracker.getRecordedBulkOpVersionTags();
    assertEquals("memberToTags=" + memberToTags, 1, memberToTags.size());
    BulkOperationHolder holder = memberToTags.values().iterator().next();
    // We expect the holder to retain only the last putAll that was performed.
    assertEquals("entryToVersionTags=" + holder.getEntryVersionTags(), numberOfEvents,
        holder.getEntryVersionTags().size());
  }

  protected void startCacheServer() throws IOException {
    CacheServer cacheServer = getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    cacheServerPort = cacheServer.getPort();
  }

  protected static int getCacheServerPort() {
    return cacheServerPort;
  }

  /**
   * Tests event track is initialized after gii
   */
  @Test
  public void testEventTrackerIsInitalized() throws CacheException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    createPRInVMs(vm0, vm1, vm2);

    createPR();

    doPutsInVMs(vm0, vm1, vm2);

    doPuts();

    verifyEventTrackerContent();

    // close the region
    getCache().getRegion(getName()).close();

    // create the region again.
    createPR();

    for (int i = 0; i < 12; i++) {
      waitEntryIsLocal(i);
    }

    // verify event track initialized after create region
    verifyEventTrackerContent();

  }

  private void waitEntryIsLocal(int i) {
    await()
        .until(() -> getCache().getRegion(getName()).getEntry(i) != null);
  }

  private void verifyEventTrackerContent() {
    PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(getName());
    BucketRegion br = pr.getDataStore().getLocalBucketById(0);
    Map<?, ?> eventStates = br.getEventState();
    assertTrue(eventStates.size() == 4);
  }

  public void createPRInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> createPR());
    }
  }

  private void createPR() {
    PartitionAttributesFactory paf =
        new PartitionAttributesFactory().setRedundantCopies(3).setTotalNumBuckets(4);
    RegionFactory fact = getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(paf.create());
    fact.create(getName());
  }

  public void doPutsInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> doPuts());
    }
  }

  private void doPuts() {
    Region region = getCache().getRegion(getName());
    for (int i = 0; i < 12; i++) {
      region.put(i, i);
    }
  }
}
