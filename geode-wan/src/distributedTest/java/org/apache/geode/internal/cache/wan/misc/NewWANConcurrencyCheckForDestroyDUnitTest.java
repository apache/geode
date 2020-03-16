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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token.Tombstone;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.junit.categories.WanTest;

/**
 *
 * Test verifies that version tag for destroyed entry is propagated back to origin distributed
 * system if the version tag is applied and replaces old version information in destination
 * distributed system.
 *
 * Version tag information which is relevant between multiple distributed systems consistency check
 * is basically dsid and timestamp.
 */
@Category({WanTest.class})
public class NewWANConcurrencyCheckForDestroyDUnitTest extends WANTestBase {

  public NewWANConcurrencyCheckForDestroyDUnitTest() {
    super();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testVersionTagTimestampForDestroy() {


    // create three distributed systems with each having a cache containing
    // a Replicated Region with one entry and concurrency checks enabled.

    // Site 2 and Site 3 only know about Site 1 but Site 1 knows about both
    // Site 2 and Site 3.

    // Site 1
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1);
    vm1.invoke(WANTestBase::createReceiver);

    // Site 2
    Integer nyPort = vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    createCacheInVMs(nyPort, vm3);
    vm3.invoke(WANTestBase::createReceiver);

    // Site 3
    Integer tkPort = vm4.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));
    createCacheInVMs(tkPort, vm5);
    vm5.invoke(WANTestBase::createReceiver);

    getLogWriter().info("Created locators and receivers in 3 distributed systems");

    // Site 1
    vm1.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 1, false, false, null, true));
    vm1.invoke(() -> WANTestBase.createSender("ln2", 3, true, 10, 1, false, false, null, true));

    vm1.invoke(
        () -> WANTestBase.createPartitionedRegion("repRegion", "ln1,ln2", 0, 1, isOffHeap()));
    vm1.invoke(() -> WANTestBase.startSender("ln1"));
    vm1.invoke(() -> WANTestBase.startSender("ln2"));
    vm1.invoke(() -> WANTestBase.waitForSenderRunningState("ln1"));
    vm1.invoke(() -> WANTestBase.waitForSenderRunningState("ln2"));

    // Site 2
    vm3.invoke(() -> WANTestBase.createSender("ny1", 1, true, 10, 1, false, false, null, true));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion("repRegion", "ny1", 0, 1, isOffHeap()));
    vm3.invoke(() -> WANTestBase.startSender("ny1"));
    vm3.invoke(() -> WANTestBase.waitForSenderRunningState("ny1"));

    // Site 3 which only knows about Site 1.
    vm5.invoke(() -> WANTestBase.createSender("tk1", 1, true, 10, 1, false, false, null, true));

    vm5.invoke(() -> WANTestBase.createPartitionedRegion("repRegion", "tk1", 0, 1, isOffHeap()));
    vm5.invoke(() -> WANTestBase.startSender("tk1"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("tk1"));

    deprecatedPause(2000);

    // Perform a put in vm1
    vm1.invoke(new CacheSerializableRunnable("Putting an entry in ds1") {

      @Override
      public void run2() throws CacheException {
        assertNotNull(cache);

        Region<String, String> region = cache.getRegion(SEPARATOR + "repRegion");
        region.put("testKey", "testValue");

        assertEquals(1, region.size());
      }
    });

    // wait for vm1 to propagate put to vm3 and vm5
    deprecatedPause(2000);

    long destroyTimeStamp = vm3
        .invoke(NewWANConcurrencyCheckForDestroyDUnitTest::getVersionTimestampAfterOp);

    // wait for vm1 to propagate destroyed entry's new version tag to vm5
    deprecatedPause(2000);

    vm5.invoke(() -> NewWANConcurrencyCheckForDestroyDUnitTest.verifyTimestampAfterOp(
        destroyTimeStamp, 1 /* ds 3 receives gateway event only from ds 1 */));
  }

  /**
   * Test creates two sites and one Replicated Region on each with Serial GatewaySender on each.
   * Test checks for sequence of events being sent from site1 to site2 for PUTALL and PUT and
   * finally checks for final timestamp in version for RegionEntry with key "testKey". If timestamp
   * on both site is same that means events were transferred in correct sequence.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testPutAllEventSequenceOnSerialGatewaySenderWithRR() {

    // create two distributed systems with each having a cache containing
    // a Replicated Region with one entry and concurrency checks enabled.

    // Site 1
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    vm1.invoke(() -> WANTestBase.createCache(lnPort));
    vm1.invoke(WANTestBase::createReceiver);

    // Site 2
    Integer nyPort = vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));
    vm3.invoke(WANTestBase::createReceiver);

    getLogWriter().info("Created locators and receivers in 2 distributed systems");

    // Site 1
    vm1.invoke(() -> WANTestBase.createSender("ln1", 2, false, 10, 1, false, false, null, true));

    vm1.invoke(() -> WANTestBase.createReplicatedRegion("repRegion", "ln1", isOffHeap()));
    vm1.invoke(() -> WANTestBase.startSender("ln1"));
    vm1.invoke(() -> WANTestBase.waitForSenderRunningState("ln1"));

    // Site 2
    vm3.invoke(() -> WANTestBase.createSender("ny1", 1, false, 10, 1, false, false, null, true));

    vm3.invoke(() -> WANTestBase.createReplicatedRegion("repRegion", "ny1", isOffHeap()));
    vm3.invoke(() -> WANTestBase.startSender("ny1"));
    vm3.invoke(() -> WANTestBase.waitForSenderRunningState("ny1"));

    deprecatedPause(2000);

    // Perform a put in vm1
    AsyncInvocation<Void> asynch1 =
        vm1.invokeAsync("Putting an entry in ds1", () -> {
          assertNotNull(cache);
          // Test hook to make put wait after RE lock is released but before Gateway events are
          // sent.
          DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 2000;

          Region<String, String> region = cache.getRegion(SEPARATOR + "repRegion");
          Map<String, String> testMap = new HashMap<>();
          testMap.put("testKey", "testValue1");
          region.putAll(testMap);

          assertEquals(1, region.size());
          assertEquals("testValue2", region.get("testKey"));
        });

    // wait for vm1 to propagate put to vm3
    deprecatedPause(1000);

    AsyncInvocation<Void> asynch2 =
        vm1.invokeAsync("Putting an entry in ds1", () -> {
          assertNotNull(cache);
          Region<String, String> region = cache.getRegion(SEPARATOR + "repRegion");

          while (!region.containsKey("testKey")) {
            deprecatedPause(10);
          }
          // Test hook to make put wait after RE lock is released but before Gateway events are
          // sent.
          DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 0;

          region.put("testKey", "testValue2");

          assertEquals(1, region.size());
          assertEquals("testValue2", region.get("testKey"));
        });

    try {
      asynch1.join(5000);
      asynch2.join(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      vm1.invoke(new CacheSerializableRunnable("Reset Test Hook") {

        @Override
        public void run2() throws CacheException {
          DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 0;
        }
      });
    }

    // Wait for all Gateway events be received by vm3.
    deprecatedPause(1000);

    long putAllTimeStampVm1 = vm1
        .invoke(NewWANConcurrencyCheckForDestroyDUnitTest::getVersionTimestampAfterPutAllOp);

    long putAllTimeStampVm3 = vm3
        .invoke(NewWANConcurrencyCheckForDestroyDUnitTest::getVersionTimestampAfterPutAllOp);

    assertEquals(putAllTimeStampVm1, putAllTimeStampVm3);
  }

  /**
   * This is similar to above test but for PartitionedRegion.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testPutAllEventSequenceOnSerialGatewaySenderWithPR() {

    // create two distributed systems with each having a cache containing
    // a Replicated Region with one entry and concurrency checks enabled.

    // Site 1
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1);
    vm1.invoke(WANTestBase::createReceiver);

    // Site 2
    Integer nyPort = vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    createCacheInVMs(nyPort, vm3);
    vm3.invoke(WANTestBase::createReceiver);

    getLogWriter().info("Created locators and receivers in 2 distributed systems");

    // Site 1
    vm1.invoke(() -> WANTestBase.createSender("ln1", 2, false, 10, 1, false, false, null, true));

    vm1.invoke(() -> WANTestBase.createPartitionedRegion("repRegion", "ln1", 0, 1, isOffHeap()));
    vm1.invoke(() -> WANTestBase.startSender("ln1"));
    vm1.invoke(() -> WANTestBase.waitForSenderRunningState("ln1"));

    // Site 2
    vm3.invoke(() -> WANTestBase.createSender("ny1", 1, false, 10, 1, false, false, null, true));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion("repRegion", "ny1", 0, 1, isOffHeap()));
    vm3.invoke(() -> WANTestBase.startSender("ny1"));
    vm3.invoke(() -> WANTestBase.waitForSenderRunningState("ny1"));

    deprecatedPause(2000);

    // Perform a put in vm1
    AsyncInvocation<Void> asynch1 =
        vm1.invokeAsync("Putting an entry in ds1", () -> {
          assertNotNull(cache);
          // Test hook to make put wait after RE lock is released but before Gateway events are
          // sent.
          DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 2000;

          Region<String, String> region = cache.getRegion(SEPARATOR + "repRegion");
          Map<String, String> testMap = new HashMap<>();
          testMap.put("testKey", "testValue1");
          region.putAll(testMap);

          assertEquals(1, region.size());
          assertEquals("testValue2", region.get("testKey"));
        });

    // wait for vm1 to propagate put to vm3
    deprecatedPause(1000);

    AsyncInvocation<Void> asynch2 =
        vm1.invokeAsync("Putting an entry in ds1", () -> {
          assertNotNull(cache);
          Region<String, String> region = cache.getRegion(SEPARATOR + "repRegion");

          while (!region.containsKey("testKey")) {
            deprecatedPause(10);
          }
          // Test hook to make put wait after RE lock is released but before Gateway events are
          // sent.
          DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 0;

          region.put("testKey", "testValue2");

          assertEquals(1, region.size());
          assertEquals("testValue2", region.get("testKey"));
        });

    try {
      asynch1.join(5000);
      asynch2.join(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      vm1.invoke(new CacheSerializableRunnable("Reset Test Hook") {

        @Override
        public void run2() throws CacheException {
          DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 0;
        }
      });
    }

    // Wait for all Gateway events be received by vm3.
    deprecatedPause(1000);

    long putAllTimeStampVm1 = vm1
        .invoke(NewWANConcurrencyCheckForDestroyDUnitTest::getVersionTimestampAfterPutAllOp);

    long putAllTimeStampVm3 = vm3
        .invoke(NewWANConcurrencyCheckForDestroyDUnitTest::getVersionTimestampAfterPutAllOp);

    assertEquals(putAllTimeStampVm1, putAllTimeStampVm3);
  }

  /**
   * Tests if conflict checks are happening based on DSID and timestamp even if version tag is
   * generated in local distributed system.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testConflictChecksBasedOnDsidAndTimeStamp() {


    // create two distributed systems with each having a cache containing
    // a Replicated Region with one entry and concurrency checks enabled.

    // Site 1
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1);
    vm1.invoke(WANTestBase::createReceiver);

    // Site 2
    Integer nyPort = vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    createCacheInVMs(nyPort, vm3);
    vm3.invoke(WANTestBase::createReceiver);

    getLogWriter().info("Created locators and receivers in 2 distributed systems");

    // Site 1
    vm1.invoke(() -> WANTestBase.createSender("ln1", 2, false, 10, 1, false, false, null, true));

    vm1.invoke(() -> WANTestBase.createReplicatedRegion("repRegion", "ln1", isOffHeap()));
    vm1.invoke(() -> WANTestBase.startSender("ln1"));
    vm1.invoke(() -> WANTestBase.waitForSenderRunningState("ln1"));

    // Site 2
    vm3.invoke(() -> WANTestBase.createReplicatedRegion("repRegion", "ny1", isOffHeap()));

    vm4.invoke(() -> WANTestBase.createCache(nyPort));
    vm4.invoke(() -> WANTestBase.createSender("ny1", 1, false, 10, 1, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createReplicatedRegion("repRegion", "ny1", isOffHeap()));
    vm4.invoke(() -> WANTestBase.startSender("ny1"));
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ny1"));

    deprecatedPause(2000);

    // Perform a put in vm1
    vm1.invoke(new CacheSerializableRunnable("Putting an entry in ds1") {

      @Override
      public void run2() throws CacheException {
        assertNotNull(cache);

        Region<String, String> region = cache.getRegion(SEPARATOR + "repRegion");
        region.put("testKey", "testValue1");

        assertEquals(1, region.size());
      }
    });

    // wait for vm4 to have later timestamp before sending operation to vm1
    deprecatedPause(300);

    AsyncInvocation<Void> asynch =
        vm4.invokeAsync("Putting an entry in ds2 in vm4", () -> {
          assertNotNull(cache);
          Region<String, String> region = cache.getRegion(SEPARATOR + "repRegion");

          region.put("testKey", "testValue2");

          assertEquals(1, region.size());
          assertEquals("testValue2", region.get("testKey"));
        });

    try {
      asynch.join(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Wait for all local ds events be received by vm3.
    deprecatedPause(1000);

    vm3.invoke("Check dsid", () -> {
      Region<String, String> region = cache.getRegion("repRegion");

      @SuppressWarnings("unchecked")
      Region.Entry<String, String> entry = ((LocalRegion) region).getEntry("testKey", /* null, */
          true); // commented while merging revision 43582
      RegionEntry re = null;
      if (entry instanceof EntrySnapshot) {
        re = ((EntrySnapshot) entry).getRegionEntry();
      } else if (entry instanceof NonTXEntry) {
        re = ((NonTXEntry) entry).getRegionEntry();
      }
      VersionTag<?> tag = re.getVersionStamp().asVersionTag();
      assertEquals(2, tag.getDistributedSystemId());
    });

    // Check vm3 has latest timestamp from vm4.
    long putAllTimeStampVm1 = vm4
        .invoke(NewWANConcurrencyCheckForDestroyDUnitTest::getVersionTimestampAfterPutAllOp);

    long putAllTimeStampVm3 = vm3
        .invoke(NewWANConcurrencyCheckForDestroyDUnitTest::getVersionTimestampAfterPutAllOp);

    assertEquals(putAllTimeStampVm1, putAllTimeStampVm3);
  }

  /*
   * For VM1 in ds 1. Used in testPutAllEventSequenceOnSerialGatewaySender.
   */
  @SuppressWarnings("deprecation")
  public static long getVersionTimestampAfterPutAllOp() {
    Region<String, String> region = cache.getRegion("repRegion");

    while (!(region.containsKey("testKey") /* && region.get("testKey").equals("testValue2") */)) {
      deprecatedPause(10);
    }
    assertEquals(1, region.size());

    @SuppressWarnings("unchecked")
    Region.Entry<String, String> entry =
        ((LocalRegion) region).getEntry("testKey", /* null, */ true);
    RegionEntry re = null;
    if (entry instanceof EntrySnapshot) {
      re = ((EntrySnapshot) entry).getRegionEntry();
    } else if (entry instanceof NonTXEntry) {
      re = ((NonTXEntry) entry).getRegionEntry();
    }
    if (re != null) {
      getLogWriter().fine(
          "RegionEntry for testKey: " + re.getKey() + " " + re.getValueInVM((LocalRegion) region));

      VersionTag<?> tag = re.getVersionStamp().asVersionTag();
      return tag.getVersionTimeStamp();
    } else {
      return -1;
    }
  }

  /*
   * For VM3 in ds 2.
   */
  @SuppressWarnings("deprecation")
  public static long getVersionTimestampAfterOp() {
    Region<String, ?> region = cache.getRegion("repRegion");
    assertEquals(1, region.size());

    region.destroy("testKey");

    @SuppressWarnings("unchecked")
    Region.Entry<String, ?> entry = ((LocalRegion) region).getEntry("testKey", /* null, */ true);
    RegionEntry re = ((EntrySnapshot) entry).getRegionEntry();
    getLogWriter().fine(
        "RegionEntry for testKey: " + re.getKey() + " " + re.getValueInVM((LocalRegion) region));
    assertTrue(re.getValueInVM((LocalRegion) region) instanceof Tombstone);

    VersionTag<?> tag = re.getVersionStamp().asVersionTag();
    return tag.getVersionTimeStamp();
  }

  /*
   * For VM 5 in ds 3.
   */
  public static void verifyTimestampAfterOp(long timestamp, int memberid) {
    Region<String, ?> region = cache.getRegion("repRegion");
    assertEquals(0, region.size());

    @SuppressWarnings("unchecked")
    Region.Entry<String, ?> entry = ((LocalRegion) region).getEntry("testKey", /* null, */ true);
    RegionEntry re = ((EntrySnapshot) entry).getRegionEntry();
    assertTrue(re.getValueInVM((LocalRegion) region) instanceof Tombstone);

    VersionTag<?> tag = re.getVersionStamp().asVersionTag();
    assertEquals(timestamp, tag.getVersionTimeStamp());
    assertEquals(memberid, tag.getDistributedSystemId());
  }
}
