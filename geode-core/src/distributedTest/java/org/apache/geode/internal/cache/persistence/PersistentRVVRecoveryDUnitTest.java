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
package org.apache.geode.internal.cache.persistence;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.internal.cache.TombstoneService.EXPIRED_TOMBSTONE_LIMIT_DEFAULT;
import static org.apache.geode.internal.cache.TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT_DEFAULT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.CacheObserverAdapter;
import org.apache.geode.internal.cache.CacheObserverHolder;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskStoreFactoryImpl;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.DiskStoreObserver;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token.Tombstone;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.PersistenceTest;

@Category(PersistenceTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class PersistentRVVRecoveryDUnitTest extends PersistentReplicatedTestBase {

  private static final int TEST_REPLICATED_TOMBSTONE_TIMEOUT = 1_000;
  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @After
  public void tearDown() {
    for (VM vm : toArray(getAllVMs(), getController())) {
      vm.invoke(() -> {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        CacheObserverHolder.setInstance(null);
        DiskStoreObserver.setInstance(null);
        TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT = REPLICATE_TOMBSTONE_TIMEOUT_DEFAULT;
        TombstoneService.EXPIRED_TOMBSTONE_LIMIT = EXPIRED_TOMBSTONE_LIMIT_DEFAULT;
      });
    }
  }

  @Test
  public void testNoConcurrencyChecks() {
    getCache();

    RegionFactory regionFactory = new RegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setConcurrencyChecksEnabled(false);

    Throwable thrown = catchThrowable(() -> regionFactory.create(regionName));
    assertThat(thrown).isInstanceOf(IllegalStateException.class);
  }

  /**
   * Test that we can recover the RVV information.
   */
  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}({params})")
  public void testRecoveryWithOrWithoutKRF(boolean deleteKRFs) throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    // Create the region in few members to test recovery
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    createPersistentRegion(vm2);

    // Create and delete some keys (to update the RVV)
    createData(vm0, 0, 5, "value1");
    createData(vm1, 3, 8, "value2");
    createData(vm2, 6, 11, "value3");

    delete(vm1, 0, 1);
    delete(vm0, 10, 11);

    // Make sure the RVVs are the same in the members
    RegionVersionVector vm0RVV = getRVV(vm0);
    RegionVersionVector vm1RVV = getRVV(vm1);
    RegionVersionVector vm2RVV = getRVV(vm2);

    assertSameRVV(vm0RVV, vm1RVV);
    assertSameRVV(vm0RVV, vm2RVV);

    // Closing the cache will ensure the disk store is closed
    closeCache(vm2);
    closeCache(vm1);
    closeCache(vm0);

    if (deleteKRFs) {
      deleteKRFs(getVM(0));
    }

    // Make sure we can recover the RVV
    createPersistentRegion(vm0);

    RegionVersionVector new0RVV = getRVV(vm0);
    assertSameRVV(vm0RVV, new0RVV);
    assertThat(new0RVV.getOwnerId()).isEqualTo(vm0RVV.getOwnerId());

    createData(vm0, 12, 15, "value");

    // Make sure we can GII the RVV
    new0RVV = getRVV(vm0);
    assertSameRVV(new0RVV, getDiskRVV(vm0));
    createPersistentRegion(vm1);
    assertSameRVV(new0RVV, getRVV(vm1));
    assertSameRVV(new0RVV, getDiskRVV(vm1));

    closeCache(vm0);
    closeCache(vm1);

    if (deleteKRFs) {
      deleteKRFs(getVM(0));
    }

    // Make the sure member that GII'd the RVV can also recover it
    createPersistentRegion(vm1);
    assertSameRVV(new0RVV, getRVV(vm1));
  }

  /**
   * Test that we correctly recover and expire recovered tombstones, with compaction enabled
   */
  @Test
  public void testLotsOfTombstones() throws Exception {
    VM vm0 = getVM(0);

    // I think we just need to assert the number of tombstones, maybe?
    // Bruce has code that won't even let the tombstones expire for 10 minutes
    // That means on recovery we need to recover them all? Or do we need to recover
    // any? We're doing a GII. Won't we GII tombstones anyway? Ahh, but we need
    // to know that we don't need to record the new tombstones.

    InternalRegion region = createRegion(vm0);

    int initialCount = getTombstoneCount(region);
    assertThat(initialCount).isEqualTo(0);

    int entryCount = 20;
    for (int i = 0; i < entryCount; i++) {
      region.put(i, new byte[100]);
      region.destroy(i);
    }

    assertThat(getTombstoneCount(region)).isEqualTo(entryCount);

    // roll to a new oplog
    region.getDiskStore().forceRoll();

    // Force a compaction. This should do nothing, because the tombstones are not garbage, so only
    // 50% of the oplog is garbage (the creates).
    region.getDiskStore().forceCompaction();

    assertThat(region.getDiskStore().numCompactableOplogs()).isEqualTo(0);
    assertThat(getTombstoneCount(region)).isEqualTo(entryCount);

    getCache().close();

    region = createRegion(vm0);

    assertThat(getTombstoneCount(region)).isEqualTo(entryCount);

    TombstoneService tombstoneService = getCache().getTombstoneService();

    // Before expiring tombstones, no oplogs are available for compaction
    assertThat(region.getDiskStore().numCompactableOplogs()).isEqualTo(0);

    region.getDiskStore().forceCompaction();

    assertThat(
        tombstoneService.forceBatchExpirationForTests(entryCount / 2, TIMEOUT_MILLIS, MILLISECONDS))
            .isTrue();
    assertThat(getTombstoneCount(region)).isEqualTo(entryCount / 2);

    // After expiring, we should have an oplog available for compaction.
    assertThat(region.getDiskStore().numCompactableOplogs()).isEqualTo(1);

    // Test after restart the tombstones are still missing
    getCache().close();
    region = createRegion(vm0);
    assertThat(getTombstoneCount(region)).isEqualTo(entryCount / 2);

    // Should have an oplog available for compaction, because the tombstones were garbage collected
    assertThat(region.getDiskStore().numCompactableOplogs()).isEqualTo(1);

    // This should compact some oplogs
    region.getDiskStore().forceCompaction();
    assertThat(region.getDiskStore().numCompactableOplogs()).isEqualTo(0);

    // Restart again, and make sure the compaction didn't mess up our tombstone count
    getCache().close();
    region = createRegion(vm0);
    assertThat(getTombstoneCount(region)).isEqualTo(entryCount / 2);

    // Add a test hook that will shutdown the system as soon as we write a GC RVV record
    DiskStoreObserver.setInstance(new DiskStoreObserver() {

      @Override
      public void afterWriteGCRVV(DiskRegion dr) {
        // This will cause the disk store to shut down, preventing us from writing any other records
        throw new DiskAccessException();
      }
    });

    addIgnoredException(DiskAccessException.class);
    addIgnoredException(RegionDestroyedException.class);

    // Force expiration, with our test hook that should close the cache
    tombstoneService = getCache().getTombstoneService();
    assertThat(
        tombstoneService.forceBatchExpirationForTests(entryCount / 4, TIMEOUT_MILLIS, MILLISECONDS))
            .isTrue();

    getCache().close();

    // Restart again, and make sure the tombstones are in fact removed
    region = createRegion(vm0);
    assertThat(getTombstoneCount(region)).isEqualTo(entryCount / 4);
  }

  /**
   * Test that we correctly recover and expire recovered tombstones, with compaction enabled.
   *
   * This test differs from above test in that we need to make sure tombstones start expiring based
   * on their original time-stamp, NOT the time-stamp assigned during scheduling for expiration
   * after recovery.
   */
  @Ignore
  @Test
  public void testLotsOfTombstonesExpiration() {
    VM vm0 = getVM(0);

    vm0.invoke(() -> {
      InternalRegion region = createRegion(vm0);

      int initialCount = getTombstoneCount(region);
      assertThat(initialCount).isEqualTo(0);

      int entryCount = 20;
      for (int i = 0; i < entryCount; i++) {
        region.put(i, new byte[100]);
        region.destroy(i);
      }

      assertThat(getTombstoneCount(region)).isEqualTo(entryCount);

      // roll to a new oplog
      region.getDiskStore().forceRoll();

      // Force a compaction. This should do nothing, because the tombstones are not garbage, so
      // only 50% of the oplog is garbage (the creates).
      region.getDiskStore().forceCompaction();

      assertThat(region.getDiskStore().numCompactableOplogs()).isEqualTo(0);
      assertThat(getTombstoneCount(region)).isEqualTo(entryCount);

      getCache().close();

      // We should wait for timeout time so that tombstones are expired
      // right away when they are gIId based on their original timestamp.
      Wait.pause(TEST_REPLICATED_TOMBSTONE_TIMEOUT);

      TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT = TEST_REPLICATED_TOMBSTONE_TIMEOUT;
      TombstoneService.EXPIRED_TOMBSTONE_LIMIT = entryCount;

      // Do region GII
      region = createRegion(vm0);

      assertThat(getTombstoneCount(region)).isEqualTo(entryCount);

      // maximumSleepTime+500 in TombstoneSweeper GC thread
      Wait.pause(10_000);

      // Tombstones should have been expired and garbage collected by now by TombstoneService.
      assertThat(getTombstoneCount(region)).isEqualTo(0);

      // This should compact some oplogs
      region.getDiskStore().forceCompaction();
      assertThat(region.getDiskStore().numCompactableOplogs()).isEqualTo(0);

      // Test after restart the tombstones are still missing
      getCache().close();
      region = createRegion(vm0);
      assertThat(getTombstoneCount(region)).isEqualTo(0);

      // should have an oplog available for compaction because the tombstones were garbage collected
      assertThat(region.getDiskStore().numCompactableOplogs()).isEqualTo(0);

      getCache().close();
    });
  }

  /**
   * This test creates 2 VMs in a distributed system with a persistent PartitionedRegion and one VM
   * (VM1) puts an entry in region. Second VM (VM2) starts later and does a delta GII. During Delta
   * GII in VM2 a DESTROY operation happens in VM1 and gets propagated to VM2 concurrently with GII.
   * At this point if entry version is greater than the once received from GII then it must not get
   * applied. Which is Bug #45921.
   */
  @Test
  public void testConflictChecksDuringConcurrentDeltaGIIAndOtherOp() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    vm0.invoke(() -> {
      getCache();

      PartitionAttributesFactory<String, String> partitionAttributesFactory =
          new PartitionAttributesFactory<>();
      partitionAttributesFactory.setRedundantCopies(1);
      partitionAttributesFactory.setTotalNumBuckets(1);

      AttributesFactory<String, String> attributesFactory = new AttributesFactory<>();
      attributesFactory.setPartitionAttributes(partitionAttributesFactory.create());

      RegionFactory<String, String> regionFactory =
          getCache().createRegionFactory(attributesFactory.create());

      Region<String, String> region = regionFactory.create("prRegion");

      region.put("testKey", "testValue");
      assertThat(region.size()).isEqualTo(1);
    });

    // Create a cache and region, do an update to change the version no. and
    // restart the cache and region.
    vm1.invoke(() -> {
      getCache();

      PartitionAttributesFactory<String, String> partitionAttributesFactory =
          new PartitionAttributesFactory<>();
      partitionAttributesFactory.setRedundantCopies(1);
      partitionAttributesFactory.setTotalNumBuckets(1);

      AttributesFactory<String, String> attributesFactory = new AttributesFactory<>();
      attributesFactory.setPartitionAttributes(partitionAttributesFactory.create());

      RegionFactory<String, String> regionFactory =
          getCache().createRegionFactory(attributesFactory.create());
      Region<String, String> region = regionFactory.create("prRegion");

      region.put("testKey", "testValue2");

      getCache().close();

      // Restart
      regionFactory = getCache().createRegionFactory(attributesFactory.create());
      regionFactory.create("prRegion");
    });

    // Do a DESTROY in vm0 when delta GII is in progress in vm1 (Hopefully, Not guaranteed).
    AsyncInvocation destroyDuringDeltaGiiInVM0 = vm0.invokeAsync(() -> {
      Region<String, String> region = getCache().getRegion("prRegion");

      await().until(() -> region.get("testKey").equals("testValue2"));

      region.destroy("testKey");
    });

    destroyDuringDeltaGiiInVM0.await();

    vm1.invoke(() -> {
      InternalRegion region = (InternalRegion) getCache().getRegion("prRegion");

      Region.Entry entry = region.getEntry("testKey", true);
      RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();
      assertThat(regionEntry.getValueInVM(region)).isInstanceOf(Tombstone.class);

      VersionTag tag = regionEntry.getVersionStamp().asVersionTag();
      assertThat(tag.getEntryVersion()).isEqualTo(3 /* Two puts and a Destroy */);
    });

    closeCache(vm0);
    closeCache(vm1);
  }

  /**
   * Test that we skip conflict checks with entries that are on disk compared to entries that come
   * in as part of a GII
   */
  @Test
  public void testSkipConflictChecksForGIIdEntries() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Create the region in few members to test recovery
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    // Create an update some entries in vm0 and vm1.
    createData(vm0, 0, 1, "value1");
    createData(vm0, 0, 2, "value2");

    closeCache(vm1);

    // Reset the entry version in vm0.
    // This means that if we did a conflict check, vm0's key will have a lower entry version than
    // vm1, which would cause us to prefer vm1's value

    vm0.invoke(() -> {
      InternalRegion region = (InternalRegion) getCache().getRegion(regionName);
      region.put(0, "value3");
      RegionEntry regionEntry = region.getRegionEntry(0);
      // Sneak in and change the version number for an entry to generate a conflict.
      VersionTag tag = regionEntry.getVersionStamp().asVersionTag();
      tag.setEntryVersion(tag.getEntryVersion() - 2);
      regionEntry.getVersionStamp().setVersions(tag);
    });

    // Create vm1, doing a GII
    createPersistentRegion(vm1);

    checkData(vm0, 0, 1, "value3");
    // If we did a conflict check, this would be value2
    checkData(vm1, 0, 1, "value3");
  }

  /**
   * Test that we skip conflict checks with entries that are on disk compared to entries that come
   * in as part of a concurrent operation
   */
  @Test
  public void testSkipConflictChecksForConcurrentOps() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Create the region in few members to test recovery
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    // Create an update some entries in vm0 and vm1.
    createData(vm0, 0, 1, "value1");
    createData(vm0, 0, 1, "value2");
    createData(vm0, 0, 1, "value2");

    closeCache(vm1);

    // Update the keys in vm0 until the entry version rolls over. This means that if we did a
    // conflict check, vm0's key will have a lower entry version than vm1, which would cause us to
    // prefer vm1's value
    vm0.invoke(() -> {
      InternalRegion region = (InternalRegion) getCache().getRegion(regionName);
      region.put(0, "value3");
      RegionEntry regionEntry = region.getRegionEntry(0);
      // Sneak in and change the version number for an entry to generate a conflict.
      VersionTag tag = regionEntry.getVersionStamp().asVersionTag();
      tag.setEntryVersion(tag.getEntryVersion() - 2);
      regionEntry.getVersionStamp().setVersions(tag);
    });

    // Add an observer to vm0 which will perform a concurrent operation during the GII. If we do a
    // conflict check, this operation will be rejected because it will have a lower entry version
    // that what vm1 recovered from disk
    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage msg) {
          if (msg instanceof InitialImageOperation.RequestImageMessage) {
            if (((InitialImageOperation.RequestImageMessage) msg).regionPath.contains(regionName)) {
              createData(vm0, 0, 1, "value4");
              DistributionMessageObserver.setInstance(null);
            }
          }
        }
      });
    });

    // Create vm1, doing a GII
    createPersistentRegion(vm1);

    // If we did a conflict check, this would be value2
    checkData(vm0, 0, 1, "value4");
    checkData(vm1, 0, 1, "value4");
  }

  /**
   * Test that with concurrent updates to an async disk region, we correctly update the RVV On disk
   */
  @Test
  public void testUpdateRVVWithAsyncPersistence() throws Exception {
    VM vm1 = getVM(1);

    // Create a region with async persistence
    vm1.invoke(() -> createRegionWithAsyncPersistence(vm1));

    // In two different threads, perform updates to the same key on the same region
    AsyncInvocation doPutsInThread1InVM0 = vm1.invokeAsync(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      for (int i = 0; i < 500; i++) {
        region.put("A", "vm0-" + i);
      }
    });

    AsyncInvocation doPutsInThread2InVM0 = vm1.invokeAsync(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      for (int i = 0; i < 500; i++) {
        region.put("A", "vm1-" + i);
      }
    });

    // Wait for the update threads to finish.
    doPutsInThread1InVM0.await();
    doPutsInThread2InVM0.await();

    // Make sure the async queue is flushed to disk
    vm1.invoke(() -> {
      DiskStore diskStore = getCache().findDiskStore(regionName);
      diskStore.flush();
    });

    // Assert that the disk has seen all of the updates
    RegionVersionVector rvv = getRVV(vm1);
    RegionVersionVector diskRVV = getDiskRVV(vm1);
    assertSameRVV(rvv, diskRVV);

    // Bounce the cache and make the same assertion
    closeCache(vm1);

    vm1.invoke(() -> createRegionWithAsyncPersistence(vm1));

    // Assert that the recovered RVV is the same as before the restart
    RegionVersionVector rvv2 = getRVV(vm1);
    assertSameRVV(rvv, rvv2);

    // The disk RVV should also match.
    RegionVersionVector diskRVV2 = getDiskRVV(vm1);
    assertSameRVV(rvv2, diskRVV2);
  }

  /**
   * Test that when we generate a krf, we write the version tag that matches the entry in the crf.
   */
  @Test
  public void testWriteCorrectVersionToKrf() throws Exception {
    VM vm1 = getVM(1);

    InternalRegion region = (InternalRegion) createAsyncRegionWithSmallQueue(vm1);

    // The idea here is to do a bunch of puts with async persistence
    // At some point the oplog will switch. At that time, we wait for a krf
    // to be created and then throw an exception to shutdown the disk store.
    //
    // This should cause us to create a krf with some entries that have been
    // modified since the crf was written and are still in the async queue.
    //
    // To avoid deadlocks, we need to mark that the oplog was switched,
    // and then do the wait in the flusher thread.

    // Setup the callbacks to wait for krf creation and throw an exception
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    try (IgnoredException e = addIgnoredException(DiskAccessException.class)) {
      CountDownLatch krfCreated = new CountDownLatch(1);
      AtomicBoolean oplogSwitched = new AtomicBoolean(false);

      CacheObserverHolder.setInstance(new CacheObserverAdapter() {

        @Override
        public void afterKrfCreated() {
          krfCreated.countDown();
        }

        @Override
        public void afterWritingBytes() {
          if (oplogSwitched.get()) {
            errorCollector
                .checkSucceeds(() -> assertThat(krfCreated.await(3_000, SECONDS)).isTrue());

            throw new DiskAccessException();
          }
        }

        @Override
        public void afterSwitchingOplog() {
          oplogSwitched.set(true);
        }
      });

      // This is just to make sure the first oplog is not completely garbage.
      region.put("testkey", "key");

      // Do some puts to trigger an oplog roll.
      try {
        // Starting with a value of 1 means the value should match
        // the region version for easier debugging.
        int i = 1;
        while (krfCreated.getCount() > 0) {
          i++;
          region.put("key" + (i % 3), i);
          sleep(2);
        }
      } catch (CacheClosedException | DiskAccessException expected) {
        // do nothing
      }

      // Wait for the region to be destroyed. The region won't be destroyed
      // until the async flusher thread ends up switching oplogs
      await().until(() -> region.isDestroyed());
      closeCache();
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(null);
    }

    // Get the version tags from the krf
    InternalRegion recoveredRegion = (InternalRegion) createAsyncRegionWithSmallQueue(vm1);
    VersionTag[] tagsFromKrf = new VersionTag[3];
    for (int i = 0; i < 3; i++) {
      NonTXEntry entry = (NonTXEntry) recoveredRegion.getEntry("key" + i);
      tagsFromKrf[i] = entry.getRegionEntry().getVersionStamp().asVersionTag();
      LogWriterUtils.getLogWriter()
          .info("krfTag[" + i + "]=" + tagsFromKrf[i] + ",value=" + entry.getValue());
    }

    closeCache();

    // Set a system property to skip recovering from the krf so we can
    // get the version tag from the crf.
    System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "true");

    // Get the version tags from the crf
    recoveredRegion = (InternalRegion) createAsyncRegionWithSmallQueue(vm1);
    VersionTag[] tagsFromCrf = new VersionTag[3];
    for (int i = 0; i < 3; i++) {
      NonTXEntry entry = (NonTXEntry) recoveredRegion.getEntry("key" + i);
      tagsFromCrf[i] = entry.getRegionEntry().getVersionStamp().asVersionTag();
      LogWriterUtils.getLogWriter()
          .info("crfTag[" + i + "]=" + tagsFromCrf[i] + ",value=" + entry.getValue());
    }

    // Make sure the version tags from the krf and the crf match.
    for (int i = 0; i < 3; i++) {
      assertThat(tagsFromKrf[i]).isEqualTo(tagsFromCrf[i]);
    }
  }

  private void createRegionWithAsyncPersistence(VM vm) {
    getCache();

    File dir = getDiskDirForVM(vm);
    dir.mkdirs();

    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {dir});
    diskStoreFactory.setMaxOplogSize(1);
    diskStoreFactory.setQueueSize(100);
    diskStoreFactory.setTimeInterval(1000);

    DiskStore diskStore = diskStoreFactory.create(regionName);

    RegionFactory regionFactory = new RegionFactory();
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);
    regionFactory.setDiskSynchronous(false);

    regionFactory.create(regionName);
  }

  private InternalRegion createRegion(VM vm0) {
    getCache();

    File dir = getDiskDirForVM(vm0);
    dir.mkdirs();

    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {dir});
    diskStoreFactory.setMaxOplogSize(1);
    // Turn off automatic compaction
    diskStoreFactory.setAllowForceCompaction(true);
    diskStoreFactory.setAutoCompact(false);
    // The compaction javadocs seem to be wrong. This is the amount of live data in the oplog
    diskStoreFactory.setCompactionThreshold(40);

    DiskStore diskStore = diskStoreFactory.create(regionName);

    RegionFactory regionFactory = new RegionFactory();
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(true);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);

    return (InternalRegion) regionFactory.create(regionName);
  }

  private int getTombstoneCount(InternalRegion region) {
    int regionCount = region.getTombstoneCount();
    int actualCount = 0;
    for (RegionEntry entry : region.getRegionMap().regionEntries()) {
      if (entry.isTombstone()) {
        actualCount++;
      }
    }

    assertThat(actualCount).isEqualTo(regionCount);

    return actualCount;
  }

  private Region createAsyncRegionWithSmallQueue(VM vm0) {
    getCache();

    File dir = getDiskDirForVM(vm0);
    dir.mkdirs();

    DiskStoreFactoryImpl diskStoreFactory =
        (DiskStoreFactoryImpl) getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {dir});
    diskStoreFactory.setMaxOplogSizeInBytes(500);
    diskStoreFactory.setQueueSize(1000);
    diskStoreFactory.setTimeInterval(1000);

    DiskStore diskStore = diskStoreFactory.create(regionName);

    RegionFactory regionFactory = new RegionFactory();
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);
    regionFactory.setDiskSynchronous(false);

    return regionFactory.create(regionName);
  }

  private void deleteKRFs(VM vm) {
    vm.invoke(() -> {
      File file = getDiskDirForVM(vm);
      File[] krfs = file.listFiles((dir, name) -> name.endsWith(".krf"));

      assertThat(krfs.length).isGreaterThan(0);
      for (File krf : krfs) {
        assertThat(krf.delete()).isTrue();
      }
    });
  }

  private void assertSameRVV(RegionVersionVector expectedRVV, RegionVersionVector actualRVV) {
    assertThat(expectedRVV.sameAs(actualRVV))
        .as("Expected " + expectedRVV + " but was " + actualRVV)
        .isTrue();
  }

  private void createData(VM vm, int startKey, int endKey, String value) {
    vm.invoke(() -> {
      Region<Integer, String> region = getCache().getRegion(regionName);

      for (int i = startKey; i < endKey; i++) {
        region.put(i, value);
      }
    });
  }

  private void checkData(VM vm, int startKey, int endKey, String value) {
    vm.invoke(() -> {
      Region<Integer, String> region = getCache().getRegion(regionName);

      for (int i = startKey; i < endKey; i++) {
        assertThat(region.get(i)).as("Value for key " + i).isEqualTo(value);
      }
    });
  }

  protected void delete(VM vm, int startKey, int endKey) {
    vm.invoke(() -> {
      Region<Integer, String> region = getCache().getRegion(regionName);

      for (int i = startKey; i < endKey; i++) {
        region.destroy(i);
      }
    });
  }

  private RegionVersionVector getRVV(VM vm) throws IOException, ClassNotFoundException {
    byte[] result = vm.invoke(() -> {
      InternalRegion region = (InternalRegion) getCache().getRegion(regionName);

      RegionVersionVector rvv = region.getVersionVector();
      rvv = rvv.getCloneForTransmission();
      HeapDataOutputStream hdos = new HeapDataOutputStream(2048);

      // Using gemfire serialization because RegionVersionVector is not java serializable
      DataSerializer.writeObject(rvv, hdos);
      return hdos.toByteArray();
    });

    ByteArrayInputStream bais = new ByteArrayInputStream(result);
    return DataSerializer.readObject(new DataInputStream(bais));
  }

  private RegionVersionVector getDiskRVV(VM vm) throws IOException, ClassNotFoundException {
    byte[] result = vm.invoke(() -> {
      InternalRegion region = (InternalRegion) getCache().getRegion(regionName);

      RegionVersionVector rvv = region.getDiskRegion().getRegionVersionVector();
      rvv = rvv.getCloneForTransmission();
      HeapDataOutputStream hdos = new HeapDataOutputStream(2048);

      // Using gemfire serialization because RegionVersionVector is not java serializable
      DataSerializer.writeObject(rvv, hdos);
      return hdos.toByteArray();
    });

    ByteArrayInputStream bais = new ByteArrayInputStream(result);
    return DataSerializer.readObject(new DataInputStream(bais));
  }
}
