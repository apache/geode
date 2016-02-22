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
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import util.TestException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.CacheObserverAdapter;
import com.gemstone.gemfire.internal.cache.CacheObserverHolder;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.DiskStoreFactoryImpl;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.DiskStoreObserver;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.Token.Tombstone;
import com.gemstone.gemfire.internal.cache.TombstoneService;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

public class PersistentRVVRecoveryDUnitTest extends PersistentReplicatedTestBase {
  
  private static final int TEST_REPLICATED_TOMBSTONE_TIMEOUT = 1000;

  public PersistentRVVRecoveryDUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected final void postTearDownPersistentReplicatedTestBase() throws Exception {
    Invoke.invokeInEveryVM(PersistentRecoveryOrderDUnitTest.class, "resetAckWaitThreshold");
  }
  
  public void testNoConcurrencyChecks () {
    Cache cache = getCache();
    RegionFactory rf = new RegionFactory();
    rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    rf.setConcurrencyChecksEnabled(false);
    try {
      LocalRegion region = (LocalRegion) rf.create(REGION_NAME);
      fail("Expected to get an IllegalStateException because concurrency checks can't be disabled");
    } catch(IllegalStateException expected) {
      //do nothing
    }
  }
  
  /**
   * Test that we can recover the RVV information with some normal
   * usage.
   */
  public void testRecoveryWithKRF() throws Throwable {
    doTestRecovery(new Runnable() {
      @Override
      public void run() {
        //do nothing
      }
    });
  }
  
  /**
   * Test that we can recover the RVV information if the krf is missing
   */
  public void testRecoveryWithoutKRF() throws Throwable {
    doTestRecovery(new Runnable() {
      @Override
      public void run() {
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        VM vm2 = host.getVM(2);
        deleteKRFs(vm0);
      }
    });
  }
  
  /**
   * Test that we correctly recover and expire recovered tombstones, with compaction enabled
   */
  public void testLotsOfTombstones() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);

    //I think we just need to assert the number of tombstones, maybe?
    //Bruce has code that won't even let the tombstones expire for 10 minutes
    //That means on recovery we need to recover them all? Or do we need to recover
    //any? We're doing a GII. Won't we GII tombstones anyway? Ahh, but we need
    //to know that we don't need to record the new tombstones.

    LocalRegion region = createRegion(vm0);

    int initialCount = getTombstoneCount(region);
    assertEquals(0, initialCount);

    final int entryCount = 20;
    for(int i =0 ; i < entryCount; i++) {
      region.put(i, new byte[100]);
      //destroy each entry.
      region.destroy(i);
    }

    assertEquals(entryCount, getTombstoneCount(region));


    //roll to a new oplog
    region.getDiskStore().forceRoll();
    
    //Force a compaction. This should do nothing, because
    //The tombstones are not garbage, so only 50% of the oplog
    //is garbage (the creates).
    region.getDiskStore().forceCompaction();
    
    assertEquals(0, region.getDiskStore().numCompactableOplogs());

    assertEquals(entryCount, getTombstoneCount(region));

    getCache().close();

    region = createRegion(vm0);

    assertEquals(entryCount, getTombstoneCount(region));

    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    TombstoneService tombstoneService = cache.getTombstoneService();

    //Before expiring tombstones, no oplogs are available for compaction
    assertEquals(0, region.getDiskStore().numCompactableOplogs());
    region.getDiskStore().forceCompaction();
    assertTrue(tombstoneService.forceBatchExpirationForTests(entryCount/2));
    
    assertEquals(entryCount/2, getTombstoneCount(region));
    
    //After expiring, we should have an oplog available for compaction.
    assertEquals(1, region.getDiskStore().numCompactableOplogs());

    //Test after restart the tombstones are still missing
    getCache().close();
    region = createRegion(vm0);
    assertEquals(entryCount/2, getTombstoneCount(region));

    //We should have an oplog available for compaction, because the tombstones 
    //were garbage collected
    assertEquals(1, region.getDiskStore().numCompactableOplogs());
    //This should compact some oplogs
    region.getDiskStore().forceCompaction();
    assertEquals(0, region.getDiskStore().numCompactableOplogs());
    
    //Restart again, and make sure the compaction didn't mess up our tombstone
    //count
    getCache().close();
    region = createRegion(vm0);
    assertEquals(entryCount/2, getTombstoneCount(region));
    cache = (GemFireCacheImpl) getCache();
    
    //Add a test hook that will shutdown the system as soon as we write a GC RVV record
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      @Override
      public void afterWriteGCRVV(DiskRegion dr) {
        //This will cause the disk store to shut down,
        //preventing us from writing any other records.
        throw new DiskAccessException();
      }
      
    });
    IgnoredException ex = IgnoredException.addIgnoredException("DiskAccessException");
    try {
      //Force expiration, with our test hook that should close the cache
      tombstoneService = cache.getTombstoneService();
      tombstoneService.forceBatchExpirationForTests(entryCount/4);

      getCache().close();
      assertTrue(cache.isClosed());

      //Restart again, and make sure the tombstones are in fact removed
      region = createRegion(vm0);
      assertEquals(entryCount/4, getTombstoneCount(region));
    } finally {
      ex.remove();
    }
    
    
  }

  /**
   * Test that we correctly recover and expire recovered tombstones, with
   * compaction enabled.
   * 
   * This test differs from above test in that we need to make sure tombstones
   * start expiring based on their original time-stamp, NOT the time-stamp
   * assigned during scheduling for expiration after recovery.
   */
  public void DISABLED_testLotsOfTombstonesExpiration() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    
    vm0.invoke(new CacheSerializableRunnable("") {
      
      @Override
      public void run2() throws CacheException {
        // TODO Auto-generated method stub
        long replicatedTombstoneTomeout = TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT;
        long expiriredTombstoneLimit = TombstoneService.EXPIRED_TOMBSTONE_LIMIT;
        
        try {
          LocalRegion region = createRegion(vm0);

          int initialCount = getTombstoneCount(region);
          assertEquals(0, initialCount);

          final int entryCount = 20;
          for (int i = 0; i < entryCount; i++) {
            region.put(i, new byte[100]);
            // destroy each entry.
            region.destroy(i);
          }

          assertEquals(entryCount, getTombstoneCount(region));

          // roll to a new oplog
          region.getDiskStore().forceRoll();

          // Force a compaction. This should do nothing, because
          // The tombstones are not garbage, so only 50% of the oplog
          // is garbage (the creates).
          region.getDiskStore().forceCompaction();

          assertEquals(0, region.getDiskStore().numCompactableOplogs());

          assertEquals(entryCount, getTombstoneCount(region));

          getCache().close();

          // We should wait for timeout time so that tomstones are expired
          // right away when they are gIId based on their original timestamp.
          Wait.pause((int) TEST_REPLICATED_TOMBSTONE_TIMEOUT);

          TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT = TEST_REPLICATED_TOMBSTONE_TIMEOUT;
          TombstoneService.EXPIRED_TOMBSTONE_LIMIT = entryCount;
          // Do region GII
          region = createRegion(vm0);

          assertEquals(entryCount, getTombstoneCount(region));

          getCache().getLogger().fine("Waiting for maximumSleepTime ms");
          Wait.pause(10000); // maximumSleepTime+500 in TombstoneSweeper GC thread

          // Tombstones should have been expired and garbage collected by now by
          // TombstoneService.
          assertEquals(0, getTombstoneCount(region));

          // This should compact some oplogs
          region.getDiskStore().forceCompaction();
          assertEquals(0, region.getDiskStore().numCompactableOplogs());

          // Test after restart the tombstones are still missing
          getCache().close();
          region = createRegion(vm0);
          assertEquals(0, getTombstoneCount(region));

          // We should have an oplog available for compaction, because the
          // tombstones
          // were garbage collected
          assertEquals(0, region.getDiskStore().numCompactableOplogs());

          GemFireCacheImpl cache = (GemFireCacheImpl) getCache();

          cache.close();
        } finally {
          TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT = replicatedTombstoneTomeout;
          TombstoneService.EXPIRED_TOMBSTONE_LIMIT = expiriredTombstoneLimit;
        }    
      }
    });
  }

  /**
   * This test creates 2 VMs in a distributed system with a persistent
   * PartitionedRegion and one VM (VM1) puts an entry in region. Second VM (VM2)
   * starts later and does a delta GII. During Delta GII in VM2 a DESTROY
   * operation happens in VM1 and gets propagated to VM2 concurrently with GII.
   * At this point if entry version is greater than the once received from GII
   * then it must not get applied. Which is Bug #45921.
   *
   * @author shobhit
   */
  public void testConflictChecksDuringConcurrentDeltaGIIAndOtherOp() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(new CacheSerializableRunnable("Create PR and put an entry") {
      
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        
        PartitionAttributes attrs = new PartitionAttributesFactory()
            .setRedundantCopies(1).setTotalNumBuckets(1).create();
        AttributesFactory factory = new AttributesFactory();
        factory.setPartitionAttributes(attrs);
        RegionAttributes rAttrs = factory.create();
        Region region = cache.createRegionFactory(rAttrs).create("prRegion");

        region.put("testKey", "testValue");
        assertEquals(1, region.size());
      }
    });

    // Create a cache and region, do an update to change the version no. and
    // restart the cache and region.
    vm1.invoke(new CacheSerializableRunnable("Create PR and put an entry") {

      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        PartitionAttributes attrs = new PartitionAttributesFactory()
            .setRedundantCopies(1).setTotalNumBuckets(1).create();
        AttributesFactory factory = new AttributesFactory();
        factory.setPartitionAttributes(attrs);
        RegionAttributes rAttrs = factory.create();
        Region region = cache.createRegionFactory(rAttrs).create("prRegion");
        region.put("testKey", "testValue2");
        cache.close();
        
        //Restart
        cache = getCache();
        region = cache.createRegionFactory(rAttrs).create("prRegion");
      }
    });
    
    // Do a DESTROY in vm0 when delta GII is in progress in vm1 (Hopefully, Not
    // guaranteed).
    AsyncInvocation async = vm0.invokeAsync(new CacheSerializableRunnable("Detroy entry in region") {
      
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        Region region = cache.getRegion("prRegion");
        while (!region.get("testKey").equals("testValue2")) {
          Wait.pause(100);
        }
        region.destroy("testKey");
      }
    });

    try {
      async.join(3000);
    } catch (InterruptedException e) {
      new TestException("VM1 entry destroy did not finish in 3000 ms");
    }

    vm1.invoke(new CacheSerializableRunnable("Verifying entry version in new node VM1") {
      
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        Region region = cache.getRegion("prRegion");
        
        Region.Entry entry = ((PartitionedRegion)region).getEntry("testKey", true /*Entry is destroyed*/);
        RegionEntry re = ((EntrySnapshot)entry).getRegionEntry();
        LogWriterUtils.getLogWriter().fine("RegionEntry for testKey: " + re.getKey() + " " + re.getValueInVM((LocalRegion) region));
        assertTrue(re.getValueInVM((LocalRegion) region) instanceof Tombstone);
        
        VersionTag tag = re.getVersionStamp().asVersionTag();
        assertEquals(3 /*Two puts and a Destroy*/, tag.getEntryVersion());
      }
    });

    closeCache(vm0);
    closeCache(vm1);
  }

  private LocalRegion createRegion(final VM vm0) {
    Cache cache = getCache();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File dir = getDiskDirForVM(vm0);
    dir.mkdirs();
    dsf.setDiskDirs(new File[] {dir});
    dsf.setMaxOplogSize(1);
    //Turn of automatic compaction
    dsf.setAllowForceCompaction(true);
    dsf.setAutoCompact(false);
    //The compaction javadocs seem to be wrong. This
    //is the amount of live data in the oplog
    dsf.setCompactionThreshold(40);
    DiskStore ds = dsf.create(REGION_NAME);
    RegionFactory rf = new RegionFactory();
    rf.setDiskStoreName(ds.getName());
    rf.setDiskSynchronous(true);
    rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    rf.setScope(Scope.DISTRIBUTED_ACK);
    LocalRegion region = (LocalRegion) rf.create(REGION_NAME);
    return region;
  }

  private int getTombstoneCount(LocalRegion region) {
    int regionCount = region.getTombstoneCount();
    int actualCount = 0;
    for(RegionEntry entry : region.entries.regionEntries()) {
      if(entry.isTombstone()) {
        actualCount++;
      }
    }

    assertEquals(actualCount, regionCount);

    return actualCount;
  }
  
  
  public void doTestRecovery(Runnable doWhileOffline) throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    //Create the region in few members to test recovery
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    createPersistentRegion(vm2);
    
    //Create and delete some keys (to update the RVV)
    createData(vm0, 0,5, "value1");
    createData(vm1, 3,8, "value2");
    createData(vm2, 6,11, "value3");
    
    delete(vm1, 0,1);
    delete(vm0, 10,11);
    
    
    //Make sure the RVVs are the same in the members
    RegionVersionVector vm0RVV = getRVV(vm0);
    RegionVersionVector vm1RVV = getRVV(vm1);
    RegionVersionVector vm2RVV = getRVV(vm2);
    
    
    assertSameRVV(vm0RVV, vm1RVV);
    assertSameRVV(vm0RVV, vm2RVV);
    
    //Closing the cache will ensure the disk store is closed
    closeCache(vm2);
    closeCache(vm1);
    closeCache(vm0);

    doWhileOffline.run();
    
    //Make sure we can recover the RVV
    createPersistentRegion(vm0);
    
    RegionVersionVector new0RVV = getRVV(vm0);
    assertSameRVV(vm0RVV, new0RVV);
    assertEquals(vm0RVV.getOwnerId(), new0RVV.getOwnerId());
    
    createData(vm0, 12, 15, "value");
    
    //Make sure we can GII the RVV
    new0RVV = getRVV(vm0);
    assertSameRVV(new0RVV, getDiskRVV(vm0));
    createPersistentRegion(vm1);
    assertSameRVV(new0RVV, getRVV(vm1));
    assertSameRVV(new0RVV, getDiskRVV(vm1));
    
    closeCache(vm0);
    closeCache(vm1);
    
    doWhileOffline.run();
    
    //Make the sure member that GII'd the RVV can also recover it
    createPersistentRegion(vm1);
    RegionVersionVector new1RVV = getRVV(vm1);
    assertSameRVV(new0RVV, getRVV(vm1));
  }
  
  /**
   * Test that we skip conflict checks with entries that are on
   * disk compared to entries that come in as part of a GII
   */
  public void testSkipConflictChecksForGIIdEntries() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    //Create the region in few members to test recovery
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    
    //Create an update some entries in vm0 and vm1.
    createData(vm0, 0,1, "value1");
    createData(vm0, 0,2, "value2");
    
    closeCache(vm1);

    //Reset the entry version in vm0.
    //This means that if we did a conflict check, vm0's key will have
    //a lower entry version than vm1, which would cause us to prefer vm1's
    //value
    SerializableRunnable createData = new SerializableRunnable("rollEntryVersion") {
      
      public void run() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion(REGION_NAME);
        region.put(0, "value3");
        RegionEntry entry = region.getRegionEntry(0);
        entry = region.getRegionEntry(0);
        //Sneak in and change the version number for an entry to generate
        //a conflict.
        VersionTag tag = entry.getVersionStamp().asVersionTag();
        tag.setEntryVersion(tag.getEntryVersion() - 2);
        entry.getVersionStamp().setVersions(tag);
      }
    };
    
    vm0.invoke(createData);
    
    //Create vm1, doing a GII
    createPersistentRegion(vm1);

    checkData(vm0, 0, 1, "value3");
    //If we did a conflict check, this would be value2
    checkData(vm1, 0, 1, "value3");
  }
  
  /**
   * Test that we skip conflict checks with entries that are on
   * disk compared to entries that come in as part of a concurrent operation
   */
  public void testSkipConflictChecksForConcurrentOps() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    //Create the region in few members to test recovery
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    
    //Create an update some entries in vm0 and vm1.
    createData(vm0, 0,1, "value1");
    createData(vm0, 0,1, "value2");
    createData(vm0, 0,1, "value2");
    
    closeCache(vm1);

    //Update the keys  in vm0 until the entry version rolls over.
    //This means that if we did a conflict check, vm0's key will have
    //a lower entry version than vm1, which would cause us to prefer vm1's
    //value
    SerializableRunnable createData = new SerializableRunnable("rollEntryVersion") {
      
      public void run() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion(REGION_NAME);
        region.put(0, "value3");
        RegionEntry entry = region.getRegionEntry(0);
        entry = region.getRegionEntry(0);
        //Sneak in and change the version number for an entry to generate
        //a conflict.
        VersionTag tag = entry.getVersionStamp().asVersionTag();
        tag.setEntryVersion(tag.getEntryVersion() - 2);
        entry.getVersionStamp().setVersions(tag); 
      }
    };
    vm0.invoke(createData);

    //Add an observer to vm0 which will perform a concurrent operation during
    //the GII. If we do a conflict check, this operation will be rejected
    //because it will have a lower entry version that what vm1 recovered from
    //disk
    vm0.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage msg) {
            if(msg instanceof InitialImageOperation.RequestImageMessage) {
              if(((InitialImageOperation.RequestImageMessage) msg).regionPath.contains(REGION_NAME)) {
                createData(vm0, 0, 1, "value4");
                DistributionMessageObserver.setInstance(null);
            }
          }
          } 
          
        });
      }
    });
    
    //Create vm1, doing a GII
    createPersistentRegion(vm1);

    //If we did a conflict check, this would be value2
    checkData(vm0, 0, 1, "value4");
    checkData(vm1, 0, 1, "value4");
  }

  /**
   * Test that with concurrent updates to an async disk region,
   * we correctly update the RVV On disk
   */
  public void testUpdateRVVWithAsyncPersistence() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(1);
    
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File dir = getDiskDirForVM(vm0);
        dir.mkdirs();
        dsf.setDiskDirs(new File[] {dir});
        dsf.setMaxOplogSize(1);
        dsf.setQueueSize(100);
        dsf.setTimeInterval(1000);
        DiskStore ds = dsf.create(REGION_NAME);
        RegionFactory rf = new RegionFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.setDiskSynchronous(false);
        rf.create(REGION_NAME);
      }
    };
    
    //Create a region with async persistence
    vm0.invoke(createRegion);
    
    //In two different threads, perform updates to the same key on the same region
    AsyncInvocation ins0 = vm0.invokeAsync(new SerializableRunnable("change the entry at vm0") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        for (int i=0; i<500; i++) {
          region.put("A", "vm0-"+i);
        }
      }
    });
    AsyncInvocation ins1 = vm0.invokeAsync(new SerializableRunnable("change the entry at vm1") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        for (int i=0; i<500; i++) {
          region.put("A", "vm1-"+i);
        }
      }
    });
    
    //Wait for the update threads to finish.
    ins0.getResult(MAX_WAIT);
    ins1.getResult(MAX_WAIT);

    //Make sure the async queue is flushed to disk
    vm0.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        DiskStore ds = cache.findDiskStore(REGION_NAME);
        ds.flush();
      }
    });
    
    //Assert that the disk has seen all of the updates
    RegionVersionVector rvv = getRVV(vm0);
    RegionVersionVector diskRVV = getDiskRVV(vm0);
    assertSameRVV(rvv, diskRVV);

    //Bounce the cache and make the same assertion
    closeCache(vm0);
    vm0.invoke(createRegion);
    
    //Assert that the recovered RVV is the same as before the restart
    RegionVersionVector rvv2 = getRVV(vm0);
    assertSameRVV(rvv, rvv2);
    
    //The disk RVV should also match.
    RegionVersionVector diskRVV2 = getDiskRVV(vm0);
    assertSameRVV(rvv2, diskRVV2);
  }
  
  /**
   * Test that when we generate a krf, we write the version tag
   * that matches the entry in the crf.
   */
  public void testWriteCorrectVersionToKrf() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(1);
    
    final LocalRegion region = (LocalRegion) createAsyncRegionWithSmallQueue(vm0);

    
    //The idea here is to do a bunch of puts with async persistence
    //At some point the oplog will switch. At that time, we wait for a krf
    //to be created and then throw an exception to shutdown the disk store.
    //
    //This should cause us to create a krf with some entries that have been
    //modified since the crf was written and are still in the async queue.
    //
    //To avoid deadlocks, we need to mark that the oplog was switched,
    //and then do the wait in the flusher thread.
    
    //Setup the callbacks to wait for krf creation and throw an exception
    IgnoredException ex = IgnoredException.addIgnoredException("DiskAccessException");
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=true;
    try {
      final CountDownLatch krfCreated = new CountDownLatch(1);
      final AtomicBoolean oplogSwitched = new AtomicBoolean(false);
      CacheObserverHolder.setInstance(new CacheObserverAdapter() {


        @Override
        public void afterKrfCreated() {
          krfCreated.countDown();
        }
        

        @Override
        public void afterWritingBytes() {
          if(oplogSwitched.get()) {
            try {
              if(!krfCreated.await(3000, TimeUnit.SECONDS)) {
                fail("KRF was not created in 30 seconds!");
              }
            } catch (InterruptedException e) {
              fail("interrupted");
            }

            //Force a failure
            throw new DiskAccessException();
          }
        }



        @Override
        public void afterSwitchingOplog() {
          oplogSwitched.set(true);
        }
      });

      //This is just to make sure the first oplog is not completely garbage.

      region.put("testkey", "key");
      //Do some puts to trigger an oplog roll.
      try {
        //Starting with a value of 1 means the value should match
        //the region version for easier debugging.
        int i = 1;
        while(krfCreated.getCount() > 0) {
          i++;
          region.put("key" + (i % 3), i);
          Thread.sleep(2);
        }
      } catch(CacheClosedException expected) {
        //do nothing
      }

      //Wait for the region to be destroyed. The region won't be destroyed
      //until the async flusher thread ends up switching oplogs
      Wait.waitForCriterion(new WaitCriterion() {

        @Override
        public boolean done() {
          return region.isDestroyed();
        }

        @Override
        public String description() {
          return "Region was not destroyed : " + region.isDestroyed();
        }
      }, 3000 * 1000, 100, true);
      closeCache();
    } finally {
      ex.remove();
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=false;
      CacheObserverHolder.setInstance(null);
    }
    
    
    
    //Get the version tags from the krf 
    LocalRegion recoveredRegion = (LocalRegion) createAsyncRegionWithSmallQueue(vm0);
    VersionTag[] tagsFromKrf = new VersionTag[3];
    for(int i = 0; i < 3; i++) {
      NonTXEntry entry = (NonTXEntry) recoveredRegion.getEntry("key" + i);
      tagsFromKrf[i] = entry.getRegionEntry().getVersionStamp().asVersionTag();
      LogWriterUtils.getLogWriter().info("krfTag[" + i + "]="+ tagsFromKrf[i] + ",value=" + entry.getValue());
    }
    
    closeCache();

    //Set a system property to skip recovering from the krf so we can
    //get the version tag from the crf.
    System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "true");
    try {
      //Get the version tags from the crf
      recoveredRegion = (LocalRegion) createAsyncRegionWithSmallQueue(vm0);
      VersionTag[] tagsFromCrf = new VersionTag[3];
      for(int i = 0; i < 3; i++) {
        NonTXEntry entry = (NonTXEntry) recoveredRegion.getEntry("key" + i);
        tagsFromCrf[i] = entry.getRegionEntry().getVersionStamp().asVersionTag();
        LogWriterUtils.getLogWriter().info("crfTag[" + i + "]="+ tagsFromCrf[i] + ",value=" + entry.getValue());
      }
      
      //Make sure the version tags from the krf and the crf match.
      for(int i = 0; i < 3; i++) {
        assertEquals(tagsFromCrf[i], tagsFromKrf[i]);
      }
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "false");
    }
  }

  private Region createAsyncRegionWithSmallQueue(final VM vm0) {
    Cache cache = getCache();
    DiskStoreFactoryImpl dsf = (DiskStoreFactoryImpl) cache.createDiskStoreFactory();
    File dir = getDiskDirForVM(vm0);
    dir.mkdirs();
    dsf.setDiskDirs(new File[] {dir});
    dsf.setMaxOplogSizeInBytes(500);
    dsf.setQueueSize(1000);
    dsf.setTimeInterval(1000);
    DiskStore ds = dsf.create(REGION_NAME);
    RegionFactory rf = new RegionFactory();
    rf.setDiskStoreName(ds.getName());
    rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    rf.setScope(Scope.DISTRIBUTED_ACK);
    rf.setDiskSynchronous(false);
    Region region = rf.create(REGION_NAME);
    return region;
  }
    
  
  private void deleteKRFs(final VM vm0) {
    vm0.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        File file = getDiskDirForVM(vm0);
        File[] krfs = file.listFiles(new FilenameFilter() {
          
          @Override
          public boolean accept(File dir, String name) {
            return name.endsWith(".krf");
          }
        });
        assertTrue(krfs.length > 0);
        for(File krf : krfs) {
          assertTrue(krf.delete());
        }
        
      }
    });
  }
  
  private void assertSameRVV(RegionVersionVector rvv1,
      RegionVersionVector rvv2) {
    if(!rvv1.sameAs(rvv2)) {
      fail("Expected " + rvv1 + " but was " + rvv2);
    }
  }

  protected void createData(VM vm, final int startKey, final int endKey,
    final String value) {
      SerializableRunnable createData = new SerializableRunnable("createData") {
        
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion(REGION_NAME);
          
          for(int i =startKey; i < endKey; i++) {
            region.put(i, value);
          }
        }
      };
      vm.invoke(createData);
    }
  
  protected void checkData(VM vm0, final int startKey, final int endKey,
      final String value) {
        SerializableRunnable checkData = new SerializableRunnable("CheckData") {
          
          public void run() {
            Cache cache = getCache();
            Region region = cache.getRegion(REGION_NAME);
            
            for(int i =startKey; i < endKey; i++) {
              assertEquals("For key " + i, value, region.get(i));
            }
          }
        };
        
        vm0.invoke(checkData);
      }

  protected void delete(VM vm, final int startKey, final int endKey) {
    SerializableRunnable createData = new SerializableRunnable("destroy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);

        for(int i =startKey; i < endKey; i++) {
          region.destroy(i);
        }
      }
    };
    vm.invoke(createData);
  }
  
  protected RegionVersionVector getRVV(VM vm) throws IOException, ClassNotFoundException {
    SerializableCallable createData = new SerializableCallable("getRVV") {

      public Object call() throws Exception {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion(REGION_NAME);
        RegionVersionVector rvv = region.getVersionVector();
        rvv = rvv.getCloneForTransmission();
        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);

        //Using gemfire serialization because 
        //RegionVersionVector is not java serializable
        DataSerializer.writeObject(rvv, hdos);
        return hdos.toByteArray();
      }
    };
    byte[] result= (byte[]) vm.invoke(createData);
    ByteArrayInputStream bais = new ByteArrayInputStream(result);
    return DataSerializer.readObject(new DataInputStream(bais));
  }
  
  protected RegionVersionVector getDiskRVV(VM vm) throws IOException, ClassNotFoundException {
    SerializableCallable createData = new SerializableCallable("getRVV") {
      
      public Object call() throws Exception {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion(REGION_NAME);
        RegionVersionVector rvv = region.getDiskRegion().getRegionVersionVector();
        rvv = rvv.getCloneForTransmission();
        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
        
        //Using gemfire serialization because 
        //RegionVersionVector is not java serializable
        DataSerializer.writeObject(rvv, hdos);
        return hdos.toByteArray();
      }
    };
    byte[] result= (byte[]) vm.invoke(createData);
    ByteArrayInputStream bais = new ByteArrayInputStream(result);
    return DataSerializer.readObject(new DataInputStream(bais));
  }
}
