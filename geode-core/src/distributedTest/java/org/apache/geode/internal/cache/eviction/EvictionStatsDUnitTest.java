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
package org.apache.geode.internal.cache.eviction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.EvictionTest;

@Category({EvictionTest.class})
public class EvictionStatsDUnitTest extends CacheTestCase {

  protected static Cache cache = null;

  protected static VM dataStore1 = null;

  protected static VM dataStore2 = null;

  protected static Region region = null;

  static int maxEntries = 20;

  static int maxSizeInMb = 20;

  static int totalNoOfBuckets = 2;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
  }

  @Test
  public void testEntryLruLimit() {
    // Ignore this exception as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    prepareScenario(EvictionAlgorithm.LRU_ENTRY);
    putData("PR1", 100);
    putData("PR2", 60);
    dataStore1.invoke(new CacheSerializableRunnable("testlimit") {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("PR1");
        final PartitionedRegion pr2 = (PartitionedRegion) cache.getRegion("PR2");
        assertEquals(maxEntries, pr1.getEvictionLimit());
        assertEquals(maxEntries, pr2.getEvictionLimit());
      }
    });

    dataStore2.invoke(new CacheSerializableRunnable("testlimit") {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("PR1");
        final PartitionedRegion pr2 = (PartitionedRegion) cache.getRegion("PR2");
        assertEquals(maxEntries, pr1.getEvictionLimit());
        assertEquals(maxEntries, pr2.getEvictionLimit());
      }
    });
  }

  @Test
  public void testMemLruLimit() {
    // Ignore this exception as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    prepareScenario(EvictionAlgorithm.LRU_MEMORY);
    putData("PR1", 100);
    putData("PR2", 60);
    dataStore1.invoke(new CacheSerializableRunnable("testlimit") {
      @Override
      public void run2() throws CacheException {
        final long ONE_MEG = 1024L * 1024L;
        final PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("PR1");
        final PartitionedRegion pr2 = (PartitionedRegion) cache.getRegion("PR2");
        assertEquals(pr1.getLocalMaxMemory(), pr1.getEvictionLimit() / ONE_MEG);
        assertEquals(pr2.getLocalMaxMemory(), pr2.getEvictionLimit() / ONE_MEG);
      }
    });

    dataStore2.invoke(new CacheSerializableRunnable("testlimit") {
      @Override
      public void run2() throws CacheException {
        final long ONE_MEG = 1024L * 1024L;
        final PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("PR1");
        final PartitionedRegion pr2 = (PartitionedRegion) cache.getRegion("PR2");
        assertEquals(pr1.getLocalMaxMemory(), pr1.getEvictionLimit() / ONE_MEG);
        assertEquals(pr2.getLocalMaxMemory(), pr2.getEvictionLimit() / ONE_MEG);
      }
    });

  }

  @Test
  public void testEntryLruCounter() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    prepareScenario(EvictionAlgorithm.LRU_ENTRY);
    putData("PR1", 10);
    putData("PR2", 16);
    long sizeOfPr1 = getPRCounter("PR1");
    long sizeOfPr2 = getPRCounter("PR2");

    long totalBucketSizeForPR1 = getCounterForBucketsOfPR("PR1");
    long totalBucketSizeForPR2 = getCounterForBucketsOfPR("PR2");

    assertEquals(sizeOfPr1, totalBucketSizeForPR1);
    assertEquals(sizeOfPr2, totalBucketSizeForPR2);
  }

  @Test
  public void testMemLruCounter() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    prepareScenario(EvictionAlgorithm.LRU_MEMORY);
    putData("PR1", 10);
    putData("PR2", 16);
    long sizeOfPr1 = getPRCounter("PR1");
    long sizeOfPr2 = getPRCounter("PR2");

    long totalBucketSizeForPR1 = getCounterForBucketsOfPR("PR1");
    long totalBucketSizeForPR2 = getCounterForBucketsOfPR("PR2");

    assertEquals(sizeOfPr1, totalBucketSizeForPR1);
    assertEquals(sizeOfPr2, totalBucketSizeForPR2);
  }

  @Test
  public void testHeapLruCounter() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    prepareScenario(EvictionAlgorithm.LRU_HEAP);
    System.setProperty(HeapLRUController.TOP_UP_HEAP_EVICTION_PERCENTAGE_PROPERTY,
        Float.toString(0));
    putData("PR1", 20);
    putData("PR2", 20);
    long sizeOfPr1 = getPRCounter("PR1");
    long sizeOfPr2 = getPRCounter("PR2");

    long totalBucketSizeForPR1 = getCounterForBucketsOfPR("PR1");
    long totalBucketSizeForPR2 = getCounterForBucketsOfPR("PR2");
  }

  @Test
  public void testEntryLruAllCounterMethods() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    final long ONE_MEG = 1024L * 1024L;
    createCache();
    createPartitionedRegion(true, EvictionAlgorithm.LRU_ENTRY, "PR1", 2, 1, 10000);
    for (int counter = 1; counter <= maxEntries; counter++) {
      region.put(new Integer(counter), new byte[(1 * 1024 * 1024) - 2]);
    }
    PartitionedRegion pr = (PartitionedRegion) region;
    long sizeOfPRegion = pr.getEvictionCounter();

    assertEquals(sizeOfPRegion, 20);
    long bucketSize = 0;
    for (final Map.Entry<Integer, BucketRegion> regionEntry : ((PartitionedRegion) region)
        .getDataStore()
        .getAllLocalBuckets()) {
      final Map.Entry entry = (Map.Entry) regionEntry;
      final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
      if (bucketRegion == null) {
        continue;
      }
      assertEquals(bucketRegion.getCounter(), 10);
      bucketSize = bucketSize + (bucketRegion.getCounter());
    }
    assertEquals(sizeOfPRegion, bucketSize);

    // Cause Eviction.
    bucketSize = 0;
    final int extraEnteries = 4;
    for (int counter = 1; counter <= extraEnteries; counter++) {
      region.put(new Integer(counter), new byte[(1 * 1024 * 1024) - 2]);
    }
    sizeOfPRegion = pr.getEvictionCounter();
    assertEquals(sizeOfPRegion, 20);
    for (final Map.Entry<Integer, BucketRegion> bucketRegionEntry : pr.getDataStore()
        .getAllLocalBuckets()) {
      final Map.Entry entry = (Map.Entry) bucketRegionEntry;
      final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
      if (bucketRegion == null) {
        continue;
      }
      assertEquals(bucketRegion.getCounter(), 10);
      bucketSize = bucketSize + (bucketRegion.getCounter());
    }
    assertEquals(sizeOfPRegion, bucketSize);

    // Clear one bucket
    for (final Map.Entry<Integer, BucketRegion> integerBucketRegionEntry : pr.getDataStore()
        .getAllLocalBuckets()) {
      final Map.Entry entry = (Map.Entry) integerBucketRegionEntry;
      final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
      if (bucketRegion == null) {
        continue;
      }
      // Don't just clear the bucket here, use the real region
      // api to remove the entries.
      for (Object key : bucketRegion.keySet()) {
        region.destroy(key);
      }
      assertEquals(bucketRegion.getCounter(), 0);
      break;
    }
    sizeOfPRegion = pr.getEvictionCounter();
    assertEquals(sizeOfPRegion, 10);
  }



  @Test
  public void testEntryLRUEvictionNDestroyNNumOverflowOnDiskCount() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    final int extraEntries = 24;
    prepareScenario(EvictionAlgorithm.LRU_ENTRY);
    putData("PR1", maxEntries + extraEntries);
    putData("PR2", maxEntries + extraEntries);
    dataStore1.invoke(
        new CacheSerializableRunnable("testEntryLRUEvictionNDestroyNNumOverflowOnDiskCount") {
          @Override
          public void run2() throws CacheException {
            PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("PR1");
            assertEquals(pr1.getTotalEvictions(), (extraEntries - maxEntries) / 2);
            assertEquals(pr1.getEvictionDestroys(), pr1.getTotalEvictions());

            PartitionedRegion pr2 = (PartitionedRegion) cache.getRegion("PR2");
            assertEquals(pr2.getTotalEvictions(), (extraEntries - maxEntries) / 2);
            assertEquals(pr2.getEvictionDestroys(), 0);
            assertEquals(pr2.getDiskRegionStats().getNumOverflowOnDisk(),
                (extraEntries - maxEntries) / 2);
          }
        });

    dataStore2.invoke(
        new CacheSerializableRunnable("testEntryLRUEvictionNDestroyNNumOverflowOnDiskCount") {
          @Override
          public void run2() throws CacheException {
            PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("PR1");
            assertEquals(pr1.getTotalEvictions(), (extraEntries - maxEntries) / 2);
            assertEquals(pr1.getEvictionDestroys(), pr1.getTotalEvictions());

            PartitionedRegion pr2 = (PartitionedRegion) cache.getRegion("PR2");
            assertEquals(pr2.getTotalEvictions(), (extraEntries - maxEntries) / 2);
            assertEquals(pr2.getEvictionDestroys(), 0);
            assertEquals(pr2.getDiskRegionStats().getNumOverflowOnDisk(),
                (extraEntries - maxEntries) / 2);
          }
        });
  }

  @Test
  public void testMemLRUEvictionNDestroyNNumOverflowOnDiskCount() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    int localMaxMem = 50;
    final int extraEntries = 6;
    prepareScenario(EvictionAlgorithm.LRU_MEMORY);
    putData("PR1", (2 * localMaxMem) + extraEntries);
    putData("PR2", (2 * localMaxMem/* localmaxmem */) + extraEntries);
    dataStore1.invoke(new CacheSerializableRunnable("testEvictionCount") {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("PR1");
        LogWriterUtils.getLogWriter().info("dddd  local" + pr1.getLocalMaxMemory());
        LogWriterUtils.getLogWriter().info("dddd  local evi" + pr1.getTotalEvictions());
        LogWriterUtils.getLogWriter()
            .info("dddd  local entries" + (pr1.getEvictionCounter() / (1024 * 1024)));
        HeapMemoryMonitor hmm =
            ((InternalResourceManager) cache.getResourceManager()).getHeapMonitor();
        long memused = hmm.getBytesUsed() / (1024 * 1024);
        LogWriterUtils.getLogWriter().info("dddd  local memused= " + memused);
        assertTrue(pr1.getTotalEvictions() >= extraEntries / 2);
        assertEquals(pr1.getEvictionDestroys(), pr1.getTotalEvictions());
        PartitionedRegion pr2 = (PartitionedRegion) cache.getRegion("PR2");
        assertTrue(pr2.getTotalEvictions() >= extraEntries / 2);
        assertEquals(pr2.getEvictionDestroys(), 0);
        assertTrue(pr2.getDiskRegionStats().getNumOverflowOnDisk() >= extraEntries / 2);
      }
    });

    dataStore2.invoke(new CacheSerializableRunnable("testEvictionCount") {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("PR1");
        assertTrue(pr1.getTotalEvictions() >= extraEntries / 2);
        assertEquals(pr1.getEvictionDestroys(), pr1.getTotalEvictions());

        PartitionedRegion pr2 = (PartitionedRegion) cache.getRegion("PR2");
        assertTrue(pr2.getTotalEvictions() >= extraEntries / 2);
        assertEquals(pr2.getEvictionDestroys(), 0);
        assertTrue(pr2.getDiskRegionStats().getNumOverflowOnDisk() >= extraEntries / 2);
      }
    });
  }


  public void prepareScenario(EvictionAlgorithm evictionAlgorithm) {
    createCacheInAllVms();
    createPartitionedRegionInAllVMS(true, evictionAlgorithm, "PR1", totalNoOfBuckets, 1, 10000);
    createPartitionedRegionInAllVMS(true, evictionAlgorithm, "PR2", totalNoOfBuckets, 2, 10000);
  }

  public void createCacheInAllVms() {
    createCache();

    dataStore1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createCache();
      }
    });

    dataStore2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createCache();
      }
    });
  }

  public static void createCacheInVm() {
    new EvictionStatsDUnitTest().createCache();
  }

  public void createCache() {
    try {
      Properties props = new Properties();
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
      LogWriterUtils.getLogWriter().info("cache= " + cache);
      LogWriterUtils.getLogWriter().info("cache closed= " + cache.isClosed());
      cache.getResourceManager().setEvictionHeapPercentage(20);
    } catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }

  private void createPartitionedRegionInAllVMS(final boolean setEvictionOn,
      final EvictionAlgorithm evictionAlgorithm, final String regionName,
      final int totalNoOfBuckets, final int evictionAction, final int evictorInterval) {

    dataStore1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(setEvictionOn, evictionAlgorithm, regionName, totalNoOfBuckets,
            evictionAction, evictorInterval);
      }
    });

    dataStore2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(setEvictionOn, evictionAlgorithm, regionName, totalNoOfBuckets,
            evictionAction, evictorInterval);
      }
    });
  }

  public void createPartitionedRegion(boolean setEvictionOn, EvictionAlgorithm evictionAlgorithm,
      String regionName, int totalNoOfBuckets, int evictionAction, int evictorInterval) {

    final AttributesFactory factory = new AttributesFactory();
    factory.setOffHeap(isOffHeapEnabled());
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory()
        .setRedundantCopies(0).setTotalNumBuckets(totalNoOfBuckets).setLocalMaxMemory(50);

    factory.setPartitionAttributes(partitionAttributesFactory.create());
    if (setEvictionOn) {
      if (evictionAlgorithm.isLRUHeap()) {
        factory.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null,
            evictionAction == 1 ? EvictionAction.LOCAL_DESTROY : EvictionAction.OVERFLOW_TO_DISK));
      } else if (evictionAlgorithm.isLRUMemory()) {
        factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(maxSizeInMb,
            ObjectSizer.DEFAULT,
            evictionAction == 1 ? EvictionAction.LOCAL_DESTROY : EvictionAction.OVERFLOW_TO_DISK));
      } else {
        factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries,
            evictionAction == 1 ? EvictionAction.LOCAL_DESTROY : EvictionAction.OVERFLOW_TO_DISK));
      }
      if (evictionAction == 2) {
        final File[] diskDirs = new File[1];
        diskDirs[0] =
            new File("Partitioned_Region_EvictionStats/" + "LogFile" + "_" + OSProcess.getId());
        diskDirs[0].mkdirs();
        factory.setDiskSynchronous(true);
        factory.setDiskStoreName(cache.createDiskStoreFactory().setDiskDirs(diskDirs)
            .create("EvictionStatsDUnitTest").getName());
      }
    }

    region = cache.createRegion(regionName, factory.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info("Partitioned Region created Successfully :" + region);
  }

  public static void putData(final String regionName, final int noOfElememts) {
    dataStore1.invoke(new CacheSerializableRunnable("putData") {
      @Override
      public void run2() throws CacheException {
        final Region pr = cache.getRegion(regionName);
        for (int counter = 1; counter <= noOfElememts; counter++) {
          pr.put(new Integer(counter), new byte[1 * 1024 * 1024]);
          // getLogWriter().info("Deep put data element no->" + counter);
        }
      }
    });
  }

  private long getPRCounter(String prRegionName) {
    final long ONE_MEG = 1024L * 1024L;
    long sizeOfPR = 0;
    sizeOfPR = sizeOfPR + dataStore1
        .invoke(() -> EvictionStatsDUnitTest.getPartionRegionCounter(prRegionName));
    sizeOfPR = sizeOfPR + dataStore2
        .invoke(() -> EvictionStatsDUnitTest.getPartionRegionCounter(prRegionName));
    return sizeOfPR / ONE_MEG;
  }

  public static long getPartionRegionCounter(String prRegionName) {
    final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(prRegionName);
    return pr.getEvictionCounter();
  }

  private long getCounterForBucketsOfPR(String prRegionName) {
    final long ONE_MEG = 1024L * 1024L;
    long totalBucketSize = 0;
    totalBucketSize = totalBucketSize
        + dataStore1.invoke(() -> EvictionStatsDUnitTest.getCounterForBuckets(prRegionName));
    totalBucketSize = totalBucketSize
        + dataStore2.invoke(() -> EvictionStatsDUnitTest.getCounterForBuckets(prRegionName));
    return totalBucketSize / ONE_MEG;

  }

  public static long getCounterForBuckets(String prRegionName) {
    long bucketSize = 0;
    final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(prRegionName);
    for (final Map.Entry<Integer, BucketRegion> integerBucketRegionEntry : pr.getDataStore()
        .getAllLocalBuckets()) {
      final Map.Entry entry = (Map.Entry) integerBucketRegionEntry;
      final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
      if (bucketRegion == null) {
        continue;
      }
      LogWriterUtils.getLogWriter().info("Size of bucket " + bucketRegion.getName() + "of Pr "
          + prRegionName + " = " + bucketRegion.getCounter() / (1000000));
      bucketSize = bucketSize + bucketRegion.getCounter();
    }
    return bucketSize;
  }

  public boolean isOffHeapEnabled() {
    return false;
  }
}
