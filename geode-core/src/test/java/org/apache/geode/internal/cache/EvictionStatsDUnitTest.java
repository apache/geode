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
package org.apache.geode.internal.cache;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizerImpl;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.lru.HeapEvictor;
import org.apache.geode.internal.cache.lru.HeapLRUCapacityController;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

@Category(DistributedTest.class)
public class EvictionStatsDUnitTest extends JUnit4CacheTestCase {

  protected static Cache cache = null;

  protected static VM dataStore1 = null;

  protected static VM dataStore2 = null;

  protected static Region region = null;

  static int maxEnteries = 20;

  static int maxSizeInMb = 20;

  static int totalNoOfBuckets = 2;

  public EvictionStatsDUnitTest() {
    super();
    // TODO Auto-generated constructor stub
  }

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
  }

  @Test
  public void testEntryLruLimitNDestroyLimit() {
    prepareScenario(EvictionAlgorithm.LRU_ENTRY);
    putData("PR1", 100);
    putData("PR2", 60);
    dataStore1.invoke(new CacheSerializableRunnable("testlimit") {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr1 = (PartitionedRegion)cache.getRegion("PR1");
        final PartitionedRegion pr2 = (PartitionedRegion)cache.getRegion("PR2");
        assertEquals(maxEnteries, ((AbstractLRURegionMap)pr1.entries)
            ._getLruList().stats().getLimit());
        assertEquals(maxEnteries, ((AbstractLRURegionMap)pr2.entries)
            ._getLruList().stats().getLimit());

        assertEquals(1000, ((AbstractLRURegionMap)pr1.entries)._getLruList()
            .stats().getDestroysLimit());
        assertEquals(1000, ((AbstractLRURegionMap)pr2.entries)._getLruList()
            .stats().getDestroysLimit());
      }
    });

    dataStore2.invoke(new CacheSerializableRunnable("testlimit") {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr1 = (PartitionedRegion)cache.getRegion("PR1");
        final PartitionedRegion pr2 = (PartitionedRegion)cache.getRegion("PR2");
        assertEquals(maxEnteries, ((AbstractLRURegionMap)pr1.entries)
            ._getLruList().stats().getLimit());
        assertEquals(maxEnteries, ((AbstractLRURegionMap)pr2.entries)
            ._getLruList().stats().getLimit());
        assertEquals(1000, ((AbstractLRURegionMap)pr1.entries)._getLruList()
            .stats().getDestroysLimit());
        assertEquals(1000, ((AbstractLRURegionMap)pr2.entries)._getLruList()
            .stats().getDestroysLimit());
       }
    });
  }

  @Test
  public void testMemLruLimitNDestroyLimit() {
    prepareScenario(EvictionAlgorithm.LRU_MEMORY);
    putData("PR1", 100);
    putData("PR2", 60);
    dataStore1.invoke(new CacheSerializableRunnable("testlimit") {
      @Override
      public void run2() throws CacheException {
        final long ONE_MEG = 1024L * 1024L;
        final PartitionedRegion pr1 = (PartitionedRegion)cache.getRegion("PR1");
        final PartitionedRegion pr2 = (PartitionedRegion)cache.getRegion("PR2");
        assertEquals(pr1.getLocalMaxMemory(), ((AbstractLRURegionMap)pr1.entries)
            ._getLruList().stats().getLimit()
            / ONE_MEG);
        assertEquals(pr2.getLocalMaxMemory(), ((AbstractLRURegionMap)pr2.entries)
            ._getLruList().stats().getLimit()
            / ONE_MEG);
        assertEquals(1000, ((AbstractLRURegionMap)pr1.entries)._getLruList()
            .stats().getDestroysLimit());
        assertEquals(1000, ((AbstractLRURegionMap)pr2.entries)._getLruList()
            .stats().getDestroysLimit());
       }
    });

    dataStore2.invoke(new CacheSerializableRunnable("testlimit") {
      @Override
      public void run2() throws CacheException {
        final long ONE_MEG = 1024L * 1024L;
        final PartitionedRegion pr1 = (PartitionedRegion)cache.getRegion("PR1");
        final PartitionedRegion pr2 = (PartitionedRegion)cache.getRegion("PR2");
        assertEquals(pr1.getLocalMaxMemory(), ((AbstractLRURegionMap)pr1.entries)
            ._getLruList().stats().getLimit()
            / ONE_MEG);
        assertEquals(pr2.getLocalMaxMemory(), ((AbstractLRURegionMap)pr2.entries)
            ._getLruList().stats().getLimit()
            / ONE_MEG);
        assertEquals(1000, ((AbstractLRURegionMap)pr1.entries)._getLruList()
            .stats().getDestroysLimit());
        assertEquals(1000, ((AbstractLRURegionMap)pr2.entries)._getLruList()
            .stats().getDestroysLimit());
      }
    });

  }

  @Test
  public void testEntryLruCounter() {
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
    prepareScenario(EvictionAlgorithm.LRU_HEAP);
    System.setProperty(
        HeapLRUCapacityController.TOP_UP_HEAP_EVICTION_PERCENTAGE_PROPERTY,
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
    final long ONE_MEG = 1024L * 1024L;
    createCache();
    createPartitionedRegion(true, EvictionAlgorithm.LRU_ENTRY, "PR1", 2, 1,
        10000);
    for (int counter = 1; counter <= maxEnteries; counter++) {
      region.put(new Integer(counter), new byte[(1 * 1024 * 1024) - 2]);
    }
    long sizeOfPRegion = ((AbstractLRURegionMap)((PartitionedRegion)region).entries)
        ._getLruList().stats().getCounter();

    assertEquals(sizeOfPRegion, 20);
    long bucketSize = 0;
    for (final Iterator i = ((PartitionedRegion)region).getDataStore()
        .getAllLocalBuckets().iterator(); i.hasNext();) {
      final Map.Entry entry = (Map.Entry)i.next();
      final BucketRegion bucketRegion = (BucketRegion)entry.getValue();
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
    sizeOfPRegion = ((AbstractLRURegionMap)((PartitionedRegion)region).entries)
        ._getLruList().stats().getCounter();
    ;
    assertEquals(sizeOfPRegion, 20);
    for (final Iterator i = ((PartitionedRegion)region).getDataStore()
        .getAllLocalBuckets().iterator(); i.hasNext();) {
      final Map.Entry entry = (Map.Entry)i.next();
      final BucketRegion bucketRegion = (BucketRegion)entry.getValue();
      if (bucketRegion == null) {
        continue;
      }
      assertEquals(bucketRegion.getCounter(), 10);
      bucketSize = bucketSize + (bucketRegion.getCounter());
    }
    assertEquals(sizeOfPRegion, bucketSize);

    // Clear one bucket
    for (final Iterator i = ((PartitionedRegion)region).getDataStore()
        .getAllLocalBuckets().iterator(); i.hasNext();) {
      final Map.Entry entry = (Map.Entry)i.next();
      final BucketRegion bucketRegion = (BucketRegion)entry.getValue();
      if (bucketRegion == null) {
        continue;
      }
      //Don't just clear the bucket here, use the real region
      //api to remove the entries.
      for(Object key: bucketRegion.keySet()) {
        region.destroy(key);
      }
      assertEquals(bucketRegion.getCounter(), 0);
      break;
    }
    sizeOfPRegion = ((AbstractLRURegionMap)((PartitionedRegion)region).entries)
        ._getLruList().stats().getCounter();
    ;
    assertEquals(sizeOfPRegion, 10);
  }

  

  @Test
  public void testEntryLRUEvictionNDestroyNNumOverflowOnDiskCount() {
    final int extraEnteries = 24;
    prepareScenario(EvictionAlgorithm.LRU_ENTRY);
    putData("PR1", maxEnteries + extraEnteries);
    putData("PR2", maxEnteries + extraEnteries);
    dataStore1.invoke(new CacheSerializableRunnable(
        "testEntryLRUEvictionNDestroyNNumOverflowOnDiskCount") {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion)cache.getRegion("PR1");
        assertEquals(((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
            .getEvictions(), (extraEnteries-maxEnteries) / 2);
        assertEquals(((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
            .getDestroys(), ((AbstractLRURegionMap)pr1.entries)._getLruList()
            .stats().getEvictions());
        
        PartitionedRegion pr2 = (PartitionedRegion)cache.getRegion("PR2");
        assertEquals(((AbstractLRURegionMap)pr2.entries)._getLruList().stats()
            .getEvictions(),  (extraEnteries-maxEnteries) / 2);
        assertEquals(((AbstractLRURegionMap)pr2.entries)._getLruList().stats()
            .getDestroys(), 0);
        assertEquals(pr2.getDiskRegionStats().getNumOverflowOnDisk(),
            (extraEnteries-maxEnteries) / 2);
      }
    });

    dataStore2.invoke(new CacheSerializableRunnable(
        "testEntryLRUEvictionNDestroyNNumOverflowOnDiskCount") {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion)cache.getRegion("PR1");
        assertEquals(((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
            .getEvictions(),  (extraEnteries-maxEnteries) / 2);
        assertEquals(((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
            .getDestroys(), ((AbstractLRURegionMap)pr1.entries)._getLruList()
            .stats().getEvictions());
       
        PartitionedRegion pr2 = (PartitionedRegion)cache.getRegion("PR2");
        assertEquals(((AbstractLRURegionMap)pr2.entries)._getLruList().stats()
            .getEvictions(),  (extraEnteries-maxEnteries) / 2);
        assertEquals(((AbstractLRURegionMap)pr2.entries)._getLruList().stats()
            .getDestroys(), 0);
        assertEquals(pr2.getDiskRegionStats().getNumOverflowOnDisk(),
            (extraEnteries-maxEnteries) / 2);
      }
    });
  }

  @Test
  public void testMemLRUEvictionNDestroyNNumOverflowOnDiskCount() {
    int localMaxMem=50;
    final int extraEntries = 6;
    prepareScenario(EvictionAlgorithm.LRU_MEMORY);
    putData("PR1", (2 * localMaxMem)+extraEntries);
    putData("PR2", (2 * localMaxMem/*localmaxmem*/)+extraEntries);
    dataStore1.invoke(new CacheSerializableRunnable("testEvictionCount") {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion)cache.getRegion("PR1");
        LogWriterUtils.getLogWriter().info("dddd  local"+pr1.getLocalMaxMemory());
        LogWriterUtils.getLogWriter().info("dddd  local evi"+((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
        .getEvictions());
        LogWriterUtils.getLogWriter().info("dddd  local entries"+((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
            .getCounter()/(1024*1024));
        HeapMemoryMonitor hmm = ((InternalResourceManager) cache.getResourceManager()).getHeapMonitor();
        long memused=hmm.getBytesUsed()/(1024*1024);
        LogWriterUtils.getLogWriter().info("dddd  local memused= "+memused);
        assertTrue(((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
            .getEvictions() >= extraEntries / 2);
        assertEquals(((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
            .getDestroys(), ((AbstractLRURegionMap)pr1.entries)._getLruList()
            .stats().getEvictions());

        PartitionedRegion pr2 = (PartitionedRegion)cache.getRegion("PR2");
        assertTrue(((AbstractLRURegionMap)pr2.entries)._getLruList().stats()
            .getEvictions() >= extraEntries / 2);
        assertEquals(((AbstractLRURegionMap)pr2.entries)._getLruList().stats()
            .getDestroys(), 0);
        assertTrue(pr2.getDiskRegionStats().getNumOverflowOnDisk()>=
            extraEntries / 2);
      }
    });

    dataStore2.invoke(new CacheSerializableRunnable("testEvictionCount") {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion)cache.getRegion("PR1");
        assertTrue(((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
            .getEvictions() >= extraEntries / 2);
        assertEquals(((AbstractLRURegionMap)pr1.entries)._getLruList().stats()
            .getDestroys(), ((AbstractLRURegionMap)pr1.entries)._getLruList()
            .stats().getEvictions());

        PartitionedRegion pr2 = (PartitionedRegion)cache.getRegion("PR2");
        assertTrue(((AbstractLRURegionMap)pr2.entries)._getLruList().stats()
            .getEvictions() >= extraEntries / 2);
        assertEquals(((AbstractLRURegionMap)pr2.entries)._getLruList().stats()
            .getDestroys(), 0);
        assertTrue(pr2.getDiskRegionStats().getNumOverflowOnDisk()>=
            extraEntries / 2);
      }
    });
  }
 

 public void prepareScenario(EvictionAlgorithm evictionAlgorithm) {
    createCacheInAllVms();
    createPartitionedRegionInAllVMS(true, evictionAlgorithm, "PR1",
        totalNoOfBuckets, 1, 10000);
    createPartitionedRegionInAllVMS(true, evictionAlgorithm, "PR2",
        totalNoOfBuckets, 2, 10000);
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
    }
    catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }

  private void createPartitionedRegionInAllVMS(final boolean setEvictionOn,
      final EvictionAlgorithm evictionAlgorithm, final String regionName,
      final int totalNoOfBuckets, final int evictionAction, final int evictorInterval) {

    dataStore1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(setEvictionOn, evictionAlgorithm, regionName,totalNoOfBuckets, evictionAction, evictorInterval);
      }      
    });
    
    dataStore2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(setEvictionOn, evictionAlgorithm, regionName,totalNoOfBuckets, evictionAction, evictorInterval);
      }      
    });
  }

  public void createPartitionedRegion(boolean setEvictionOn,
      EvictionAlgorithm evictionAlgorithm, String regionName,
      int totalNoOfBuckets, int evictionAction, int evictorInterval) {

    final AttributesFactory factory = new AttributesFactory();
    factory.setOffHeap(isOffHeapEnabled());
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory()
        .setRedundantCopies(0).setTotalNumBuckets(totalNoOfBuckets).setLocalMaxMemory(50);

    factory.setPartitionAttributes(partitionAttributesFactory.create());
    if (setEvictionOn) {
      if (evictionAlgorithm.isLRUHeap()) {
        factory.setEvictionAttributes(EvictionAttributes
            .createLRUHeapAttributes(null,
                evictionAction == 1 ? EvictionAction.LOCAL_DESTROY
                    : EvictionAction.OVERFLOW_TO_DISK));
      }
      else if (evictionAlgorithm.isLRUMemory()) {
        factory.setEvictionAttributes(EvictionAttributes
            .createLRUMemoryAttributes(maxSizeInMb, new ObjectSizerImpl(),
                evictionAction == 1 ? EvictionAction.LOCAL_DESTROY
                    : EvictionAction.OVERFLOW_TO_DISK));
      }
      else {
        factory.setEvictionAttributes(EvictionAttributes
            .createLRUEntryAttributes(maxEnteries,
                evictionAction == 1 ? EvictionAction.LOCAL_DESTROY
                    : EvictionAction.OVERFLOW_TO_DISK));
      }
      if (evictionAction == 2) {
        final File[] diskDirs = new File[1];
        diskDirs[0] = new File("Partitioned_Region_EvictionStats/" + "LogFile"
            + "_" + OSProcess.getId());
        diskDirs[0].mkdirs();
        factory.setDiskSynchronous(true);
        factory.setDiskStoreName(cache.createDiskStoreFactory()
                                 .setDiskDirs(diskDirs)
                                 .create("EvictionStatsDUnitTest")
                                 .getName());
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
          //getLogWriter().info("Deep put data element no->" + counter);
        }
      }
    });
  }

  private long getPRCounter(String prRegionName) {
    final long ONE_MEG = 1024L * 1024L;
    long sizeOfPR = 0;
    sizeOfPR = sizeOfPR
        + (Long)dataStore1.invoke(() -> EvictionStatsDUnitTest.getPartionRegionCounter( prRegionName ));
    sizeOfPR = sizeOfPR
        + (Long)dataStore2.invoke(() -> EvictionStatsDUnitTest.getPartionRegionCounter( prRegionName ));
    return sizeOfPR / ONE_MEG;
  }

  public static long getPartionRegionCounter(String prRegionName) {
    final PartitionedRegion pr = (PartitionedRegion)cache
        .getRegion(prRegionName);
    return ((AbstractLRURegionMap)pr.entries)._getLruList().stats()
        .getCounter();
  }

  private long getCounterForBucketsOfPR(String prRegionName) {
    final long ONE_MEG = 1024L * 1024L;
    long totalBucketSize = 0;
    totalBucketSize = totalBucketSize
        + (Long)dataStore1.invoke(() -> EvictionStatsDUnitTest.getCounterForBuckets( prRegionName ));
    totalBucketSize = totalBucketSize
        + (Long)dataStore2.invoke(() -> EvictionStatsDUnitTest.getCounterForBuckets( prRegionName ));
    return totalBucketSize / ONE_MEG;

  }

  public static long getCounterForBuckets(String prRegionName) {
    long bucketSize = 0;
    final PartitionedRegion pr = (PartitionedRegion)cache
        .getRegion(prRegionName);
    for (final Iterator i = pr.getDataStore().getAllLocalBuckets().iterator(); i
        .hasNext();) {
      final Map.Entry entry = (Map.Entry)i.next();
      final BucketRegion bucketRegion = (BucketRegion)entry.getValue();
      if (bucketRegion == null) {
        continue;
      }
      LogWriterUtils.getLogWriter().info(
          "Size of bucket " + bucketRegion.getName() + "of Pr " + prRegionName
              + " = " + bucketRegion.getCounter() / (1000000));
      bucketSize = bucketSize + bucketRegion.getCounter();
    }
    return bucketSize;
  }

  public boolean isOffHeapEnabled() {
    return false;
  }    
}
