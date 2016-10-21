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

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizerImpl;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryEvent;
import org.apache.geode.internal.cache.control.MemoryThresholds.MemoryState;
import org.apache.geode.internal.cache.lru.HeapEvictor;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class EvictionTestBase extends JUnit4CacheTestCase {

  protected static Cache cache = null;

  protected static VM dataStore1 = null;

  protected static VM dataStore2 = null;

  protected static VM dataStore3 = null;

  protected static VM dataStore4 = null;

  protected static Region region = null;

  static int maxEnteries = 20;

  static int maxSizeInMb = 20;

  static int totalNoOfBuckets = 4;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    dataStore3 = host.getVM(2);
    dataStore4 = host.getVM(3);
  }

  public void prepareScenario1(EvictionAlgorithm evictionAlgorithm, int maxEntries) {
    createCacheInAllVms();
    createPartitionedRegionInAllVMS(true, evictionAlgorithm, "PR1", totalNoOfBuckets, 1, 10000,
        maxEntries);
    createPartitionedRegionInAllVMS(true, evictionAlgorithm, "PR2", totalNoOfBuckets, 2, 10000,
        maxEntries);
  }

  public void raiseFakeNotification(VM vm, final String prName, final int noOfExpectedEvictions) {
    vm.invoke(new CacheSerializableRunnable("fakeNotification") {
      @Override
      public void run2() throws CacheException {
        final LocalRegion region = (LocalRegion) cache.getRegion(prName);
        getEvictor().testAbortAfterLoopCount = 1;

        RegionEvictorTask.TEST_EVICTION_BURST_PAUSE_TIME_MILLIS = 0;

        InternalResourceManager irm = ((GemFireCacheImpl) cache).getResourceManager();
        HeapMemoryMonitor hmm = irm.getHeapMonitor();
        hmm.setTestMaxMemoryBytes(100);
        hmm.updateStateAndSendEvent(90);

        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            final long currentEvictions =
                ((AbstractLRURegionMap) region.entries)._getLruList().stats().getEvictions();
            if (Math.abs(currentEvictions - noOfExpectedEvictions) <= 1) { // Margin of error is 1
              return true;
            } else if (currentEvictions > noOfExpectedEvictions) {
              fail(description());
            }
            return false;
          }

          public String description() {
            return "expected " + noOfExpectedEvictions + " evictions, but got "
                + ((AbstractLRURegionMap) region.entries)._getLruList().stats().getEvictions();
          }
        };
        Wait.waitForCriterion(wc, 60000, 1000, true);
      }
    });
  }

  public void prepareScenario2(final EvictionAlgorithm evictionAlgorithm,
      final String partitionRegion1, final String partitionRegion2) {
    dataStore3.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createCache();
      }
    });

    dataStore4.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createCache();
      }
    });

    dataStore3.invoke(() -> EvictionTestBase.setTolerance());
    dataStore4.invoke(() -> EvictionTestBase.setTolerance());

    dataStore3.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(true, evictionAlgorithm, partitionRegion1, 2, 2, 10000, 0);
      }
    });

    dataStore4.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(true, evictionAlgorithm, partitionRegion1, 2, 2, 10000, 0);
      }
    });

    dataStore3.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(true, evictionAlgorithm, partitionRegion2, 2, 2, 10000, 0);
      }
    });

    dataStore4.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(true, evictionAlgorithm, partitionRegion2, 2, 2, 10000, 0);
      }
    });
  }

  public void fakeNotification() {
    dataStore3.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        sendFakeNotification();
      }
    });

    dataStore4.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        sendFakeNotification();
      }
    });
  }

  public void sendFakeNotification() {
    HeapMemoryMonitor hmm = ((InternalResourceManager) cache.getResourceManager()).getHeapMonitor();
    RegionEvictorTask.TEST_EVICTION_BURST_PAUSE_TIME_MILLIS = 0;
    MemoryEvent event = new MemoryEvent(getResourceType(), MemoryState.NORMAL, MemoryState.EVICTION,
        cache.getDistributedSystem().getDistributedMember(), 90, true, hmm.getThresholds());
    getEvictor().onEvent(event);
  }

  public static void setTolerance() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "memoryEventTolerance",
        Integer.toString(0));
  }

  public void createDistributedRegion() {
    dataStore1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createDistRegion();
      }
    });
  }

  public void createDistRegion() {
    final AttributesFactory factory = new AttributesFactory();
    factory.setOffHeap(getOffHeapEnabled());
    factory.setDataPolicy(DataPolicy.NORMAL);
    factory.setEvictionAttributes(
        EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.LOCAL_DESTROY));
    DistributedRegion distRegion = (DistributedRegion) cache.createRegion("DR1", factory.create());
    assertNotNull(distRegion);

  }

  public static void createDistRegionWithMemEvictionAttr() {
    final AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.NORMAL);
    factory.setEvictionAttributes(EvictionAttributes
        .createLRUMemoryAttributes(new ObjectSizerImpl(), EvictionAction.LOCAL_DESTROY));
    DistributedRegion distRegion = (DistributedRegion) cache.createRegion("DR1", factory.create());
    assertNotNull(distRegion);

  }


  public void createCacheInAllVms() {
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
    new EvictionTestBase().createCache();
  }

  public void createCache() {
    try {
      HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "memoryEventTolerance", "0");

      Properties props = new Properties();
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
      LogWriterUtils.getLogWriter().info("cache= " + cache);
      LogWriterUtils.getLogWriter().info("cache closed= " + cache.isClosed());
      cache.getResourceManager().setEvictionHeapPercentage(85);
      LogWriterUtils.getLogWriter()
          .info("eviction= " + cache.getResourceManager().getEvictionHeapPercentage());
      LogWriterUtils.getLogWriter()
          .info("critical= " + cache.getResourceManager().getCriticalHeapPercentage());
    } catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }

  public ArrayList getTestTaskSetSizes() {
    return getEvictor().testOnlyGetSizeOfTasks();
  }

  protected void createPartitionedRegionInAllVMS(final boolean setEvictionOn,
      final EvictionAlgorithm evictionAlgorithm, final String regionName,
      final int totalNoOfBuckets, final int evictionAction, final int evictorInterval,
      final int maxEntries) {

    dataStore1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(setEvictionOn, evictionAlgorithm, regionName, totalNoOfBuckets,
            evictionAction, evictorInterval, maxEntries);
      }
    });

    dataStore2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(setEvictionOn, evictionAlgorithm, regionName, totalNoOfBuckets,
            evictionAction, evictorInterval, maxEntries);
      }
    });
  }

  public void createPartitionedRegion(boolean setEvictionOn, EvictionAlgorithm evictionAlgorithm,
      String regionName, int totalNoOfBuckets, int evictionAction, int evictorInterval,
      int maxEnteries) {

    final AttributesFactory factory = new AttributesFactory();

    factory.setOffHeap(getOffHeapEnabled());

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory()
        .setRedundantCopies(totalNoOfBuckets == 4 ? 0 : 1).setTotalNumBuckets(totalNoOfBuckets);
    if (evictionAlgorithm.isLRUMemory())
      partitionAttributesFactory.setLocalMaxMemory(maxEnteries);

    factory.setPartitionAttributes(partitionAttributesFactory.create());
    if (setEvictionOn) {
      if (evictionAlgorithm.isLRUHeap()) {
        factory.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null,
            evictionAction == 1 ? EvictionAction.LOCAL_DESTROY : EvictionAction.OVERFLOW_TO_DISK));
      } else if (evictionAlgorithm.isLRUMemory()) {

        factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(
            new ObjectSizerImpl(),
            evictionAction == 1 ? EvictionAction.LOCAL_DESTROY : EvictionAction.OVERFLOW_TO_DISK));
      } else {
        factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEnteries,
            evictionAction == 1 ? EvictionAction.LOCAL_DESTROY : EvictionAction.OVERFLOW_TO_DISK));
      }
      if (evictionAction == 2) {
        final File[] diskDirs = new File[1];
        diskDirs[0] =
            new File("Partitioned_Region_Eviction/" + "LogFile" + "_" + OSProcess.getId());
        diskDirs[0].mkdirs();
        factory.setDiskSynchronous(true);
        factory.setDiskStoreName(cache.createDiskStoreFactory().setDiskDirs(diskDirs)
            .create("EvictionTestBase").getName());
      }
    }

    region = cache.createRegion(regionName, factory.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info("Partitioned Region created Successfully :" + region);
  }

  public static void putData(final String regionName, final int noOfElememts,
      final int sizeOfElement) {
    dataStore1.invoke(new CacheSerializableRunnable("putData") {
      @Override
      public void run2() throws CacheException {
        final Region pr = cache.getRegion(regionName);
        for (int counter = 1; counter <= noOfElememts; counter++) {
          pr.put(new Integer(counter), new byte[sizeOfElement * 1024 * 1024]);
        }
      }
    });
  }

  public static void putDataInDistributedRegion(final int noOfElememts, final int sizeOfElement) {
    dataStore1.invoke(new CacheSerializableRunnable("putData") {
      @Override
      public void run2() throws CacheException {
        final Region pr = cache.getRegion("DR1");
        for (int counter = 1; counter <= noOfElememts; counter++) {
          pr.put(new Integer(counter), new byte[sizeOfElement * 1024 * 1024]);
          LogWriterUtils.getLogWriter().info("Amar put data element no->" + counter);
        }
      }
    });
  }

  public void validateNoOfEvictions(final String regionName, final int noOfEvictions) {
    final SerializableCallable validate = new SerializableCallable("Validate evictions") {
      public Object call() throws Exception {

        try {
          final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);

          for (final Iterator i = pr.getDataStore().getAllLocalBuckets().iterator(); i.hasNext();) {
            final Map.Entry entry = (Map.Entry) i.next();
            final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
            if (bucketRegion == null) {
              continue;
            }
            LogWriterUtils.getLogWriter().info(
                "FINAL bucket= " + bucketRegion.getFullPath() + "size= " + bucketRegion.size());
          }

          return new Long(((AbstractLRURegionMap) pr.entries)._getLruList().stats().getEvictions());

        } finally {
        }
      }
    };
    long evictionsInVM1 = (Long) dataStore1.invoke(validate);
    long evictionsInVM2 = (Long) dataStore2.invoke(validate);
    LogWriterUtils.getLogWriter()
        .info("EEE evicitons = " + noOfEvictions + " " + (evictionsInVM1 + evictionsInVM2));
    assertEquals(noOfEvictions, (evictionsInVM1 + evictionsInVM2));
  }

  public void verifyThreadPoolTaskCount(final int taskCountToBeVerified) {
    final SerializableCallable getThreadPoolTaskCount =
        new SerializableCallable("Validate evictions") {
          public Object call() throws Exception {
            try {
              return getEvictor().getEvictorThreadPool() != null
                  ? getEvictor().getEvictorThreadPool().getTaskCount() : 0;
            } finally {
            }
          }
        };
    Long taskCountOfVM = (Long) dataStore1.invoke(getThreadPoolTaskCount);
    assertTrue(taskCountOfVM > 0 && taskCountOfVM <= taskCountToBeVerified);
  }

  public static void putDataInDataStore3(final String regionName, final int noOfElememts,
      final int sizeOfElement) {
    dataStore3.invoke(new CacheSerializableRunnable("putData") {
      @Override
      public void run2() throws CacheException {
        final Region pr = cache.getRegion(regionName);
        for (int counter = 1; counter <= noOfElememts; counter++) {
          pr.put(new Integer(counter), new byte[sizeOfElement * 1024 * 1024]);
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new CacheSerializableRunnableException(e.getLocalizedMessage(), e);
          }
        }
      }
    });
  }

  public static void print(final String regionName) {
    dataStore3.invoke(new CacheSerializableRunnable("putData") {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
        Set<BucketRegion> bucket = pr.getDataStore().getAllLocalBucketRegions();
        Iterator itr = bucket.iterator();
        while (itr.hasNext()) {
          BucketRegion br = (BucketRegion) itr.next();
          LogWriterUtils.getLogWriter().info("Print " + br.size());
        }
      }
    });
  }

  public void validateNoOfEvictionsInDataStore3N4(final String regionName,
      final int noOfEvictions) {
    final SerializableCallable validate = new SerializableCallable("Validate evictions") {
      public Object call() throws Exception {

        try {
          final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
          return new Long(((AbstractLRURegionMap) pr.entries)._getLruList().stats().getEvictions());

        } finally {
        }
      }
    };
    long evictionsInVM1 = (Long) dataStore3.invoke(validate);
    long evictionsInVM2 = (Long) dataStore4.invoke(validate);
    assertEquals(noOfEvictions, evictionsInVM1 + evictionsInVM2);
  }

  public void killVm() {
    dataStore4.invoke(() -> EvictionTestBase.close());

  }

  public static void close() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public void bringVMBackToLife() {
    dataStore4.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createCache();
      }
    });

    dataStore4.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(true, EvictionAlgorithm.LRU_HEAP, "PR3", 2, 2, 10000, 0);
      }
    });

    dataStore4.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(true, EvictionAlgorithm.LRU_HEAP, "PR4", 2, 2, 10000, 0);
      }
    });
  }

  public int getPRSize(String string) {
    Integer prSize = (Integer) dataStore4.invoke(() -> EvictionTestBase.returnPRSize(string));
    return prSize;

  }

  public static int returnPRSize(String string) {
    final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(string);
    return pr.size();
  }

  public boolean getOffHeapEnabled() {
    return false;
  }

  public HeapEvictor getEvictor() {
    return ((GemFireCacheImpl) cache).getHeapEvictor();
  }

  @SuppressWarnings("serial")
  public int getExpectedEvictionRatioOnVm(final VM vm) {
    return (Integer) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return (int) Math
            .ceil((getEvictor().getTotalBytesToEvict() / 2) / (double) ((1024 * 1024) + 100)) * 2;
      }
    });
  }

  public ResourceType getResourceType() {
    return ResourceType.HEAP_MEMORY;
  }
}
