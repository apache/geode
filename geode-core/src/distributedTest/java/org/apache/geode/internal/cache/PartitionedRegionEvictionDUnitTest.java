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

import static java.lang.System.getProperties;
import static java.util.Map.Entry;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.eviction.HeapLRUController;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class PartitionedRegionEvictionDUnitTest extends JUnit4CacheTestCase {

  @Test
  public void testHeapLRUWithOverflowToDisk() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    final Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final String uniqName = getUniqueName();
    final float heapPercentage = 50.9f;
    final int redundantCopies = 1;
    final int evictorInterval = 100;
    final String name = uniqName + "-PR";

    final CacheSerializableRunnable create =
        new CacheSerializableRunnable("Create Heap LRU with Overflow to disk partitioned Region") {
          public void run2() {
            System.setProperty(HeapLRUController.TOP_UP_HEAP_EVICTION_PERCENTAGE_PROPERTY,
                Float.toString(0));
            setEvictionPercentage(heapPercentage);
            final Properties sp = System.getProperties();
            final AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            factory.setEvictionAttributes(
                EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
            factory.setDiskSynchronous(true);
            DiskStoreFactory dsf = getCache().createDiskStoreFactory();
            final File[] diskDirs = new File[1];
            diskDirs[0] = new File("overflowDir/" + uniqName + "_" + OSProcess.getId());
            diskDirs[0].mkdirs();
            dsf.setDiskDirs(diskDirs);
            DiskStore ds = dsf.create(name);
            factory.setDiskStoreName(ds.getName());
            final PartitionedRegion pr =
                (PartitionedRegion) createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    vm3.invoke(create);
    vm2.invoke(create);

    final int bucketsToCreate = 3;
    final SerializableRunnable createBuckets = new SerializableRunnable("Create Buckets") {
      public void run() {
        setEvictionPercentage(heapPercentage);
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        // Create three buckets, add enough stuff in each to force two
        // overflow
        // ops in each
        for (int i = 0; i < bucketsToCreate; i++) {
          // assume mod-based hashing for bucket creation
          pr.put(new Integer(i), "value0");
          pr.put(new Integer(i + pr.getPartitionAttributes().getTotalNumBuckets()), "value1");
          pr.put(new Integer(i + (pr.getPartitionAttributes().getTotalNumBuckets()) * 2), "value2");
        }
      }
    };
    vm3.invoke(createBuckets);

    final SerializableCallable assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          public Object call() throws Exception {

            try {
              setEvictionPercentage(heapPercentage);
              final Properties sp = getProperties();
              final int expectedInvocations = 10;
              final int maximumWaitSeconds = 60; // seconds
              final int pollWaitMillis = evictorInterval * 2;
              assertTrue(pollWaitMillis < (SECONDS.toMillis(maximumWaitSeconds) * 4));
              final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
              assertNotNull(pr);
              for (final Iterator i =
                  ((PartitionedRegion) pr).getDataStore().getAllLocalBuckets().iterator(); i
                      .hasNext();) {
                final Entry entry = (Entry) i.next();
                final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
                if (bucketRegion == null) {
                  continue;
                }
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                    .isLRUHeap());
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                    .isOverflowToDisk());
              }
              raiseFakeNotification();
              WaitCriterion wc = new WaitCriterion() {
                String excuse;

                public boolean done() {
                  // we have a primary
                  if (pr.getDiskRegionStats().getNumOverflowOnDisk() == 9) {
                    return true;
                  }
                  return false;
                }

                public String description() {
                  return excuse;
                }
              };
              GeodeAwaitility.await().untilAsserted(wc);

              int entriesEvicted = 0;

              entriesEvicted += pr.getDiskRegionStats().getNumOverflowOnDisk();
              return new Integer(entriesEvicted);
            } finally {
              cleanUpAfterFakeNotification();
              // Clean up the observer for future tests
              // final Properties sp = System.getProperties();
              /*
               * assertNotNull(sp .remove(HeapLRUCapacityController.FREE_MEMORY_OBSERVER));
               */
            }
          }
        };
    final Integer v2i = (Integer) vm2.invoke(assertBucketAttributesAndEviction);
    final Integer v3i = (Integer) vm3.invoke(assertBucketAttributesAndEviction);
    final int totalEvicted = v2i.intValue() + v3i.intValue();
    // assume all three entries in each bucket were evicted to disk
    assertEquals((3 * bucketsToCreate * (redundantCopies + 1)), totalEvicted);
  }

  protected void raiseFakeNotification() {
    ((GemFireCacheImpl) getCache()).getHeapEvictor().setTestAbortAfterLoopCount(1);
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "memoryEventTolerance", "0");

    setEvictionPercentage(85);
    HeapMemoryMonitor hmm =
        ((GemFireCacheImpl) getCache()).getInternalResourceManager().getHeapMonitor();
    hmm.setTestMaxMemoryBytes(100);

    hmm.updateStateAndSendEvent(90);
  }

  protected void cleanUpAfterFakeNotification() {
    ((GemFireCacheImpl) getCache()).getHeapEvictor().setTestAbortAfterLoopCount(Integer.MAX_VALUE);
    HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "memoryEventTolerance");
  }

  @Test
  public void testHeapLRUWithLocalDestroy() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    final Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final String uniqName = getUniqueName();
    final float heapPercentage = 50.9f;
    final int redundantCopies = 1;
    final int evictorInterval = 100;
    final String name = uniqName + "-PR";

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Heap LRU with local destroy on a partitioned Region") {
      public void run2() {
        System.setProperty(HeapLRUController.TOP_UP_HEAP_EVICTION_PERCENTAGE_PROPERTY,
            Float.toString(0));
        setEvictionPercentage(heapPercentage);
        final Properties sp = System.getProperties();
        final AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
        final PartitionedRegion pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm3.invoke(create);
    vm2.invoke(create);

    final int bucketsToCreate = 3;
    final SerializableRunnable createBuckets = new SerializableRunnable("Create Buckets") {
      public void run() {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        // Create three buckets, add enough stuff in each to force two
        // overflow
        // ops in each
        for (int i = 0; i < bucketsToCreate; i++) {
          // assume mod-based hashing for bucket creation
          pr.put(new Integer(i), "value0");
          pr.put(new Integer(i + pr.getPartitionAttributes().getTotalNumBuckets()), "value1");
          pr.put(new Integer(i + (pr.getPartitionAttributes().getTotalNumBuckets()) * 2), "value2");
        }
      }
    };
    vm3.invoke(createBuckets);

    final SerializableCallable assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          public Object call() throws Exception {
            try {
              final Properties sp = getProperties();
              final int expectedInvocations = 10;
              final int maximumWaitSeconds = 60; // seconds
              final int pollWaitMillis = evictorInterval * 2;
              assertTrue(pollWaitMillis < (SECONDS.toMillis(maximumWaitSeconds) * 4));
              final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
              assertNotNull(pr);

              long entriesEvicted = 0;
              for (final Iterator i = pr.getDataStore().getAllLocalBuckets().iterator(); i
                  .hasNext();) {
                final Entry entry = (Entry) i.next();

                final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
                if (bucketRegion == null) {
                  continue;
                }
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                    .isLRUHeap());
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                    .isLocalDestroy());
              }
              raiseFakeNotification();
              WaitCriterion wc = new WaitCriterion() {
                String excuse;

                public boolean done() {
                  // we have a primary
                  if (pr.getTotalEvictions() == 9) {
                    return true;
                  }
                  return false;
                }

                public String description() {
                  return excuse;
                }
              };
              GeodeAwaitility.await().untilAsserted(wc);

              entriesEvicted = pr.getTotalEvictions();
              return new Long(entriesEvicted);
            } finally {
              cleanUpAfterFakeNotification();
              // Clean up the observer for future tests
              // final Properties sp = System.getProperties();
              /*
               * assertNotNull(sp .remove(HeapLRUCapacityController.FREE_MEMORY_OBSERVER));
               */
            }
          }
        };
    final Long v2i = (Long) vm2.invoke(assertBucketAttributesAndEviction);
    final Long v3i = (Long) vm3.invoke(assertBucketAttributesAndEviction);
    final int totalEvicted = v2i.intValue() + v3i.intValue();
    // assume all three entries in each bucket were evicted to disk
    assertEquals((3 * bucketsToCreate * (redundantCopies + 1)), totalEvicted);
  }

  @Test
  public void testMemoryLRUWithOverflowToDisk() {
    final Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final VM vm0 = host.getVM(0);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxBuckets = 8;
    final int localMaxMem = 16;
    final int halfKb = 512;
    final int justShyOfTwoMb = (2 * 1024 * 1024) - halfKb;

    final String name = uniqName + "-PR";
    final SerializableRunnable create =
        new SerializableRunnable("Create Memory LRU with Overflow to disk partitioned Region") {
          public void run() {
            try {
              final AttributesFactory factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(
                  new PartitionAttributesFactory().setRedundantCopies(redundantCopies)
                      .setLocalMaxMemory(localMaxMem).setTotalNumBuckets(maxBuckets).create());

              factory.setEvictionAttributes(EvictionAttributes
                  .createLRUMemoryAttributes(ObjectSizer.DEFAULT, EvictionAction.OVERFLOW_TO_DISK));
              factory.setDiskSynchronous(true);
              DiskStoreFactory dsf = getCache().createDiskStoreFactory();
              final File[] diskDirs = new File[1];
              diskDirs[0] = new File("overflowDir/" + uniqName + "_" + OSProcess.getId());
              diskDirs[0].mkdirs();
              dsf.setDiskDirs(diskDirs);
              DiskStore ds = dsf.create(name);
              factory.setDiskStoreName(ds.getName());
              final PartitionedRegion pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              assertNotNull(pr);
            } catch (final CacheException ex) {
              Assert.fail("While creating Partitioned region", ex);
            }
          }
        };
    vm3.invoke(create);
    vm2.invoke(create);
    final int extraEntries = 4;
    final SerializableRunnable createBuckets = new SerializableRunnable("Create Buckets") {
      public void run() {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (int counter = 1; counter <= localMaxMem + extraEntries; counter++) {
          pr.put(new Integer(counter), new byte[1 * 1024 * 1024]);
        }
      }
    };
    vm3.invoke(createBuckets);

    final SerializableCallable assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          public Object call() throws Exception {
            final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
            assertNotNull(pr);
            assertNull(pr.getDiskRegion());

            // assert over-flow behavior in local buckets and number of
            // entries
            // overflowed
            long entriesEvicted = 0;
            for (final Iterator i = pr.getDataStore().getAllLocalBuckets().iterator(); i
                .hasNext();) {
              final Map.Entry entry = (Map.Entry) i.next();

              final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
              if (bucketRegion == null) {
                continue;
              }
              assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                  .isLRUMemory());
              assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                  .isOverflowToDisk());

            }
            entriesEvicted += pr.getDiskRegionStats().getNumOverflowOnDisk();
            return new Long(entriesEvicted);
          }
        };
    final Long vm2i = (Long) vm2.invoke(assertBucketAttributesAndEviction);
    final Long vm3i = (Long) vm3.invoke(assertBucketAttributesAndEviction);
    final int totalEvicted = vm2i.intValue() + vm3i.intValue();
    assertTrue(2 * extraEntries <= totalEvicted);

    // Test for bug 42056 - make sure we get the faulted out entries.
    vm0.invoke(create);

    vm0.invoke(new SerializableRunnable("Test to see that we can get keys") {
      public void run() {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        // Make sure we can get all of the entries
        for (int counter = 1; counter <= localMaxMem + extraEntries; counter++) {
          assertNotNull(pr.get(new Integer(counter)));
        }
      }
    });
  }

  @Test
  public void testMemoryLRUWithLocalDestroy() {
    final Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxBuckets = 8;
    final int localMaxMem = 16;
    final int halfKb = 512;
    final int justShyOfTwoMb = (2 * 1024 * 1024) - halfKb;
    final String name = uniqName + "-PR";

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Memory LRU with local destroy on a partitioned Region") {
      public void run2() {
        final AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies)
                .setLocalMaxMemory(16).setTotalNumBuckets(maxBuckets).create());
        factory.setEvictionAttributes(EvictionAttributes
            .createLRUMemoryAttributes(ObjectSizer.DEFAULT, EvictionAction.LOCAL_DESTROY));
        final PartitionedRegion pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm3.invoke(create);
    vm2.invoke(create);

    final int extraEntries = 4;
    final SerializableRunnable createBuckets = new SerializableRunnable("Create Buckets") {
      public void run() {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (int counter = 1; counter <= localMaxMem + extraEntries; counter++) {
          pr.put(new Integer(counter), new byte[1 * 1024 * 1024]);
        }
      }
    };
    vm3.invoke(createBuckets);

    final SerializableCallable assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          public Object call() throws Exception {
            try {
              final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
              assertNotNull(pr);
              long entriesEvicted = 0;
              for (final Iterator i = pr.getDataStore().getAllLocalBuckets().iterator(); i
                  .hasNext();) {
                final Map.Entry entry = (Map.Entry) i.next();

                final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
                if (bucketRegion == null) {
                  continue;
                }
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                    .isLRUMemory());
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                    .isLocalDestroy());
              }
              entriesEvicted = pr.getTotalEvictions();
              return new Long(entriesEvicted);
            } finally {
            }
          }
        };
    final Long v2i = (Long) vm2.invoke(assertBucketAttributesAndEviction);
    final Long v3i = (Long) vm3.invoke(assertBucketAttributesAndEviction);
    final int totalEvicted = v2i.intValue() + v3i.intValue();
    assertTrue(2 * extraEntries <= totalEvicted);
  }

  @Test
  public void testEntryLRUWithOverflowToDisk() {
    final Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxBuckets = 8;
    final int maxEntries = 16;
    final String name = uniqName + "-PR";
    final SerializableRunnable create =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          public void run() {
            try {
              final AttributesFactory factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(new PartitionAttributesFactory()
                  .setRedundantCopies(redundantCopies).setTotalNumBuckets(maxBuckets).create());
              factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries,
                  EvictionAction.OVERFLOW_TO_DISK));
              factory.setDiskSynchronous(true);
              DiskStoreFactory dsf = getCache().createDiskStoreFactory();
              final File[] diskDirs = new File[1];
              diskDirs[0] = new File("overflowDir/" + uniqName + "_" + OSProcess.getId());
              diskDirs[0].mkdirs();
              dsf.setDiskDirs(diskDirs);
              DiskStore ds = dsf.create(name);
              factory.setDiskStoreName(ds.getName());
              final PartitionedRegion pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              assertNotNull(pr);
            } catch (final CacheException ex) {
              Assert.fail("While creating Partitioned region", ex);
            }
          }
        };
    vm3.invoke(create);
    vm2.invoke(create);
    final int extraEntries = 4;
    final SerializableRunnable createBuckets = new SerializableRunnable("Create Buckets") {
      public void run() {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (int counter = 1; counter <= maxEntries + extraEntries; counter++) {
          pr.put(new Integer(counter), new byte[1 * 1024 * 1024]);
        }
      }
    };
    vm3.invoke(createBuckets);

    final SerializableCallable assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          public Object call() throws Exception {
            final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
            assertNotNull(pr);

            // assert over-flow behavior in local buckets and number of
            // entries
            // overflowed
            int entriesEvicted = 0;
            for (final Iterator i = pr.getDataStore().getAllLocalBuckets().iterator(); i
                .hasNext();) {
              final Map.Entry entry = (Map.Entry) i.next();

              final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
              if (bucketRegion == null) {
                continue;
              }
              assertTrue(
                  bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm().isLRUEntry());
              assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                  .isOverflowToDisk());
            }
            entriesEvicted += pr.getDiskRegionStats().getNumOverflowOnDisk();
            return new Integer(entriesEvicted);

          }
        };
    final Integer vm2i = (Integer) vm2.invoke(assertBucketAttributesAndEviction);
    final Integer vm3i = (Integer) vm3.invoke(assertBucketAttributesAndEviction);
    final int totalEvicted = vm2i.intValue() + vm3i.intValue();
    assertEquals(extraEntries * 2, totalEvicted);
  }

  private static class VerifiableCacheListener extends CacheListenerAdapter
      implements java.io.Serializable {
    public boolean verify(long expectedEvictions) {
      return false;
    }
  };

  @Test
  public void testEntryLRUWithLocalDestroy() {
    final Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxBuckets = 8;
    final int maxEntries = 16;
    final String name = uniqName + "-PR";
    final int extraEntries = 4;

    // final int heapPercentage = 66;
    // final int evictorInterval = 100;

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      public void run2() {
        final AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(redundantCopies).setTotalNumBuckets(maxBuckets).create());
        factory.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY));
        factory.addCacheListener(new VerifiableCacheListener() {
          private long evictionDestroyEvents = 0;

          public void afterDestroy(EntryEvent e) {
            System.out.println("EEEEEEEEEEEEEE key:" + e.getKey());
            EntryEventImpl eei = (EntryEventImpl) e;
            if (Operation.EVICT_DESTROY.equals(eei.getOperation())) {
              evictionDestroyEvents++;
            }
          }

          public boolean verify(long expectedEntries) {
            return expectedEntries == evictionDestroyEvents;
          }
        });
        final PartitionedRegion pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm3.invoke(create);

    final SerializableRunnable create2 =
        new SerializableRunnable("Create Entry LRU with local destroy on a partitioned Region") {
          public void run() {
            try {
              final AttributesFactory factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(new PartitionAttributesFactory()
                  .setRedundantCopies(redundantCopies).setTotalNumBuckets(8).create());
              factory
                  .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries));
              final PartitionedRegion pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              assertNotNull(pr);
            } catch (final CacheException ex) {
              Assert.fail("While creating Partitioned region", ex);
            }
          }
        };
    vm2.invoke(create2);

    final SerializableRunnable createBuckets = new SerializableRunnable("Create Buckets") {
      public void run() {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (int counter = 1; counter <= maxEntries + extraEntries; counter++) {
          pr.put(new Integer(counter), new byte[1 * 1024 * 1024]);
        }
      }
    };
    vm3.invoke(createBuckets);

    final SerializableCallable assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          public Object call() throws Exception {
            try {
              final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
              assertNotNull(pr);
              for (final Iterator i = pr.getDataStore().getAllLocalBuckets().iterator(); i
                  .hasNext();) {
                final Map.Entry entry = (Map.Entry) i.next();

                final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
                if (bucketRegion == null) {
                  continue;
                }
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                    .isLRUEntry());
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                    .isLocalDestroy());
              }
              return pr.getTotalEvictions();
            } finally {
            }
          }
        };

    final Long v2i = (Long) vm2.invoke(assertBucketAttributesAndEviction);
    final Long v3i = (Long) vm3.invoke(assertBucketAttributesAndEviction);
    final int totalEvicted = v2i.intValue() + v3i.intValue();
    assertEquals(2 * extraEntries, totalEvicted);

    final SerializableCallable assertListenerCount = new SerializableCallable(
        "Assert that the number of listener invocations matches the expected total") {
      public Object call() throws Exception {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        RegionAttributes attrs = pr.getAttributes();
        assertNotNull(attrs);
        long entriesEvicted = pr.getTotalEvictions();
        VerifiableCacheListener verifyMe = null;
        for (CacheListener listener : attrs.getCacheListeners()) {
          if (listener instanceof VerifiableCacheListener) {
            verifyMe = ((VerifiableCacheListener) listener);
          }
        }
        assertNotNull(verifyMe); // assert if unable to find the expected listener
        return verifyMe.verify(entriesEvicted);
      }
    };
    assertTrue((Boolean) vm3.invoke(assertListenerCount));
  }

  // Test to validate the Eviction Attribute : LRU Check
  @Test
  public void testEvictionValidationForLRUEntry() {
    final Host host = Host.getHost(0);
    final VM testAccessor = host.getVM(1);
    final VM testDatastore = host.getVM(2);
    final VM firstDatastore = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxEntries = 226;
    final String name = uniqName + "-PR";

    // final int evictionSizeInMB = 200;
    final EvictionAttributes firstEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    // Creating LRU Entry Count Eviction Attribute
    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      public void run2() {

        // Assert that LRUEntry maximum can be less than 1 entry per
        // bucket
        // DUnit not required for this test, but its a convenient place
        // to put it.
        {
          final int buks = 11;
          final AttributesFactory factory = new AttributesFactory();
          factory.setOffHeap(isOffHeap());
          factory.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(buks)
              .setRedundantCopies(0).create());
          factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes((buks / 2)));
          final PartitionedRegion pr = (PartitionedRegion) createRootRegion(name, factory.create());
          final Integer key = new Integer(1);
          pr.put(key, "testval");
          final BucketRegion b;
          try {
            b = pr.getDataStore().getInitializedBucketForId(key,
                new Integer(PartitionedRegionHelper.getHashKey(pr, null, key, null, null)));
          } catch (final ForceReattemptException e) {
            fail();
          }
          pr.destroyRegion();
        }

        final AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(firstEvictionAttrs);
        final Region pr = createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    firstDatastore.invoke(create);

    final SerializableRunnable create2 =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          public void run() {
            final AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            // Assert a different algo is invalid
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(!firstEvictionAttrs.getAlgorithm().isLRUHeap());
              final EvictionAttributes illegalEa =
                  EvictionAttributes.createLRUHeapAttributes(null, firstEvictionAttrs.getAction());
              assertTrue(!firstEvictionAttrs.equals(illegalEa));
              factory.setEvictionAttributes(illegalEa);
              setEvictionPercentage(50);
              createRootRegion(name, factory.create());
              fail("Creating LRU Entry Count Eviction Attribute");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }

            // Assert a different action is invalid
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(firstEvictionAttrs.getAlgorithm().isLRUEntry());
              assertTrue(!firstEvictionAttrs.getAction().isOverflowToDisk());
              final EvictionAttributes illegalEa = EvictionAttributes.createLRUEntryAttributes(
                  firstEvictionAttrs.getMaximum(), EvictionAction.OVERFLOW_TO_DISK);
              assertTrue(!firstEvictionAttrs.equals(illegalEa));
              factory.setEvictionAttributes(illegalEa);
              createRootRegion(name, factory.create());
              fail("Creating LRU Entry Count Eviction Attribute");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }


            assertTrue(firstEvictionAttrs.getAlgorithm().isLRUEntry());
            final EvictionAttributes brokenEa = EvictionAttributes.createLRUEntryAttributes(
                firstEvictionAttrs.getMaximum() + 1, firstEvictionAttrs.getAction());
            assertTrue(!firstEvictionAttrs.equals(brokenEa));
            factory.setEvictionAttributes(brokenEa);
            createRootRegion(name, factory.create());
          }
        };
    testDatastore.invoke(create2);

    testAccessor.invoke(
        new CacheSerializableRunnable("Create an Accessor with and without eviction attributes") {
          public void run2() throws CacheException {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            setEvictionPercentage(50);
            Region r1 = createRootRegion(name, factory.create());
            assertNotNull(r1);
            assertEquals(firstEvictionAttrs, r1.getAttributes().getEvictionAttributes());
          }
        });
  }

  // Test to validate the Eviction Attribute : LRU Action
  @Test
  public void testEvictionValidationForLRUAction() {
    final Host host = Host.getHost(0);
    final VM testDatastore = host.getVM(2);
    final VM firstDatastore = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxEntries = 226;
    final String name = uniqName + "-PR";
    final EvictionAttributes firstEa =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    // Creating LRU Entry Count Eviction Attribute : Algorithm :
    // LOCAL_DESTROY
    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      public void run2() {
        final AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(firstEa);
        final Region pr = createRootRegion(name, factory.create());
        assertNotNull(pr);
        assertEquals(firstEa, pr.getAttributes().getEvictionAttributes());
      }
    };
    firstDatastore.invoke(create);

    final SerializableRunnable create2 =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          public void run() {
            final AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            // Creating LRU Entry Count Eviction Attribute : Action :
            // OVERFLOW_TO_DISK // Exception should occur
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(firstEa.getAlgorithm().isLRUEntry());
              assertTrue(!firstEa.getAction().isOverflowToDisk());
              factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries,
                  EvictionAction.OVERFLOW_TO_DISK));
              createRootRegion(name, factory.create());
              fail("Test to validate the Eviction Attribute : LRU Action");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }
            // Creating LRU Entry Count Eviction Attribute : Action : NONE
            // // Exception should occur
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(firstEa.getAlgorithm().isLRUEntry());
              assertTrue(!firstEa.getAction().isNone());
              factory.setEvictionAttributes(
                  EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.NONE));
              createRootRegion(name, factory.create());
              fail("Test to validate the Eviction Attribute : LRU Action");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }
            // Creating LRU Entry Count Eviction Attribute : Action :
            // LOCAL_DESTROY // Exception should not occur
            factory.setEvictionAttributes(firstEa);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
            assertEquals(firstEa, pr.getAttributes().getEvictionAttributes());
          }
        };
    testDatastore.invoke(create2);
  }

  // Test to validate the Eviction Attribute : LRU Maximum
  @Test
  public void testEvictionValidationForLRUMaximum() {
    final Host host = Host.getHost(0);
    final VM testDatastore = host.getVM(2);
    final VM firstDatastore = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxEntries = 226;
    final String name = uniqName + "-PR";
    final EvictionAttributes firstEvictionAttributes =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    // Creating LRU Entry Count Eviction Attribute : maxentries : 2
    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      public void run2() {
        final AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(firstEvictionAttributes);
        final Region pr = createRootRegion(name, factory.create());
        assertNotNull(pr);
        assertEquals(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
      }
    };
    firstDatastore.invoke(create);

    final SerializableRunnable create2 =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          public void run() {
            final AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());

            final EvictionAttributes ea = EvictionAttributes.createLRUEntryAttributes(
                firstEvictionAttributes.getMaximum() + 10, firstEvictionAttributes.getAction());
            factory.setEvictionAttributes(ea);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    testDatastore.invoke(create2);
  }

  // Test to validate the Eviction Attribute for LRUHeap
  @Test
  public void testEvictionValidationForLRUHeap() {
    final Host host = Host.getHost(0);
    final VM testDatastore = host.getVM(2);
    final VM firstDatastore = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final String name = uniqName + "-PR";
    final int heapPercentage = 66;
    final int evictorInterval = 100;

    final EvictionAttributes firstEvictionAttributes = EvictionAttributes.createLRUHeapAttributes();
    // Creating Heap LRU Eviction Attribute : evictorInterval : 100
    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      public void run2() {
        getCache().getResourceManager().setEvictionHeapPercentage(heapPercentage);
        final AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(firstEvictionAttributes);
        final Region pr = createRootRegion(name, factory.create());
        assertNotNull(pr);
        assertEquals(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
      }
    };
    firstDatastore.invoke(create);

    final SerializableRunnable create2 =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          public void run() {
            setEvictionPercentage(heapPercentage);
            final AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            // Assert that a different algo is invalid
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(!firstEvictionAttributes.getAlgorithm().isLRUEntry());
              final EvictionAttributes invalidEa = EvictionAttributes.createLRUEntryAttributes();
              assertTrue(!invalidEa.equals(firstEvictionAttributes));
              factory.setEvictionAttributes(invalidEa);
              createRootRegion(name, factory.create());
              fail("Expected an IllegalStateException");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }

            // Assert that a different action is invalid
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(firstEvictionAttributes.getAlgorithm().isLRUHeap());
              assertTrue(!firstEvictionAttributes.getAction().isOverflowToDisk());
              final EvictionAttributes invalidEa =
                  EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK);
              assertTrue(!invalidEa.equals(firstEvictionAttributes));
              factory.setEvictionAttributes(invalidEa);
              createRootRegion(name, factory.create());
              fail("Expected an IllegalStateException");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }


            // Assert that a different interval is valid
            // {
            // assertTrue(firstEvictionAttributes.getAlgorithm().isLRUHeap());
            // final EvictionAttributes okHeapLRUea = EvictionAttributes
            // .createLRUHeapAttributes((int)(firstEvictionAttributes.getInterval() + 100),
            // firstEvictionAttributes.getAction());
            // factory.setEvictionAttributes(okHeapLRUea);
            // final Region pr = createRootRegion(name, factory.create());
            // assertNotNull(pr);
            // assertIndexDetailsEquals(okHeapLRUea, pr.getAttributes().getEvictionAttributes());
            // pr.localDestroyRegion();
            // }

            // Assert that a different maximum is valid
            // {
            // assertTrue(firstEvictionAttributes.getAlgorithm().isLRUHeap());
            // final EvictionAttributes okHeapLRUea = EvictionAttributes
            // .createLRUHeapAttributes(
            // (int)firstEvictionAttributes.getInterval(),
            // firstEvictionAttributes.getAction());
            // factory.setEvictionAttributes(okHeapLRUea);
            // final Region pr = createRootRegion(name, factory.create());
            // assertNotNull(pr);
            // assertIndexDetailsEquals(okHeapLRUea, pr.getAttributes().getEvictionAttributes());
            // pr.localDestroyRegion();
            // }

            // Assert that all attributes can be the same
            {
              factory.setEvictionAttributes(firstEvictionAttributes);
              final Region pr = createRootRegion(name, factory.create());
              assertNotNull(pr);
              assertEquals(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            }
          }
        };
    testDatastore.invoke(create2);
  }

  // Test to validate an accessor can set the initial attributes
  @Test
  public void testEvictionValidationWhenInitializedByAccessor() {
    final Host host = Host.getHost(0);
    final VM testDatastore = host.getVM(2);
    final VM accessor = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final String name = uniqName;

    final EvictionAttributes firstEvictionAttributes = EvictionAttributes
        .createLRUMemoryAttributes(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT);
    accessor.invoke(
        new CacheSerializableRunnable("Create an Accessor which sets the first PR eviction attrs") {
          public void run2() {
            final AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(new PartitionAttributesFactory().setLocalMaxMemory(0)
                .setRedundantCopies(redundantCopies).create());
            factory.setEvictionAttributes(firstEvictionAttributes);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
            assertNotSame(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            assertEquals(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            assertEquals(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT,
                pr.getAttributes().getEvictionAttributes().getMaximum());
          }
        });

    testDatastore.invoke(
        new SerializableRunnable("Create a datastore to test existing eviction attributes") {
          public void run() {
            final AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            // Assert that the same attrs is valid
            factory.setEvictionAttributes(firstEvictionAttributes);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
            assertNotSame(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            assertNotSame(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT,
                pr.getAttributes().getEvictionAttributes().getMaximum());
            assertEquals(pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(),
                pr.getAttributes().getEvictionAttributes().getMaximum());
          }
        });
  }

  // Test to validate the Eviction Attribute : HeapLRU Interval
  @Test
  public void testEvictionValidationForLRUMemory() {
    final Host host = Host.getHost(0);
    final VM firstDatastore = host.getVM(1);
    final VM testDatastore = host.getVM(2);
    final VM testAccessor = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final String name = uniqName + "-PR";
    final EvictionAttributes firstEvictionAttributes = EvictionAttributes
        .createLRUMemoryAttributes(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT * 2);

    firstDatastore
        .invoke(new CacheSerializableRunnable("First datastore setting LRU memory eviction attrs") {
          public void run2() {
            // Assert that LRUMemory attributes may be lesser than 1 Mb per
            // bucket
            // DUnit not required for this test, but its a convenient place
            // to put it.
            {
              final int totalNumBuckets = 11;
              final AttributesFactory factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(new PartitionAttributesFactory()
                  .setTotalNumBuckets(totalNumBuckets).setRedundantCopies(0).create());
              factory.setEvictionAttributes(
                  EvictionAttributes.createLRUMemoryAttributes((totalNumBuckets / 2)));
              final PartitionedRegion pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              final Integer key = new Integer(1);
              pr.put(key, "testval");
              final BucketRegion b;
              try {
                b = pr.getDataStore().getInitializedBucketForId(key,
                    new Integer(PartitionedRegionHelper.getHashKey(pr, null, key, null, null)));
              } catch (final ForceReattemptException e) {
                fail();
              }
              pr.destroyRegion();
            }

            // Set the "global" eviction attributes
            final AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            factory.setEvictionAttributes(firstEvictionAttributes);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
            assertNotSame(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            assertNotSame(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT,
                pr.getAttributes().getEvictionAttributes().getMaximum());
            assertEquals(pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(),
                pr.getAttributes().getEvictionAttributes().getMaximum());
          }
        });

    class PRLRUMemoryRunnable extends CacheSerializableRunnable {
      private static final long serialVersionUID = 1L;

      final boolean isAccessor;

      public PRLRUMemoryRunnable(final boolean accessor) {
        super("Test LRU memory eviction attrs on a " + (accessor ? " accessor" : " datastore"));
        this.isAccessor = accessor;
      }

      public PartitionAttributes createPartitionAttributes(final int localMaxMemory) {
        if (this.isAccessor) {
          return new PartitionAttributesFactory().setLocalMaxMemory(0)
              .setRedundantCopies(redundantCopies).create();
        } else {
          return new PartitionAttributesFactory().setLocalMaxMemory(localMaxMemory)
              .setRedundantCopies(redundantCopies).create();
        }
      }

      public void run2() throws CacheException {
        // Configure a PR with the same local max mem as the eviction
        // attributes... and do these tests
        PartitionAttributes pra = createPartitionAttributes(firstEvictionAttributes.getMaximum());
        final AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(pra);

        // Assert that no eviction attributes is invalid
        try {
          getCache().getLogger()
              .info("<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
          final RegionAttributes attrs = factory.create();
          createRootRegion(name, attrs);
          fail("Expected a IllegalStateException to be thrown");
        } catch (final IllegalStateException expected) {
          assertTrue(expected.getMessage().contains(
              PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
        } finally {
          getCache().getLogger().info(
              "<ExpectedException action=remove>" + "IllegalStateException</ExpectedException>");
        }

        // Assert different algo is invalid
        try {
          getCache().getLogger()
              .info("<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
          assertTrue(!((EvictionAttributesImpl) firstEvictionAttributes).isLIFOMemory());
          final EvictionAttributes badEa =
              EvictionAttributesImpl.createLIFOMemoryAttributes(100, EvictionAction.LOCAL_DESTROY);
          assertTrue(!badEa.equals(firstEvictionAttributes));
          factory.setEvictionAttributes(badEa);
          createRootRegion(name, factory.create());
          fail("Test to validate the Eviction Attribute : HeapLRU Interval");
        } catch (final IllegalStateException expected) {
          assertTrue(expected.getMessage().contains(
              PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
        } finally {
          getCache().getLogger().info(
              "<ExpectedException action=remove>" + "IllegalStateException</ExpectedException>");
        }

        // Assert different action is invalid
        try {
          getCache().getLogger()
              .info("<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
          assertTrue(!firstEvictionAttributes.getAction().isOverflowToDisk());
          assertTrue(firstEvictionAttributes.getAlgorithm().isLRUMemory());
          final EvictionAttributes badEa =
              EvictionAttributes.createLRUMemoryAttributes(firstEvictionAttributes.getMaximum(),
                  firstEvictionAttributes.getObjectSizer(), EvictionAction.OVERFLOW_TO_DISK);
          assertTrue(!badEa.equals(firstEvictionAttributes));
          factory.setEvictionAttributes(badEa);
          createRootRegion(name, factory.create());
          fail("Test to validate the Eviction Attribute : HeapLRU Interval");
        } catch (final IllegalStateException expected) {
          assertTrue(expected.getMessage().contains(
              PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
        } finally {
          getCache().getLogger().info(
              "<ExpectedException action=remove>" + "IllegalStateException</ExpectedException>");
        }

        // Assert a smaller eviction maximum than the global is not valid (bug 40419)
        {
          assertTrue(firstEvictionAttributes.getAlgorithm().isLRUMemory());
          final EvictionAttributes okEa =
              EvictionAttributes.createLRUMemoryAttributes(firstEvictionAttributes.getMaximum() - 1,
                  firstEvictionAttributes.getObjectSizer(), firstEvictionAttributes.getAction());
          if (!this.isAccessor) {
            assertTrue(okEa.getMaximum() < pra.getLocalMaxMemory());
          }
          assertTrue(!okEa.equals(firstEvictionAttributes));
          factory.setEvictionAttributes(okEa);
          final RegionAttributes attrs = factory.create();
          final Region pr = createRootRegion(name, attrs);
          assertNotNull(pr);
          assertNotSame(okEa, pr.getAttributes().getEvictionAttributes());
          assertEquals(firstEvictionAttributes.getMaximum(),
              pr.getAttributes().getEvictionAttributes().getMaximum());
          pr.localDestroyRegion();
        }

        // Assert that a larger eviction maximum than the global is not
        // valid (bug 40419)
        {
          assertTrue(firstEvictionAttributes.getAlgorithm().isLRUMemory());
          final EvictionAttributes okEa =
              EvictionAttributes.createLRUMemoryAttributes(firstEvictionAttributes.getMaximum() + 1,
                  firstEvictionAttributes.getObjectSizer(), firstEvictionAttributes.getAction());
          assertTrue(!okEa.equals(firstEvictionAttributes));
          if (!this.isAccessor) {
            assertEquals(okEa.getMaximum(), pra.getLocalMaxMemory() + 1);
          }
          factory.setEvictionAttributes(okEa);
          final RegionAttributes attrs = factory.create();
          final Region pr = createRootRegion(name, attrs);
          assertNotNull(pr);
          assertNotSame(okEa, pr.getAttributes().getEvictionAttributes());
          assertEquals(firstEvictionAttributes.getMaximum(),
              pr.getAttributes().getEvictionAttributes().getMaximum());
          pr.localDestroyRegion();
        }

        // Assert that an eviction maximum larger than the
        // localMaxMemroy is allowed
        // and that the local eviction maximum is re-set to the
        // localMaxMemory value
        {
          // lower the localMaxMemory
          pra = createPartitionAttributes(firstEvictionAttributes.getMaximum() - 1);
          factory.setPartitionAttributes(pra);
          if (!this.isAccessor) {
            assertTrue(firstEvictionAttributes.getMaximum() > pra.getLocalMaxMemory());
          }
          factory.setEvictionAttributes(firstEvictionAttributes);
          final RegionAttributes attrs = factory.create();
          final Region pr = createRootRegion(name, attrs);
          assertNotNull(pr);
          if (!this.isAccessor) {
            assertEquals(pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(),
                pr.getAttributes().getEvictionAttributes().getMaximum());
          }
          pr.localDestroyRegion();
        }

        // Assert exact same attributes is valid
        {
          factory.setEvictionAttributes(firstEvictionAttributes);
          final Region pr = createRootRegion(name, factory.create());
          assertNotNull(pr);
          assertNotSame(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
          assertEquals(pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(),
              pr.getAttributes().getEvictionAttributes().getMaximum());
          pr.localDestroyRegion();
        }
      }
    }

    testDatastore.invoke(new PRLRUMemoryRunnable(false));
    // testAccessor.invoke(new PRLRUMemoryRunnable(true));
  }


  @Test
  public void testEvictionValidationForLRUEntry_AccessorFirst() {
    final Host host = Host.getHost(0);
    final VM firstAccessor = host.getVM(0);
    final VM testAccessor = host.getVM(1);
    final VM testDatastore = host.getVM(2);
    final VM firstDatastore = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxEntries = 226;
    final String name = uniqName + "-PR";

    final EvictionAttributes firstEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    final EvictionAttributes secondEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.OVERFLOW_TO_DISK);

    final SerializableRunnable createFirstAccessor =
        new CacheSerializableRunnable("Create an accessor without eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createFirstDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };

    final SerializableRunnable createSecondAccessor =
        new CacheSerializableRunnable("Create an accessor with incorrect eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(secondEvictionAttrs);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createSecondDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    firstAccessor.invoke(createFirstAccessor);
    firstDatastore.invoke(createFirstDataStore);
    testAccessor.invoke(createSecondAccessor);
    testDatastore.invoke(createSecondDataStore);
  }

  @Test
  public void testEvictionValidationForLRUEntry_DatastoreFirst() {
    final Host host = Host.getHost(0);
    final VM firstAccessor = host.getVM(0);
    final VM testAccessor = host.getVM(1);
    final VM testDatastore = host.getVM(2);
    final VM firstDatastore = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxEntries = 226;
    final String name = uniqName + "-PR";

    final EvictionAttributes firstEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    final EvictionAttributes secondEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.OVERFLOW_TO_DISK);

    final SerializableRunnable createFirstAccessor =
        new CacheSerializableRunnable("Create an accessor without eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createFirstDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };

    final SerializableRunnable createSecondAccessor =
        new CacheSerializableRunnable("Create an accessor with incorrect eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(secondEvictionAttrs);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createSecondDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    firstDatastore.invoke(createFirstDataStore);
    firstAccessor.invoke(createFirstAccessor);
    testDatastore.invoke(createSecondDataStore);
    testAccessor.invoke(createSecondAccessor);
  }

  @Test
  public void testEvictionValidationForLRUEntry_TwoAccessors() {
    final Host host = Host.getHost(0);
    final VM firstAccessor = host.getVM(0);
    final VM testAccessor = host.getVM(1);
    final VM testDatastore = host.getVM(2);
    final VM firstDatastore = host.getVM(3);
    final String uniqName = getUniqueName();
    final int redundantCopies = 1;
    final int maxEntries = 226;
    final String name = uniqName + "-PR";

    final EvictionAttributes firstEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    final SerializableRunnable createFirstAccessor =
        new CacheSerializableRunnable("Create an accessor without eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createFirstDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };

    final SerializableRunnable createSecondAccessor =
        new CacheSerializableRunnable("Create an accessor with correct eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);

          }
        };
    final SerializableRunnable createSecondDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          public void run2() {
            final PartitionAttributes pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final Region pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    firstAccessor.invoke(createFirstAccessor);
    testAccessor.invoke(createSecondAccessor);

    firstDatastore.invoke(createFirstDataStore);
    testDatastore.invoke(createSecondDataStore);
  }

  /**
   * Test that gets do not need to acquire a lock on the region entry when LRU is enabled. This is
   * bug 42265
   */
  @Test
  public void testEntryLRUDeadlock() {
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final String uniqName = getUniqueName();
    final int redundantCopies = 0;
    final int maxBuckets = 8;
    final int maxEntries = 16;
    final String name = uniqName + "-PR";
    final int extraEntries = 4;

    // final int heapPercentage = 66;
    // final int evictorInterval = 100;

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      public void run2() {
        final AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(redundantCopies).setTotalNumBuckets(maxBuckets).create());
        factory.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY));
        final PartitionedRegion pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm0.invoke(create);

    final SerializableRunnable create2 =
        new SerializableRunnable("Create Entry LRU with local destroy on a partitioned Region") {
          public void run() {
            try {
              final AttributesFactory factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(
                  new PartitionAttributesFactory().setRedundantCopies(redundantCopies)
                      .setLocalMaxMemory(0).setTotalNumBuckets(maxBuckets).create());
              factory
                  .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries));
              factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
              // this listener will cause a deadlock if the get ends up
              // locking the entry.
              factory.addCacheListener(new CacheListenerAdapter() {

                @Override
                public void afterCreate(EntryEvent event) {
                  Region region = event.getRegion();
                  Object key = event.getKey();
                  region.get(key);
                }

              });
              final PartitionedRegion pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              assertNotNull(pr);
            } catch (final CacheException ex) {
              Assert.fail("While creating Partitioned region", ex);
            }
          }
        };
    vm1.invoke(create2);

    final SerializableRunnable doPuts = new SerializableRunnable("Do Puts") {
      public void run() {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (int counter = 0; counter <= maxEntries + extraEntries; counter++) {
          pr.put(new Integer(counter), "value");
        }
      }
    };
    vm0.invoke(doPuts);
  }

  protected void setEvictionPercentage(float percentage) {
    getCache().getResourceManager().setEvictionHeapPercentage(percentage);
  }

  protected boolean isOffHeap() {
    return false;
  }

  protected ResourceType getMemoryType() {
    return ResourceType.HEAP_MEMORY;
  }

  protected HeapEvictor getEvictor(Region region) {
    return ((GemFireCacheImpl) region.getRegionService()).getHeapEvictor();
  }
}
