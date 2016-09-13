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

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;

/**
 * This test verifies the size API for 100 thousand put operations (done
 * synch/asynch) on PartitionedRegions with different combinations of Scope and
 * Redundancy (Scope DIST_ACK, Redundancy 1 AND Scope DIST_NO_ACK, Redundancy
 * 0).
 * 
 *  
 */
@Category(DistributedTest.class)
public class PartitionedRegionSizeDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  public PartitionedRegionSizeDUnitTest() {
    super();
  }

  public static final String PR_PREFIX = "PR";

  static final Boolean value = new Boolean(true);

  final static int MAX_REGIONS = 1;

  final static int cnt = 100;
  
  final int totalNumBuckets = 5;
  /**
   * This method creates Partitioned Region (Scope DIST_ACK, Redundancy = 1)
   * with DataStores on 3 VMs and only accessor on 4th VM. Then it does put
   * operations synchronosly and checks that size is matched.
   * 
   * @throws Exception
   */
  public void sizeOpsForDistAckSync() throws Exception
  {

    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    CacheSerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegions") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX
              + "DistAckSync" + i, createRegionAttributesForPR(1, 200));
        }
      }
    };

    CacheSerializableRunnable createAccessor = new CacheSerializableRunnable(
        "createAccessor") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX
              + "DistAckSync" + i, createRegionAttributesForPR(1, 0));
        }
      }
    };

    // Create PRs with dataStore on 3 VMs
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    vm2.invoke(createPRs);

    // Create only accessor on 4th VM
    vm3.invoke(createAccessor);

    // Do put operations on PR synchronosly.
    vm3.invoke(new CacheSerializableRunnable("doPutOperations") {
      public void run2()
      {
        Cache cache = getCache();
        final int oldLevel = setLogLevel(LogWriterUtils.getLogWriter(), InternalLogWriter.WARNING_LEVEL);
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX
              + "DistAckSync" + j);
          assertNotNull(pr);
          for (int k = 0; k < cnt; k++) {
            Object key = new Integer(k);
            pr.put(key, value);
          }
        }
        setLogLevel(LogWriterUtils.getLogWriter(), oldLevel);

      }
    });

    // Validate the size against the total put operations
    vm3.invoke(new CacheSerializableRunnable("validateSize") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX
              + "DistAckSync" + j);
          assertNotNull(pr);
          assertEquals("size not matching=", cnt, pr.size());
        }
      }
    });
    
    // destroying Regions created 
    vm3.invoke(new CacheSerializableRunnable("destroyRegion") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX
              + "DistAckSync" + j);
          assertNotNull(pr);
          pr.destroyRegion();
          
        }
      }
    });
    
  }

  /**
   * This method creates Partitioned Region (Scope DIST_ACK, Redundancy = 1)
   * with DataStores on 3 VMs and only accessor on 4th VM. Then it does put
   * operations Asynchronosly and checks that size is matched.
   * 
   * @throws Exception
   */
  public void sizeOpsForDistAckASync() throws Throwable
  {

    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    CacheSerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegions") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX
              + "DistAckASync" + i, createRegionAttributesForPR(1, 200));
        }
      }
    };

    CacheSerializableRunnable createAccessor = new CacheSerializableRunnable(
        "createAccessor") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX
              + "DistAckASync" + i, createRegionAttributesForPR(1, 0));
        }
      }
    };

    // Create PRs with dataStore on 3 VMs
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    vm2.invoke(createPRs);

    // Create only accessor on 4th VM
    vm3.invoke(createAccessor);

    //  Do put operations on these PR asynchronosly.
    AsyncInvocation async0;

    async0 = vm3.invokeAsync(new CacheSerializableRunnable("doPutOperations") {
      public void run2()
      {
        Cache cache = getCache();
        final int oldLevel = setLogLevel(LogWriterUtils.getLogWriter(), InternalLogWriter.WARNING_LEVEL);
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX
              + "DistAckASync" + j);
          assertNotNull(pr);
          for (int k = 0; k < cnt; k++) {
            Object key = new Integer(k);
            pr.put(key, value);
          }
        }
        setLogLevel(LogWriterUtils.getLogWriter(), oldLevel);
      }
    });

    ThreadUtils.join(async0, 30 * 1000);

	if (async0.exceptionOccurred()) {
          Assert.fail("Exception during async0", async0.getException());
	}
				   
    
	
	// Validate the size against the total put operations
    vm3.invoke(new CacheSerializableRunnable("validateSize") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX
              + "DistAckASync" + j);
          assertNotNull(pr);
          assertEquals("size not matching=", cnt, pr.size());
        }
      }
    });
    
    // destroying regions at end
    vm3.invoke(new CacheSerializableRunnable("destroyRegion") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX
              + "DistAckASync" + j);
          assertNotNull(pr);
          pr.destroyRegion();
          
        }
      }
    });
  }


  /**
   * This method creates Partitioned Region (Scope DIST_ACK, Redundancy = 1)
   * with DataStores on 2 VMs and then it does put operations synchronosly.
   * After that it brings up two VMs with datastore again and does size
   * validation. After that it disconnects first and last VM and validates size
   * again.
   * 
   * @throws Exception
   */
  public void sizeOpsForDistAckSyncChangingVMCount() throws Exception
  {

    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    CacheSerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegions") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
          for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX
              + "DistAckSyncChangingVMCount" + i, createRegionAttributesForPR(
              2, 200));
        }
      }
    };

    CacheSerializableRunnable disconnectVM = new CacheSerializableRunnable(
        "disconnectVM") {
      public void run2()
      {
        Cache cache = getCache();
        DistributedSystem ds = cache.getDistributedSystem();
        ds.disconnect();
      }
    };

    // Create PRs with dataStore on 4 VMs
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);

    //  Do put operations on these PR synchronosly.
    vm0.invoke(new CacheSerializableRunnable("doPutOperations") {
      public void run2()
      {
        Cache cache = getCache();
        final int oldLevel = setLogLevel(LogWriterUtils.getLogWriter(), InternalLogWriter.WARNING_LEVEL);
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX
              + "DistAckSyncChangingVMCount" + j);
          assertNotNull(pr);
          for (int k = 0; k < cnt; k++) {
            Object key = new Integer(k);
            pr.put(key, value);
          }
        }
        setLogLevel(LogWriterUtils.getLogWriter(), oldLevel);
      }
    });

    vm2.invoke(createPRs);
    vm3.invoke(createPRs);

    // Validate the size against the total put operations
    vm3.invoke(new CacheSerializableRunnable("validateSize") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX
              + "DistAckSyncChangingVMCount" + j);
          assertNotNull(pr);
          assertEquals("size not matching=", cnt, pr.size());
        }
      }
    });

    vm0.invoke(disconnectVM);
    vm3.invoke(disconnectVM);

    // Validate the size against the total put operations
    vm1.invoke(new CacheSerializableRunnable("validateSize") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX
              + "DistAckSyncChangingVMCount" + j);
          assertNotNull(pr);
          assertEquals("size not matching=", cnt, pr.size());
        }
      }
    });

  }

  /**
   * This test method invokes methods doing size validation on PRs.
   * 
   * @throws Exception
   */
  @Test
  public void testSize() throws Throwable
  {
    sizeOpsForDistAckSync();
    sizeOpsForDistAckASync();
  }
  
  @Test
  public void testBug39868() throws Exception {
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    SerializableRunnable createPRs = new SerializableRunnable(
        "createPrRegion") {
      public void run() throws CacheException
      {
        Cache cache = getCache();
        Region partitionedregion = cache.createRegion(PR_PREFIX, createRegionAttributesForPR(1, 200));
      }
    };
    vm0.invoke(createPRs);
    
    vm0.invoke(new SerializableRunnable("create data") {
      public void run() {
        Cache cache = getCache();
        Region partitionedregion = cache.getRegion(PR_PREFIX);
        for(int i = 0; i < 100; i++) {
          //just to be tricky, put everything in the same bucket
          partitionedregion.put(Integer.valueOf(i) * totalNumBuckets, new byte[100]);
        }
      }
    });
    
    vm1.invoke(createPRs);
    
    vm0.invoke(new SerializableRunnable("delete data") {
      public void run() {
        Cache cache = getCache();
        Region partitionedregion = cache.getRegion(PR_PREFIX);
        for(int i = 0; i < 100; i++) {
          partitionedregion.destroy(Integer.valueOf(i) * totalNumBuckets);
        }
      }
    });
    
    vm1.invoke(new SerializableRunnable("check size") {

      public void run() {
        Cache cache = getCache();
        PartitionedRegion partitionedregion = (PartitionedRegion) cache.getRegion(PR_PREFIX);
        long bytes = partitionedregion.getDataStore().currentAllocatedMemory();
        assertEquals(0, bytes);
      }
    });
  }
  
  @Test
  public void testByteSize() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    SerializableRunnable createPRs = new SerializableRunnable(
    "createPrRegion") {
      public void run() throws CacheException
      {
        Cache cache = getCache();
        Region partitionedregion = cache.createRegion(PR_PREFIX, createRegionAttributesForPR(1, 200));
      }
    };
    final long oneItemSize = runProportionalSize(createPRs);
    
    SerializableRunnable checkMemSize= new SerializableRunnable(
    "checkMemSize") {
      public void run() throws CacheException
      {
        Cache cache = getCache();
        PartitionedRegion partitionedregion = (PartitionedRegion) cache.getRegion(PR_PREFIX);
        PartitionedRegionDataStore dataStore = partitionedregion.getDataStore();
        assertEquals(50 * oneItemSize, dataStore.currentAllocatedMemory());
      }
    };
  }
  
  @Test
  public void testByteSizeWithEviction() throws Exception {
    Host host = Host.getHost(0);

    final String uniqueName = getUniqueName();
    SerializableRunnable createPRs = new SerializableRunnable(
        "createPrRegion") {
      public void run() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        PartitionAttributes prAttr = paf.setRedundantCopies(1)
            .setLocalMaxMemory(200)
            .setTotalNumBuckets(totalNumBuckets)
            .create();
        attr.setPartitionAttributes(prAttr);
        attr.setEvictionAttributes(EvictionAttributes
            .createLRUEntryAttributes(2,
                EvictionAction.OVERFLOW_TO_DISK));
        final File[] diskDirs = new File[1];
        diskDirs[0] = new File("overflowDir/" + uniqueName + "_"
            + OSProcess.getId());
        diskDirs[0].mkdirs();
        attr.setDiskSynchronous(true);
        attr.setDiskStoreName(cache.createDiskStoreFactory()
                              .setDiskDirs(diskDirs)
                              .create("PartitionedRegionSizeDUnitTest")
                              .getName());
        // why isn't attr used after this?
        Region partitionedregion = cache.createRegion(PR_PREFIX, createRegionAttributesForPR(1, 200));
      }
    };
    final long oneItemSize = runProportionalSize(createPRs);

    SerializableRunnable checkMemSize= new SerializableRunnable(
        "checkMemSize") {
      public void run() throws CacheException
      {
        Cache cache = getCache();
        PartitionedRegion partitionedregion = (PartitionedRegion) cache.getRegion(PR_PREFIX);
        PartitionedRegionDataStore dataStore = partitionedregion.getDataStore();
        
        //there should only be 2 items in memory
        assertEquals(2 * oneItemSize, dataStore.currentAllocatedMemory());

        //fault something else into memory and check again.
        partitionedregion.get(Long.valueOf(82 * totalNumBuckets));
        assertEquals(2 * oneItemSize, dataStore.currentAllocatedMemory());
        assertEquals(50 * oneItemSize, dataStore.getBucketSize(0));
      }
    };
  }
  
  public long runProportionalSize(SerializableRunnable createPRs) throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    
    final Long oneItemSize = (Long) vm0.invoke(new SerializableCallable("create data") {
      public Object call() {
        Cache cache = getCache();
        PartitionedRegion partitionedregion = (PartitionedRegion) cache.getRegion(PR_PREFIX);
        PartitionedRegionDataStore dataStore = partitionedregion.getDataStore();
          partitionedregion.put(Integer.valueOf(0), new byte[100]);
          long oneItemSize = dataStore.getBucketSize(0);
        for(int i = 1; i < 100; i++) {
          partitionedregion.put(Integer.valueOf(i * totalNumBuckets), new byte[100]);
        }
        assertEquals(1, dataStore.getBucketsManaged());
        //make sure the size is proportional to the amount of data
        assertEquals(100 * oneItemSize, dataStore.getBucketSize(0));
        
        //destroy and invalidate entries and make sure the size goes down
        for(int i = 0; i < 25; i++) {
          partitionedregion.destroy(Integer.valueOf(i * totalNumBuckets));
        }
        
        for(int i = 25; i < 50; i++) {
          partitionedregion.invalidate(Integer.valueOf(i * totalNumBuckets));
        }
        
        assertEquals(50 * oneItemSize, dataStore.getBucketSize(0));
        
        //put some larger values in and make sure the size goes up
        for(int i = 50; i < 75; i++) {
          partitionedregion.put(Integer.valueOf(i * totalNumBuckets), new byte[150]);
        }
        
        //Now put in some smaller values and see if the size balances
        //out
        for(int i = 75; i < 100; i++) {
          partitionedregion.put(Integer.valueOf(i * totalNumBuckets), new byte[50]);
        }
        
        assertEquals(50 * oneItemSize, dataStore.getBucketSize(0));
        
        return Long.valueOf(oneItemSize);
      }
    });
    
    
    vm1.invoke(new SerializableRunnable("check size") {

      public void run() {
        Cache cache = getCache();
        PartitionedRegion partitionedregion = (PartitionedRegion) cache.getRegion(PR_PREFIX);
        long bytes = partitionedregion.getDataStore().getBucketSize(0);
        assertEquals(50 * oneItemSize.longValue(), bytes);
      }
    });
    
    return oneItemSize.longValue();
  }


  /**
   * This private methods sets the passed attributes and returns RegionAttribute
   * object, which is used in create region
   * @param redundancy
   * @param localMaxMem
   * 
   * @return
   */
  protected RegionAttributes createRegionAttributesForPR(int redundancy,
      int localMaxMem) {
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy)
        .setLocalMaxMemory(localMaxMem)
        .setTotalNumBuckets(totalNumBuckets)
        .create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }
}
