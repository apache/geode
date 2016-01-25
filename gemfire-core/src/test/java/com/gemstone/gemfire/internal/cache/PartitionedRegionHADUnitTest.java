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
package com.gemstone.gemfire.internal.cache;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserver;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author tapshank, Created on Jan 17, 2006
 *  
 */
public class PartitionedRegionHADUnitTest extends PartitionedRegionDUnitTestCase
{

  //////constructor //////////
  public PartitionedRegionHADUnitTest(String name) {

    super(name);
  }//end of constructor

  public static final String PR_PREFIX = "PR";

  Properties props = new Properties();

  volatile static int regionCnt = 0;

  final static int MAX_REGIONS = 1;

  final int totalNumBuckets = 5;
  
  /**
   * Test to ensure that we have proper bucket failover, with no data loss, in the face
   * of sequential cache.close() events.
   * @throws Exception
   */
  public void testBucketFailOverDuringCacheClose() throws Exception {
    
    final String regionName = getUniqueName();
    final Boolean value = new Boolean(true);
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    CacheSerializableRunnable createPR = new CacheSerializableRunnable(
    "createRegion") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        final CountDownLatch rebalancingFinished = new CountDownLatch(1);
        InternalResourceManager.setResourceObserver(new ResourceObserverAdapter(){

          @Override
          public void rebalancingOrRecoveryFinished(Region region) {
            rebalancingFinished.countDown();
          }
          
        });
        try {
          Region partitionedregion = cache.createRegion(regionName, 
              createRegionAttributesForPR(1, 20));
          if(!rebalancingFinished.await(60000, TimeUnit.MILLISECONDS)) {
            fail("Redundancy recovery did not happen within 60 seconds");
          }
          assertNotNull(partitionedregion);
        } catch (InterruptedException e) {
          fail("interrupted",e);
        } finally {
          InternalResourceManager.setResourceObserver(null);
        }
      }
    };
    vm2.invoke(createPR);
    vm3.invoke(createPR);
    
    vm3.invoke(
        new CacheSerializableRunnable(
        "createPRBuckets") {
          public void run2() throws CacheException
          {
            Cache cache = getCache();
            PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
            assertTrue(pr.isEmpty());
            Integer k;
            // Create keys such that all buckets are created, Integer works well
            // assuming buckets are allocated on the mod of the key hashCode, x 2 just to be safe
            final int numEntries=pr.getTotalNumberOfBuckets()*2; 
            for(int i=numEntries; i>=0; --i) {
              k = new Integer(i);
              pr.put(k, value);              
            }
            assertEquals(numEntries+1,pr.size());
            assertEquals(pr.getRegionAdvisor().getBucketSet().size(), pr.getTotalNumberOfBuckets());

          }
        }
    );

    CacheSerializableRunnable existsEntryCheck = new CacheSerializableRunnable(
    "PRExistsEntryCheck") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
        Integer k;
        for(int i=pr.getTotalNumberOfBuckets()*2; i>=0; --i) {
          k=new Integer(i);
          assertTrue("containsKey for key="+k, pr.containsKey(k));
          assertEquals("get for key="+k, value, pr.get(k));
        }
      }
    };
    vm3.invoke(existsEntryCheck);
    vm2.invoke(existsEntryCheck);

    CacheSerializableRunnable closeCache = new CacheSerializableRunnable(
    "PRCloseCache") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        cache.close();
      }
    };
    
    // origin VM down!
    vm2.invoke(closeCache);
    // origin down, but no data loss
    vm3.invoke(existsEntryCheck);
    
    // get back to the desired redundancy
    vm0.invoke(createPR);
    // verify no data loss
    vm0.invoke(existsEntryCheck);
    
    // 2nd oldest VM down!
    vm3.invoke(closeCache);
    // 2nd down, but no data loss
    vm0.invoke(existsEntryCheck);

    // get back (for 2nd time) to desired redundancy
    vm1.invoke(createPR);
    // verify no data loss
    vm1.invoke(existsEntryCheck);
    vm0.invoke(existsEntryCheck);
  }

  //////////test methods ////////////////
  public void testGrabBackupBuckets() throws Throwable
  {

    Host host = Host.getHost(0);
    VM dataStore0 = host.getVM(0);
    // VM dataStore1 = host.getVM(1);
    VM dataStore2 = host.getVM(2);
    VM accessor = host.getVM(3);
    final int redundantCopies = 1;
    // Create PRs On 2 VMs
    CacheSerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegions") {

      public void run2() throws CacheException
      {
        final CountDownLatch recoveryDone = new CountDownLatch(MAX_REGIONS);
        ResourceObserver waitForRecovery = new ResourceObserverAdapter() {
          @Override
          public void rebalancingOrRecoveryFinished(Region region) {
            recoveryDone.countDown();
          }
        };
        InternalResourceManager.setResourceObserver(waitForRecovery);
        try {
          Cache cache = getCache();
          System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, "20000");
          for (int i = 0; i < MAX_REGIONS; i++) {
            cache.createRegion(PR_PREFIX + i,
                createRegionAttributesForPR(redundantCopies, 200));
          }
          System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, 
              Integer.toString(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
          if(!recoveryDone.await(60, TimeUnit.SECONDS)) {
            fail("recovery didn't happen in 60 seconds");
          }
        } catch (InterruptedException e) {
          fail("recovery wait interrupted", e);
        } finally {
          InternalResourceManager.setResourceObserver(null);
        }
      }
    };
    CacheSerializableRunnable createAccessor = new CacheSerializableRunnable(
        "createAccessor") {

      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX + i,
              createRegionAttributesForPR(redundantCopies, 0));
        }
      }
    };
    // Create PRs on only 2 VMs
    dataStore0.invoke(createPRs);
    // dataStore1.invoke(createPRs);
    final String expectedExceptions = PartitionedRegionStorageException.class.getName();
    SerializableRunnable addExpectedExceptions = 
      new CacheSerializableRunnable("addExpectedExceptions") {
        public void run2() throws CacheException {
          getCache().getLogger().info("<ExpectedException action=add>" + 
              expectedExceptions + "</ExpectedException>");
          getLogWriter().info("<ExpectedException action=add>" + 
                  expectedExceptions + "</ExpectedException>");
        }
      };
    SerializableRunnable removeExpectedExceptions = 
      new CacheSerializableRunnable("removeExpectedExceptions") {
        public void run2() throws CacheException {
          getLogWriter().info("<ExpectedException action=remove>" + 
                    expectedExceptions + "</ExpectedException>");	
          getCache().getLogger().info("<ExpectedException action=remove>" + 
              expectedExceptions + "</ExpectedException>");
        }
      };

    // Do put operations on these 2 PRs asynchronosly.
    CacheSerializableRunnable dataStore0Puts = new CacheSerializableRunnable("dataStore0PutOperations") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX + j);
          assertNotNull(pr);
          for (int k = 0; k < 10; k++) {
            pr.put(j + PR_PREFIX + k, PR_PREFIX + k);
          }
          getLogWriter().info("VM0 Done put successfully for PR = " + PR_PREFIX
              + j);
        }
      }
    };
    
    CacheSerializableRunnable dataStore1Puts = new CacheSerializableRunnable("dataStore1PutOperations") { // TODO bug36296
      public void run2()
      {

        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX + (j));
          assertNotNull(pr);
          for (int k = 10; k < 20; k++) {
            pr.put(j + PR_PREFIX + k, PR_PREFIX + k);
          }
          getLogWriter().info("VM1 Done put successfully for PR = " + PR_PREFIX
              + j);
        }
      }
    };
    dataStore0.invoke(addExpectedExceptions);
    // dataStore1.invoke(addExpectedExceptions);
    AsyncInvocation async0 = dataStore0.invokeAsync(dataStore0Puts);
    // AsyncInvocation  async1 = dataStore1.invokeAsync(dataStore1Puts);
    DistributedTestCase.join(async0, 30 * 1000, getLogWriter());
    // async1.join();
    dataStore0.invoke(removeExpectedExceptions);
    // dataStore1.invoke(removeExpectedExceptions);
    
    // Verify that buckets can not be created if there are not enough Nodes to support
    // the redundancy Configuration
    assertFalse(async0.exceptionOccurred());
    // assertTrue(async0.getException() instanceof PartitionedRegionStorageException);
    // assertTrue(async1.exceptionOccurred());
    // assertTrue(async1.getException() instanceof PartitionedRegionStorageException);

    // At this point redundancy criterion is not meet.
    // now if we create PRs on more VMs, it should create those "supposed to
    // be redundant" buckets on these nodes, if it can accommodate the data
    // (localMaxMemory>0).
    dataStore2.invoke(createPRs);
    
    async0 = dataStore0.invokeAsync(dataStore0Puts);
    // async1 = dataStore1.invokeAsync(dataStore1Puts);
    DistributedTestCase.join(async0, 30 * 1000, getLogWriter());
    // async1.join();
    
    if (async0.exceptionOccurred()) {
      fail("async0 failed", async0.getException());
    }
    // assertFalse(async1.exceptionOccurred());
    
    accessor.invoke(createAccessor);
    
    for (int c = 0; c < MAX_REGIONS; c++) {
      final Integer ri = new Integer(c);
      final SerializableCallable validateLocalBucket2RegionMapSize = 
        new SerializableCallable("validateLocalBucket2RegionMapSize") {
        public Object call() throws Exception {
          int size = 0;
          Cache cache = getCache();
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + PR_PREFIX + ri.intValue());
          if (pr.getDataStore() != null) {
            size = pr.getDataStore().getBucketsManaged();
          }
          return new Integer(size);
        }
      };
      final SerializableCallable validateBucketsOnNode = 
          new SerializableCallable("validateBucketOnNode") {
        public Object call() throws Exception {
          int containsNode = 0;
          Cache cache = getCache();
          PartitionedRegion pr = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + PR_PREFIX + ri.intValue());

          Iterator it = pr.getRegionAdvisor().getBucketSet().iterator();
          Set nodeList;
          try { 
            while (it.hasNext()) {
              Integer bucketId = (Integer) it.next();
              nodeList = pr.getRegionAdvisor().getBucketOwners(bucketId.intValue());
              if ((nodeList != null) && (nodeList.contains(pr.getMyId()))) {
                containsNode++;
              }
              else {
                getCache().getLogger().fine("I don't contain member " + pr.getMyId());
              }
            }
          } catch (NoSuchElementException done) {
          }

          return new Integer(containsNode);
        }
      };
 
//      int vm0LBRsize = ((Integer)dataStore0.invoke(validateLocalBucket2RegionMapSize)).intValue();
      int vm2LBRsize = ((Integer)dataStore2.invoke(validateLocalBucket2RegionMapSize)).intValue();
      int vm3LBRsize = ((Integer)accessor.invoke(validateLocalBucket2RegionMapSize)).intValue();
      // This would mean that up coming node didn't pick up any buckets
      assertFalse(vm2LBRsize == 0);
      // This accessor should NOT have picked up any buckets.
      assertFalse(vm3LBRsize != 0);
      int vm2B2Nsize = ((Integer)dataStore2.invoke(validateBucketsOnNode)).intValue();
      getLogWriter().info("vm2B2Nsize = " + vm2B2Nsize);
      assertEquals(vm2B2Nsize, vm2LBRsize);
    }
  }

  /**
   * This verifies the Bucket Regions on the basis of
   * redundantCopies set in RegionAttributes. 
   * @see PartitionedRegionSingleNodeOperationsJUnitTest#testBucketScope()
   * @throws Exception
   */
  public void testBucketsScope() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String PR_ZeroRedundancy = "PR_ZeroRedundancy";
    final String PR_SingleRedundancy = "PR_SingleRedundancy";
    // Create PRs On 2 VMs
    CacheSerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegionWithZeroRed") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();

        // RedundantCopies = 0 , Scope = DISTRIBUTED_ACK
        cache.createRegion(PR_ZeroRedundancy, createRegionAttributesForPR(
            0, 200));
        // RedundantCopies > 0 , Scope = DISTRIBUTED_ACK
        cache.createRegion(PR_SingleRedundancy, createRegionAttributesForPR(1,
            200));
      }
    };

    // Create PRs on only 2 VMs
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    // Do put operations on these 2 PRs asynchronosly.

    vm0.invoke(new CacheSerializableRunnable("doPutOperations") {
      public void run2()
      {
        Cache cache = getCache();
        String regionName = PR_ZeroRedundancy;
        Region pr = cache.getRegion(Region.SEPARATOR + regionName);
        assertNotNull(pr);
        for (int k = 0; k < 10; k++) {
          pr.put(k + "", k + "");
        }
        cache.getLogger().fine(
            "VM0 Done put successfully for PR = " + regionName);

        regionName = PR_SingleRedundancy;
        Region pr1 = cache.getRegion(Region.SEPARATOR + regionName);
        assertNotNull(pr1);
        for (int k = 0; k < 10; k++) {
          pr1.put(k + "", k + "");
        }
        cache.getLogger().fine(
            "VM0 Done put successfully for PR = " + regionName);
      }

    });

    CacheSerializableRunnable validateBucketScope = new CacheSerializableRunnable(
        "validateBucketScope") {
      public void run2()
      {
        Cache cache = getCache();

        
        String regionName = PR_ZeroRedundancy;
        PartitionedRegion pr = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + regionName);

        java.util.Iterator buckRegionIterator = pr.getDataStore().localBucket2RegionMap
            .values().iterator();
        while (buckRegionIterator.hasNext()) {
          BucketRegion bucket = (BucketRegion)buckRegionIterator.next();
          assertTrue(bucket.getAttributes().getScope().isDistributedAck());
        }

        regionName = PR_SingleRedundancy;
        PartitionedRegion pr1 = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + regionName);

        java.util.Iterator buckRegionIterator1 = pr1.getDataStore().localBucket2RegionMap
            .values().iterator();
        while (buckRegionIterator1.hasNext()) {
          Region bucket = (Region)buckRegionIterator1.next();
          assertEquals(DataPolicy.REPLICATE, bucket.getAttributes().getDataPolicy());
        }
      }

    };
    
    vm0.invoke(validateBucketScope);
    vm1.invoke(validateBucketScope);

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
      int localMaxMem)
  {

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
