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

import java.util.concurrent.CancellationException;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * @author tapshank, Created on Jan 19, 2006
 *  
 */
public class PartitionedRegionStatsDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  //////constructor //////////
  public PartitionedRegionStatsDUnitTest(String name) {
    super(name);
  }//end of constructor

  public static final String PR_PREFIX = "PR";

  static final int cnt = 10;

  final int PUT = 1;

  final int GET = 2;

  final int CONTAINS_KEY = 3;

  final int CONTAINS_VALUE_FOR_KEY = 4;

  final int INVALIDATE = 5;

  final int DESTROY = 6;

  final int CREATE = 7;
  
  final int GET_ENTRY = 8;
  
  final int totalNumBuckets = 5;
  static final int REDUNDANT_COPIES = 1; 
  //////////test methods ////////////////
  public void testClose() throws Exception
  {

    final Host host = Host.getHost(0);

    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);

    CacheSerializableRunnable createPR = new CacheSerializableRunnable(
        "createPrRegions") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        cache.createRegion(PR_PREFIX,
            createRegionAttributesForPR(REDUNDANT_COPIES, 200));

        cache.createRegion(PR_PREFIX + "1",
            createRegionAttributesForPR(REDUNDANT_COPIES, 200));
      }
    };

    /**
     * This class creates accessors.
     */
    CacheSerializableRunnable createAccessor = new CacheSerializableRunnable(
        "createAccessor") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        cache.createRegion(PR_PREFIX, createRegionAttributesForPR(
            REDUNDANT_COPIES, 0));
        cache.createRegion(PR_PREFIX + "1", createRegionAttributesForPR(
            REDUNDANT_COPIES, 0));
      }
    };

    /**
     * This class does put, get, create, destroy, invalidate, containsKey and
     * containsValueForKey operations on PartitionedRegion.
     */
    CacheSerializableRunnable doRegionOps = new CacheSerializableRunnable(
        "doRegionOps") {
      public void run2()
      {
      }

      public void doOps(Integer opTypeInteger) throws CacheException
      {
        Cache cache = getCache();
        int opType = opTypeInteger.intValue();
        Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX);
        assertNotNull(pr);
        doRegionOpsOnPR(pr, opType);
        pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX + "1");
        assertNotNull(pr);
        doRegionOpsOnPR(pr, opType);
      }

      private void doRegionOpsOnPR(Region pr, int opType) throws CacheException
      {
        switch (opType) {
        case PUT:
          for (int k = 0; k < cnt; k++) {
            pr.put("" + k, "" + k);
          }
          break;
        case GET:
          for (int k = 0; k < cnt; k++) {
            pr.get("" + k);
          }
          break;
        case CONTAINS_KEY:
          for (int k = 0; k < cnt; k++) {
            pr.containsKey("" + k);
          }
          break;
        case CONTAINS_VALUE_FOR_KEY:
          for (int k = 0; k < cnt; k++) {
            pr.containsValueForKey("" + k);
          }
          break;
        case INVALIDATE:
          for (int k = 0; k < cnt; k++) {
            pr.invalidate("" + k);
          }
          break;
        case DESTROY:
          for (int k = 0; k < cnt; k++) {
            pr.destroy("" + k);
          }
          break;
        case CREATE:
          for (int k = 0; k < cnt; k++) {
            pr.create("1" + k, "1" + k);
          }
          break;
        case GET_ENTRY:
          for (int k = 0; k < cnt; k++) {
            pr.getEntry(Integer.toString(k));
          }

        }
        
      }
    };
    /**
     * This class disconnects the VM from DistributedSystem.
     */
    CacheSerializableRunnable disconnectVM = new CacheSerializableRunnable( // TODO bug36296
        "disconnectVM") {
      public void run2()
      {
        Cache cache = getCache();
        DistributedSystem ds = cache.getDistributedSystem();
        ds.disconnect();
      }
    };
    /**
     * This class validates the min, max and avg Redundant Copies PR Statistics
     * before disconnecting VM.
     */
    CacheSerializableRunnable validateRedundantCopiesStats = new CacheSerializableRunnable(
        "validateStats") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        PartitionedRegion pr = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + PR_PREFIX);
        assertNotNull(pr);
        Statistics stats = pr.getPrStats().getStats();
        int minRedundantCopies = stats.get("minRedundantCopies").intValue();
        int maxRedundantCopies = stats.get("maxRedundantCopies").intValue();
        int avgRedundantCopies = stats.get("avgRedundantCopies").intValue();

        assertEquals(minRedundantCopies, 1);
        assertEquals(maxRedundantCopies, 1);
        assertEquals(avgRedundantCopies, 1);
      }
    };
    /**
     * This class validates the min, max and avg redundant copies PR Statistics
     * after disconnecting VM.
     */
    CacheSerializableRunnable validateRedundantCopiesStatsAfterDisconnect = new CacheSerializableRunnable(
        "validateRedundantCopiesStatsAfterDisconnect") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        PartitionedRegion pr = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + PR_PREFIX);
        assertNotNull(pr);
        Statistics stats = pr.getPrStats().getStats();
        int minRedundantCopies = stats.get("minRedundantCopies").intValue();
        int maxRedundantCopies = stats.get("maxRedundantCopies").intValue();
        int avgRedundantCopies = stats.get("avgRedundantCopies").intValue();
        assertEquals(minRedundantCopies, 1);
        assertEquals(maxRedundantCopies, 1);
        assertEquals(avgRedundantCopies, 1);
      }
    };

    /**
     * This class validates PartitionedRegion operations related statistics.
     * PRStatistics for put, get, create, invalidate, containsKey,
     * containsValueForKey and destroys are validated.
     */
    CacheSerializableRunnable validatePartitionedRegionOpsStats = new CacheSerializableRunnable(
        "validatePartitionedRegionOpsStats") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        PartitionedRegion pr = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + PR_PREFIX);
        assertNotNull(pr);
        pr = (PartitionedRegion)cache.getRegion(Region.SEPARATOR + PR_PREFIX);
        assertNotNull(pr);
        Statistics stats = pr.getPrStats().getStats();

        int putsCompleted = stats.get("putsCompleted").intValue();
        int getsCompleted = stats.get("getsCompleted").intValue();
        int getEntrysCompleted = stats.get("getEntryCompleted").intValue();
        int createsCompleted = stats.get("createsCompleted").intValue();
        int containsKeyCompleted = stats.get("containsKeyCompleted").intValue();
        int containsValueForKeyCompleted = stats.get(
            "containsValueForKeyCompleted").intValue();
        int invalidatesCompleted = stats.get("invalidatesCompleted").intValue();
        int destroysCompleted = stats.get("destroysCompleted").intValue();

        assertEquals(cnt, putsCompleted); 
        assertEquals(cnt, getsCompleted);
        assertEquals(cnt, getEntrysCompleted);
        assertEquals(0, createsCompleted);
        assertEquals(cnt, containsKeyCompleted);
        assertEquals(cnt, containsValueForKeyCompleted);
        assertEquals(cnt, invalidatesCompleted);
        assertEquals(cnt, destroysCompleted);
        
        // TODO: Need to validate that bucket stats add up.... 
        // pr.getDataStore().getCachePerfStats().getGets();  // etc...
      }
    };

    // Create PRs on 3 VMs and accessors on 1 VM
    vm0.invoke(createPR);
    vm1.invoke(createPR);
    vm2.invoke(createPR);
    vm3.invoke(createAccessor);

    // Do Region operations.
    Object[] put = { new Integer(PUT) };
    vm0.invoke(doRegionOps, "doOps", put);

    Object[] get = { new Integer(GET) };
    vm0.invoke(doRegionOps, "doOps", get);
    
    Object[] getEntry = { new Integer(GET_ENTRY) };
    vm0.invoke(doRegionOps, "doOps", getEntry);

    Object[] containsKey = { new Integer(CONTAINS_KEY) };
    vm0.invoke(doRegionOps, "doOps", containsKey);

    Object[] containsValueForKey = { new Integer(CONTAINS_VALUE_FOR_KEY) };
    vm0.invoke(doRegionOps, "doOps", containsValueForKey);

    Object[] invalidate = { new Integer(INVALIDATE) };
    vm0.invoke(doRegionOps, "doOps", invalidate);

    Object[] destroy = { new Integer(DESTROY) };
    vm0.invoke(doRegionOps, "doOps", destroy);
    
    vm0.invoke(validatePartitionedRegionOpsStats);
    
    CacheSerializableRunnable destroyRegion = new CacheSerializableRunnable("destroyRegion")
    {
    	public void run2() throws CacheException
    	{
    	  Cache cache = getCache();
    	  PartitionedRegion pr = (PartitionedRegion)cache.getRegion(Region.SEPARATOR + PR_PREFIX + "1");
    	  assertNotNull("Null region is " + pr.getName(),pr);
    	  pr.destroyRegion();
    	  pr = (PartitionedRegion)cache.getRegion(Region.SEPARATOR + PR_PREFIX );
    	  assertNotNull("Null region is " + pr.getName(),pr);
    	  pr.destroyRegion();
    		
    	}
    };
    
    vm0.invoke(destroyRegion);
    
    /*
     * Redundant Copies related statistics validation
     * vm0.invoke(validateRedundantCopiesStats);
     * 
     * vm1.invoke(disconnectVM);
     * 
     * Thread.sleep(20000);
     * 
     * vm0.invoke(validateRedundantCopiesStatsAfterDisconnect);
     */
  }
  
  public void testDataStoreEntryCountWithRebalance() throws InterruptedException {
    //Ok, first problem, GC'd tombstone is counted as an entry
    //To test
    //   - modifying a tombstone
    //   - modifying and doing tombstone GC?
    
    final Host host = Host.getHost(0);

    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    
    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
        RebalanceOperation op = cache.getResourceManager().createRebalanceFactory().start();
        try {
          RebalanceResults results = op.getResults();
        } catch (Exception e) {
          Assert.fail("ex", e);
        }
      }
    };
    vm0.invoke(createPrRegion);

    
    vm0.invoke(new SerializableRunnable("Put some data") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Long.valueOf(0), "A");
        region.put(Long.valueOf(1), "A");
        region.put(Long.valueOf(113), "A");
        region.put(Long.valueOf(114), "A");
        region.destroy(Long.valueOf(0));
        region.destroy(Long.valueOf(1));
      }
    });
    
    vm1.invoke(createPrRegion);
    
    validateEntryCount(vm0, 1);
    validateEntryCount(vm1, 1);
  }
  
  public void testDataStoreEntryCount2WithRebalance() throws InterruptedException {
    final Host host = Host.getHost(0);

    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);

    SerializableRunnable createPrRegion = new SerializableRunnable(
        "createRegion") {
      public void run() {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
        RebalanceOperation op = cache.getResourceManager()
            .createRebalanceFactory().start();
        try {
          RebalanceResults results = op.getResults();
        }
        catch (Exception e) {
          Assert.fail("ex", e);
        }
      }
    };
    vm0.invoke(createPrRegion);

    vm0.invoke(new SerializableRunnable("Put some data") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Long.valueOf(0), "A");
        region.put(Long.valueOf(1), "A");
        region.put(Long.valueOf(2), "A");
        region.put(Long.valueOf(3), "A");
        region.put(Long.valueOf(4), "A");
        region.put(Long.valueOf(5), "A");
      }
    });

    vm1.invoke(createPrRegion);

    validateEntryCount(vm0, 3);
    validateEntryCount(vm1, 3);
  }
  
  /**
   * Test to make sure the datastore entry count is
   * accurate.
   * @throws InterruptedException 
   */
  public void testDataStoreEntryCount() throws InterruptedException {
    //Ok, first problem, GC'd tombstone is counted as an entry
    //To test
    //   - modifying a tombstone
    //   - modifying and doing tombstone GC?
    
    final Host host = Host.getHost(0);

    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    
    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(2);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    
    vm0.invoke(new SerializableRunnable("Put some data") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Long.valueOf(0), "A");
        region.put(Long.valueOf(1), "A");
        region.put(Long.valueOf(113), "A");
        region.put(Long.valueOf(226), "A");
      }
    });
  
    validateEntryCount(vm0, 4);
    validateEntryCount(vm1, 4);

    //Do a destroy
    vm0.invoke(new SerializableRunnable("Put some data") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.destroy(Long.valueOf(0));
      }
    });
    
    //We expect the tombstone won't be recorded as part of
    //the entry count
    validateEntryCount(vm0, 3);
    validateEntryCount(vm1, 3);
    
    //Destroy and modify a tombstone
    vm0.invoke(new SerializableRunnable("Put some data") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.destroy(Long.valueOf(113));
        region.put(Long.valueOf(113), "B");
      }
    });
    
    validateEntryCount(vm0, 3);
    validateEntryCount(vm1, 3);
    
    //After GII (which might include the tombstone), a new members
    //should still see only 2 live entries.
    vm2.invoke(createPrRegion);

    
    //Wait for redundancy to be restored. Once it is the entry count should be
    //2
    vm2.invoke(new SerializableRunnable("validate stats") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        final PartitionedRegionStats stats = region.getPrStats();
        Wait.waitForCriterion(new WaitCriterion() {
          
          @Override
          public boolean done() {
            return stats.getLowRedundancyBucketCount() == 0;
          }
          
          @Override
          public String description() {
            return "Redundancy was not satisfied " + stats.getLowRedundancyBucketCount();
          }
        }, 20000, 100, true);
      }
    });
    validateEntryCount(vm2, 3);
    
    //A tombstone GC shouldn't affect the count.
    vm0.invoke(new SerializableRunnable("Put some data") {
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        TombstoneService tombstoneService = cache.getTombstoneService();
        try {
          tombstoneService.forceBatchExpirationForTests(1);
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    });
    
    validateEntryCount(vm0, 3);
    validateEntryCount(vm1, 3);
    validateEntryCount(vm2, 3);
  }
  
    
  public void testTotalNumBuckets() {
	  final Host host = Host.getHost(0);
	  
	  final VM vm0 = host.getVM(0);
	  final VM vm1 = host.getVM(1);
	  
	  SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
		  public void run()
		  {
			  Cache cache = getCache();
			  AttributesFactory attr = new AttributesFactory();
			  PartitionAttributesFactory paf = new PartitionAttributesFactory();
			  paf.setRedundantCopies(2);
			  PartitionAttributes prAttr = paf.create();
			  attr.setPartitionAttributes(prAttr);
			  cache.createRegion("region1", attr.create());
		  }
	  };
	  
	  vm0.invoke(createPrRegion);
	  vm1.invoke(createPrRegion);
	  
	  SerializableRunnable putDataInRegion = new SerializableRunnable("Put some data") {
		  public void run() {
			  Cache cache = getCache();
			  Region region = cache.getRegion("region1");
			  region.put(Long.valueOf(0), "A");
			  region.put(Long.valueOf(1), "A");
			  region.put(Long.valueOf(113), "A");
			  region.put(Long.valueOf(226), "A");
		  }
	  };
	  
	  vm0.invoke(putDataInRegion);
	  vm1.invoke(putDataInRegion);
	  
	  int expectedBucketCount = 113;
	  validateTotalNumBucketsCount(vm0, expectedBucketCount);
	  validateTotalNumBucketsCount(vm1, expectedBucketCount);
  }	
  
  private void validateEntryCount(final VM vm0,
      final long expectedCount) {
    SerializableRunnable validateStats = new SerializableRunnable("validate stats") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        PartitionedRegionStats stats = region.getPrStats();
        CachePerfStats cachePerfStats = region.getCachePerfStats();
        assertEquals(expectedCount, stats.getDataStoreEntryCount());
        assertEquals(expectedCount, cachePerfStats.getEntries());
      }
    };
    vm0.invoke(validateStats);
  }
  
  private void validateTotalNumBucketsCount(final VM vm0,
	final long expectedCount) {
	  SerializableRunnable validateStats = new SerializableRunnable("validate stats") {
		  public void run() {
			  Cache cache = getCache();
			  PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
			  PartitionedRegionStats stats = region.getPrStats();
			  CachePerfStats cachePerfStats = region.getCachePerfStats();
			  assertEquals(expectedCount, stats.getTotalNumBuckets());
		  }
	  };
	  vm0.invoke(validateStats);
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
//    Properties globalProps = new Properties();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy)
        .setLocalMaxMemory(localMaxMem)
        .setTotalNumBuckets(totalNumBuckets)
        .create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }
}
