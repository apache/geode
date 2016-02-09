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
package com.gemstone.gemfire.internal.cache.control;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import com.gemstone.gemfire.internal.cache.partitioned.BucketCountLoadProbe;
import com.gemstone.gemfire.internal.cache.partitioned.LoadProbe;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * @author dsmith
 *
 */
@SuppressWarnings("synthetic-access")
public class RebalanceOperationDUnitTest extends CacheTestCase {

  private static final long MAX_WAIT = 60;
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        InternalResourceManager.setResourceObserver(null);
        System.clearProperty("gemfire.resource.manager.threads");
      }
    });
    InternalResourceManager.setResourceObserver(null);
    System.clearProperty("gemfire.resource.manager.threads");
  }

  /**
   * @param name
   */
  public RebalanceOperationDUnitTest(String name) {
    super(name);
  }
  
  public void testRecoverRedundancySimulation() {
    recoverRedundancy(true);
  }
   
  public void testRecoverRedundancy() {
    recoverRedundancy(false);
  }
  
  public void recoverRedundancy(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Integer.valueOf(1), "A");
        region.put(Integer.valueOf(2), "A");
        region.put(Integer.valueOf(3), "A");
        region.put(Integer.valueOf(4), "A");
        region.put(Integer.valueOf(5), "A");
        region.put(Integer.valueOf(6), "A");
      }
    });
    
    SerializableRunnable checkLowRedundancy = new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(6, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(6,details.getLowRedundancyBucketCount());
      }
    };
    
    //make sure we can tell that the buckets have low redundancy
    vm0.invoke(checkLowRedundancy);

    //Create the region in the other VM (should have no effect)
    vm1.invoke(createPrRegion);
    
    //Make sure we still have low redundancy
    vm0.invoke(checkLowRedundancy);
    
    //Now simulate a rebalance
    vm0.invoke(new SerializableRunnable("simulateRebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(6, results.getTotalBucketCreatesCompleted());
        assertEquals(3, results.getTotalPrimaryTransfersCompleted());
        assertEquals(0, results.getTotalBucketTransferBytes());
        assertEquals(0, results.getTotalBucketTransfersCompleted());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(6, details.getBucketCreatesCompleted());
        assertEquals(3, details.getPrimaryTransfersCompleted());
        assertEquals(0, details.getBucketTransferBytes());
        assertEquals(0, details.getBucketTransfersCompleted());
        
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(2, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          assertEquals(6, memberDetails.getBucketCount());
          assertEquals(3, memberDetails.getPrimaryCount());
        }
        
        if(!simulate) {
          verifyStats(manager, results);
        }
      }
    });

    if(!simulate) {
      SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkLowRedundancy") {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region1");
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertEquals(6, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
        }
      };

      vm0.invoke(checkRedundancyFixed);
      vm1.invoke(checkRedundancyFixed);
    } else {
      //Make sure the simulation didn't do anything
      vm0.invoke(checkLowRedundancy);
    }
  }
  
  /** Manual test.*/
  public void z_testRedundancyLoop() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(2);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    vm3.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        for(int i =0; i < 500; i++) {
          region.put(Integer.valueOf(i), "A");
        }
      }
    });
    
    SerializableRunnable checkLowRedundancy = new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(113, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(113,details.getLowRedundancyBucketCount());
      }
    };
    
    //make sure we can tell that the buckets have low redundancy
    vm0.invoke(checkLowRedundancy);
    
    SerializableRunnable closePrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        disconnectFromDS();
//        Cache cache = getCache();
//        Region region = cache.getRegion("region1");
//        region.localDestroyRegion();
      }
    };

    for(int i =0; i < 50; i++) {
      long start = System.nanoTime();
      //Create the region in the other VM (should have no effect)
      vm1.invoke(createPrRegion);
      vm2.invoke(createPrRegion);

      
      //Now simulate a rebalance
      vm1.invoke(new SerializableRunnable("simulateRebalance") {

        public void run() {
          Cache cache = getCache();
          ResourceManager manager = cache.getResourceManager();
          RebalanceResults results = doRebalance(false, manager);
//          assertEquals(113, results.getTotalBucketCreatesCompleted());
        }
      });

      vm1.invoke(closePrRegion);
      vm2.invoke(closePrRegion);
      long end = System.nanoTime();
      System.err.println("Elapsed = " + TimeUnit.NANOSECONDS.toMillis(end - start));
    }
  }

  public void testEnforceIP() {
    enforceIp(false);
  }
  
  public void testEnforceIPSimulation() {
    enforceIp(true);
  }
  
  public void enforceIp(final boolean simulate) {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.ENFORCE_UNIQUE_HOST_NAME, "true");
        getSystem(props); 
      }
    });
    try {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Integer.valueOf(1), "A");
        region.put(Integer.valueOf(2), "A");
        region.put(Integer.valueOf(3), "A");
        region.put(Integer.valueOf(4), "A");
        region.put(Integer.valueOf(5), "A");
        region.put(Integer.valueOf(6), "A");
      }
    });
    
    SerializableRunnable checkLowRedundancy = new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(6, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(6,details.getLowRedundancyBucketCount());
      }
    };
    
    //make sure we can tell that the buckets have low redundancy
    vm0.invoke(checkLowRedundancy);

    //Create the region in the other VM (should have no effect)
    vm1.invoke(createPrRegion);
    
    //Make sure we still have low redundancy
    vm0.invoke(checkLowRedundancy);
    
    //Now simulate a rebalance
    vm0.invoke(new SerializableRunnable("simulateRebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        //We actually *will* transfer buckets, because that improves
        //the balance
        assertEquals(3, results.getTotalBucketTransfersCompleted());
//        assertEquals(0, results.getTotalBucketTransferBytes());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(0, details.getBucketCreatesCompleted());
        assertEquals(0, details.getPrimaryTransfersCompleted());
        assertEquals(3, details.getBucketTransfersCompleted());
//        assertEquals(0, details.getBucketTransferBytes());
        if(!simulate) {
          verifyStats(manager, results);
        }
      }
    });
    
    //Make sure we still have low redundancy
    vm0.invoke(checkLowRedundancy);
    vm1.invoke(checkLowRedundancy);

    } finally {
      disconnectFromDS();
      Invoke.invokeInEveryVM(new SerializableRunnable() {
        public void run() {
          disconnectFromDS(); 
        }
      });
    }
  }
  
  public void testEnforceZone() {
    enforceZone(false);
  }
  
  public void testEnforceZoneSimulation() {
    enforceZone(true);
  }
  
  /**
   * Test that we correctly use the redundancy-zone
   * property to determine where to place redundant copies of a buckets.
   * @param simulate
   */
  public void enforceZone(final boolean simulate) {
    
    try {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    setRedundancyZone(vm0, "A");
    setRedundancyZone(vm1, "A");
    final DistributedMember zoneBMember = setRedundancyZone(vm2, "B");

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Integer.valueOf(1), "A");
        region.put(Integer.valueOf(2), "A");
        region.put(Integer.valueOf(3), "A");
        region.put(Integer.valueOf(4), "A");
        region.put(Integer.valueOf(5), "A");
        region.put(Integer.valueOf(6), "A");
      }
    });
    
    SerializableRunnable checkLowRedundancy = new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(6, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(6,details.getLowRedundancyBucketCount());
      }
    };
    
    //make sure we can tell that the buckets have low redundancy
    vm0.invoke(checkLowRedundancy);

    //Create the region in the other VMs (should have no effect)
    vm1.invoke(createPrRegion);
    vm2.invoke(createPrRegion);
    
    //Make sure we still have low redundancy
    vm0.invoke(checkLowRedundancy);
    
    //Now simulate a rebalance
    vm0.invoke(new SerializableRunnable("simulateRebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        //We expect to satisfy redundancy with the zone B member
        assertEquals(6, results.getTotalBucketCreatesCompleted());
        //2 primaries will go to vm2, leaving vm0 and vm1 with 2 primaries each
        assertEquals(2, results.getTotalPrimaryTransfersCompleted());
        //We actually *will* transfer 3 buckets to the other member in zone A, because that improves
        //the balance
        assertEquals(3, results.getTotalBucketTransfersCompleted());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(6, details.getBucketCreatesCompleted());
        assertEquals(2, details.getPrimaryTransfersCompleted());
        assertEquals(3, details.getBucketTransfersCompleted());
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        for(PartitionMemberInfo info : afterDetails) {
          if(info.getDistributedMember().equals(zoneBMember)) {
            assertEquals(6, info.getBucketCount());
          } else {
            assertEquals(3, info.getBucketCount());
          }
          assertEquals(2, info.getPrimaryCount());
        }
//        assertEquals(0, details.getBucketTransferBytes());
        if(!simulate) {
          verifyStats(manager, results);
        }
      }
    });
    
    
    if(!simulate) {
    checkBucketCount(vm0, "region1", 3);
    checkBucketCount(vm1, "region1", 3);
    checkBucketCount(vm2, "region1", 6);
    }
    
    } finally {
      disconnectFromDS();
      Invoke.invokeInEveryVM(new SerializableRunnable() {
        public void run() {
          //clear the redundancy zone setting
          disconnectFromDS(); 
        }
      });
    }
  }

  private void createPR(String regionName){
    Cache cache = getCache();
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setRecoveryDelay(-1);
    paf.setStartupRecoveryDelay(-1);
    PartitionAttributes prAttr = paf.create();
    attr.setPartitionAttributes(prAttr);
    cache.createRegion(regionName, attr.create());
  }
  
  private void doPuts(String regionName) {
    Cache cache = getCache();
    Region region = cache.getRegion(regionName);
    region.put(Integer.valueOf(1), "A");
    region.put(Integer.valueOf(2), "A");
    region.put(Integer.valueOf(3), "A");
    region.put(Integer.valueOf(4), "A");
    region.put(Integer.valueOf(5), "A");
    region.put(Integer.valueOf(6), "A");
  }
  
  public static class ParallelRecoveryObserver extends InternalResourceManager.ResourceObserverAdapter {
    
    HashSet<String> regions = new HashSet<String>();
    private volatile boolean observerCalled;
    private CyclicBarrier barrier;
    
    public ParallelRecoveryObserver(int numRegions) {
      this.barrier = new CyclicBarrier(numRegions);
    }
    
    public void observeRegion(String region) {
      regions.add(region);
    }
    
    private void checkAllRegionRecoveryOrRebalanceStarted(String rn) {
      if(regions.contains(rn)) {
        try {
          barrier.await(MAX_WAIT, TimeUnit.SECONDS);
        } catch (Exception e) {
          Assert.fail("failed waiting for barrier", e);
        }
        observerCalled = true;
      } else {
        throw new RuntimeException("region not registered " + rn );
      }
    }
    
    public boolean isObserverCalled(){
      return observerCalled;
    }
    
    @Override
    public void rebalancingStarted(Region region) {
      
      // TODO Auto-generated method stub
      super.rebalancingStarted(region);
      checkAllRegionRecoveryOrRebalanceStarted(region.getName());
    }
    
    @Override
    public void recoveryStarted(Region region) {
      // TODO Auto-generated method stub
      super.recoveryStarted(region);
      checkAllRegionRecoveryOrRebalanceStarted(region.getName());
    }
  }
  
  public void testEnforceZoneWithMultipleRegions() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    try {
    setRedundancyZone(vm0, "A");
    setRedundancyZone(vm1, "A");
    
    final DistributedMember zoneBMember = setRedundancyZone(vm2, "B");
    
    SerializableRunnable setRebalanceObserver = new SerializableRunnable("RebalanceObserver") {
      @Override
      public void run() {
        InternalResourceManager.setResourceObserver(new ParallelRecoveryObserver(2));        
      }
    };

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        ParallelRecoveryObserver ob = (ParallelRecoveryObserver)InternalResourceManager.getResourceObserver();
        ob.observeRegion("region1");
        ob.observeRegion("region2");
        createPR("region1");        
        createPR("region2");
                
      }
    };
    
    vm0.invoke(setRebalanceObserver);
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        doPuts("region1");
        doPuts("region2");
      }
    });
    
    SerializableRunnable checkLowRedundancy = new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(6, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(6,details.getLowRedundancyBucketCount());
        
        region = cache.getRegion("region2");
        details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(6, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(6,details.getLowRedundancyBucketCount());
      }
    };
    
    //make sure we can tell that the buckets have low redundancy
    vm0.invoke(checkLowRedundancy);

    //Create the region in the other VMs (should have no effect)
    vm1.invoke(setRebalanceObserver);
    vm1.invoke(createPrRegion);
    vm2.invoke(setRebalanceObserver);
    vm2.invoke(createPrRegion);
    
    //Make sure we still have low redundancy
    vm0.invoke(checkLowRedundancy);
    
    //Now do a rebalance
    vm0.invoke(new SerializableRunnable("simulateRebalance") {

      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(false, manager);
        //We expect to satisfy redundancy with the zone B member
        assertEquals(12, results.getTotalBucketCreatesCompleted());
        //2 primaries will go to vm2, leaving vm0 and vm1 with 2 primaries each
        assertEquals(4, results.getTotalPrimaryTransfersCompleted());
        //We actually *will* transfer 3 buckets to the other member in zone A, because that improves
        //the balance
        assertEquals(6, results.getTotalBucketTransfersCompleted());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(2, detailSet.size());
        for(PartitionRebalanceInfo details : detailSet) {
          assertEquals(6, details.getBucketCreatesCompleted());
          assertEquals(2, details.getPrimaryTransfersCompleted());
          assertEquals(3, details.getBucketTransfersCompleted());
          Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
          for(PartitionMemberInfo info : afterDetails) {
            if(info.getDistributedMember().equals(zoneBMember)) {
              assertEquals(6, info.getBucketCount());
            } else {
              assertEquals(3, info.getBucketCount());
            }
            assertEquals(2, info.getPrimaryCount());
          }
        }
        //        assertEquals(0, details.getBucketTransferBytes());
        verifyStats(manager, results);
      }
    });
    
    vm0.invoke(new SerializableRunnable() {
      
      @Override
      public void run() {
        assertTrue(((ParallelRecoveryObserver)InternalResourceManager.getResourceObserver()).isObserverCalled());
      }
    });
    
    checkBucketCount(vm0, "region1", 3);
    checkBucketCount(vm1, "region1", 3);
    checkBucketCount(vm2, "region1", 6);
    
    checkBucketCount(vm0, "region2", 3);
    checkBucketCount(vm1, "region2", 3);
    checkBucketCount(vm2, "region2", 6);
    } finally {
      disconnectFromDS();
      Invoke.invokeInEveryVM(new SerializableRunnable() {
        public void run() {
          //clear the redundancy zone setting
          disconnectFromDS(); 
        }
      });
    }
  }
  
  private void checkBucketCount(VM vm0, final String regionName, final int numLocalBuckets) {
    vm0.invoke(new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        assertEquals(numLocalBuckets, region.getLocalBucketsListTestOnly().size());
      }
    });
  }

  
  private DistributedMember setRedundancyZone(VM vm, final String zone) {
    return (DistributedMember) vm.invoke(new SerializableCallable("set redundancy zone") {
      public Object call() {
        System.setProperty("gemfire.resource.manager.threads", "2");
        Properties props = new Properties();
        props.setProperty(DistributionConfig.REDUNDANCY_ZONE_NAME, zone);
        DistributedSystem system = getSystem(props);
        return system.getDistributedMember();
        
      }
    });
    
  }

  private RebalanceResults doRebalance(final boolean simulate,
      ResourceManager manager) {
    return doRebalance(simulate, manager, null, null);
  }
  /**
   * @param simulate
   * @param manager
   * @return
   */
  private RebalanceResults doRebalance(final boolean simulate,
      ResourceManager manager, Set<String> includes, Set<String> excludes) {
    RebalanceResults results = null;
    if(simulate) {
      try {
        results = manager.createRebalanceFactory()
          .includeRegions(includes)
          .excludeRegions(excludes)
          .simulate()
          .getResults(MAX_WAIT, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Assert.fail("Interrupted waiting on rebalance", e);
      } catch (TimeoutException e) {
        Assert.fail("Timeout waiting on rebalance", e);
      }
    } else {
      try {
        results = manager.createRebalanceFactory()
        .includeRegions(includes)
        .excludeRegions(excludes)
        .start()
        .getResults(MAX_WAIT, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Assert.fail("Interrupted waiting on rebalance", e);
      } catch (TimeoutException e) {
        Assert.fail("Timeout waiting on rebalance", e);
      }
    }
    assertEquals(Collections.emptySet(), manager.getRebalanceOperations());
    return results;
  }
  
  public void testRecoverRedundancyBalancingSimulation() {
    recoverRedundancyBalancing(true);
  }
  
  public void testRecoverRedundancyBalancing() {
    recoverRedundancyBalancing(false);
  }
  
  public void recoverRedundancyBalancing(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    

    final DistributedMember member1 = createPrRegion(vm0, "region1", 200, null);
    
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        for(int i = 0; i < 12; i++) {
          region.put(Integer.valueOf(i), "A");
        }
      }
    });
    
    
    SerializableRunnable checkRedundancy= new SerializableRunnable("checkRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(12, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(12,details.getLowRedundancyBucketCount());
      }
    };
    
    vm0.invoke(checkRedundancy);
    
    
    
    //Now create the region in 2 more VMs with half the localMaxMemory
    createPrRegion(vm1, "region1", 100, null);
    createPrRegion(vm2, "region1", 100, null);
    
    vm0.invoke(checkRedundancy);
    
    //Now simulate a rebalance
    vm0.invoke(new SerializableRunnable("rebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(12, results.getTotalBucketCreatesCompleted());
        assertEquals(6, results.getTotalPrimaryTransfersCompleted());
        assertEquals(0, results.getTotalBucketTransferBytes());
        assertEquals(0, results.getTotalBucketTransfersCompleted());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(12, details.getBucketCreatesCompleted());
        assertEquals(6, details.getPrimaryTransfersCompleted());
        assertEquals(0, details.getBucketTransferBytes());
        assertEquals(0, details.getBucketTransfersCompleted());
        
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(3, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          //We have 1 member with a size of 200 and two members with size 100
          if(memberDetails.getDistributedMember().equals(member1)) {
            assertEquals(12, memberDetails.getBucketCount());
            assertEquals(6, memberDetails.getPrimaryCount());
          } else {
            assertEquals(6, memberDetails.getBucketCount());
            assertEquals(3, memberDetails.getPrimaryCount());
          }
        }
        
        if(!simulate) {
          verifyStats(manager, results);
        }
      }
    });
    
    if(!simulate) {
      SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkLowRedundancy") {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region1");
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertEquals(12, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
        }
      };

      vm0.invoke(checkRedundancyFixed);
      vm1.invoke(checkRedundancyFixed);
      vm2.invoke(checkRedundancyFixed);
    }
  }

  private DistributedMember createPrRegion(VM vm, final String region, final int localMaxMemory, final String colocatedWith) {
    SerializableCallable createPrRegion = new SerializableCallable("createRegion") {
      public Object call()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        paf.setLocalMaxMemory(localMaxMemory);
        if(colocatedWith != null) {
          paf.setColocatedWith(colocatedWith);
        }
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion(region, attr.create());
        return cache.getDistributedSystem().getDistributedMember();
      }
    };
    return (DistributedMember) vm.invoke(createPrRegion);
  }
  
  public void testRecoverRedundancyColocatedRegionsSimulation() {
    recoverRedundancyColocatedRegions(true);
  }
  
  public void testRecoverRedundancyColocatedRegions() {
    recoverRedundancyColocatedRegions(false);
  }
  
  public void recoverRedundancyColocatedRegions(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final DistributedMember member1 = createPrRegion(vm0, "region1", 200, null);
    createPrRegion(vm0, "region2", 200, "region1");
    
    //Create some buckets. 
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        Region region2 = cache.getRegion("region2");
        for(int i =0; i< 12; i++) {
          region.put(Integer.valueOf(i), "A");
          region2.put(Integer.valueOf(i), "A");
        }
      }
    });
    
    
    //check to make sure our redundancy is impaired
    SerializableRunnable checkLowRedundancy = new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(12, details.getCreatedBucketCount());
        assertEquals(0, details.getActualRedundantCopies());
        assertEquals(12 ,details.getLowRedundancyBucketCount());
        region = cache.getRegion("region2");
        details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(12, details.getCreatedBucketCount());
        assertEquals(0,  details.getActualRedundantCopies());
        assertEquals(12,details.getLowRedundancyBucketCount());
      }
    };
    
    vm0.invoke(checkLowRedundancy);
    
    //Now create the region in 2 more vms, each which
    //has local max memory of 1/2 that of the original VM.
    createPrRegion(vm1, "region1", 100, null);
    createPrRegion(vm2, "region1", 100, null);
    createPrRegion(vm1, "region2", 100, "region1");
    createPrRegion(vm2, "region2", 100, "region1");
    
    vm0.invoke(checkLowRedundancy);
    
    //Now simulate a rebalance
    vm0.invoke(new SerializableRunnable("rebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(24, results.getTotalBucketCreatesCompleted());
        assertEquals(12, results.getTotalPrimaryTransfersCompleted());
        assertEquals(0, results.getTotalBucketTransferBytes());
        assertEquals(0, results.getTotalBucketTransfersCompleted());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(2, detailSet.size());
        for(PartitionRebalanceInfo details : detailSet) {
          assertEquals(12, details.getBucketCreatesCompleted());
          assertEquals(6, details.getPrimaryTransfersCompleted());
          assertEquals(0, details.getBucketTransferBytes());
          assertEquals(0, details.getBucketTransfersCompleted());
          
          Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
          assertEquals(3, afterDetails.size());
          for(PartitionMemberInfo memberDetails: afterDetails) {
            if(memberDetails.getDistributedMember().equals(member1)) {
              assertEquals(12, memberDetails.getBucketCount());
              assertEquals(6, memberDetails.getPrimaryCount());
            } else {
              assertEquals(6, memberDetails.getBucketCount());
              assertEquals(3, memberDetails.getPrimaryCount());
            }
          }
          if(!simulate) {
            verifyStats(manager, results);
          }
        }
      }
    });
    
    if(!simulate) {
      SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkLowRedundancy") {

        public void run() {
          Cache cache = getCache();
          PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
          PartitionedRegion region2 = (PartitionedRegion) cache.getRegion("region2");
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(cache.getRegion("region1"));
          assertEquals(12, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          details = PartitionRegionHelper.getPartitionRegionInfo(cache.getRegion("region2"));
          assertEquals(12, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          
          assertEquals(region1.getLocalPrimaryBucketsListTestOnly(), region2.getLocalPrimaryBucketsListTestOnly());
          
          assertEquals(region1.getLocalBucketsListTestOnly(), region2.getLocalBucketsListTestOnly());
        }
      };

      vm0.invoke(checkRedundancyFixed);
      vm1.invoke(checkRedundancyFixed);
      vm2.invoke(checkRedundancyFixed);
    }
  }
  
  public void testRecoverRedundancyParallelAsyncEventQueueSimulation() throws NoSuchFieldException, SecurityException {
    Invoke.invokeInEveryVM(new SerializableRunnable() {

      @Override
      public void run () {
        System.setProperty("gemfire.LOG_REBALANCE", "true");
      }
    });
    try {
      recoverRedundancyParallelAsyncEventQueue(true);
    } finally {
      System.setProperty("gemfire.LOG_REBALANCE", "false");
    }
  }
  
  public void testRecoverRedundancyParallelAsyncEventQueue() {
    recoverRedundancyParallelAsyncEventQueue(false);
  }
  
  public void recoverRedundancyParallelAsyncEventQueue(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final DistributedMember member1 = createPRRegionWithAsyncQueue(vm0, 200);
    
    //Create some buckets. Put enough data to cause the queue to overflow (more than 1 MB)
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        Region region2 = cache.getRegion("region2");
        for(int i =0; i< 12; i++) {
          region.put(Integer.valueOf(i), "A", new byte[1024 * 512]);
        }
        
        // GEODE-244 - the async event queue uses asnychronous writes. Flush 
        // the default disk store to make sure all values have overflowed
        cache.findDiskStore(null).flush();
      }
    });
    
    //check to make sure our redundancy is impaired
    SerializableRunnable checkLowRedundancy = new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(12, details.getCreatedBucketCount());
        assertEquals(0, details.getActualRedundantCopies());
        assertEquals(12 ,details.getLowRedundancyBucketCount());
        //Get the async event queue region (It's a colocated region)
        PartitionedRegion region2 = ColocationHelper.getColocatedChildRegions((PartitionedRegion)region).get(0);
        details = PartitionRegionHelper.getPartitionRegionInfo(region2);
        assertEquals(12, details.getCreatedBucketCount());
        assertEquals(0,  details.getActualRedundantCopies());
        assertEquals(12,details.getLowRedundancyBucketCount());
        AsyncEventQueue queue = cache.getAsyncEventQueue("parallelQueue");
        assertEquals(12, queue.size());
      }
    };
    
    vm0.invoke(checkLowRedundancy);
    
    
    //Create the region on two more members, each with 1/2 of the memory
    createPRRegionWithAsyncQueue(vm1, 100);
    createPRRegionWithAsyncQueue(vm2, 100);
    
    vm0.invoke(checkLowRedundancy);
    
    //Now simulate a rebalance
    vm0.invoke(new SerializableRunnable("rebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(24, results.getTotalBucketCreatesCompleted());
        assertEquals(12, results.getTotalPrimaryTransfersCompleted());
        assertEquals(0, results.getTotalBucketTransferBytes());
        assertEquals(0, results.getTotalBucketTransfersCompleted());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(2, detailSet.size());
        for(PartitionRebalanceInfo details : detailSet) {
          assertEquals(12, details.getBucketCreatesCompleted());
          assertEquals(6, details.getPrimaryTransfersCompleted());
          assertEquals(0, details.getBucketTransferBytes());
          assertEquals(0, details.getBucketTransfersCompleted());
          
          Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
          assertEquals(3, afterDetails.size());
          for(PartitionMemberInfo memberDetails: afterDetails) {
            if(memberDetails.getDistributedMember().equals(member1)) {
              assertEquals(12, memberDetails.getBucketCount());
              assertEquals(6, memberDetails.getPrimaryCount());
            } else {
              assertEquals(6, memberDetails.getBucketCount());
              assertEquals(3, memberDetails.getPrimaryCount());
            }
          }
          if(!simulate) {
            verifyStats(manager, results);
          }
        }
      }
    });
    
    if(!simulate) {
      SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkLowRedundancy") {

        public void run() {
          Cache cache = getCache();
          PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
          //Get the async event queue region (It's a colocated region)
          PartitionedRegion region2 = ColocationHelper.getColocatedChildRegions(region1).get(0);
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(cache.getRegion("region1"));
          assertEquals(12, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          details = PartitionRegionHelper.getPartitionRegionInfo(region2);
          assertEquals(12, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          
          assertEquals(region1.getLocalPrimaryBucketsListTestOnly(), region2.getLocalPrimaryBucketsListTestOnly());
          
          assertEquals(region1.getLocalBucketsListTestOnly(), region2.getLocalBucketsListTestOnly());
        }
      };

      vm0.invoke(checkRedundancyFixed);
      vm1.invoke(checkRedundancyFixed);
      vm2.invoke(checkRedundancyFixed);
    }
  }

  private DistributedMember createPRRegionWithAsyncQueue(VM vm0, final int localMaxMemory) {
    SerializableCallable createPrRegion = new SerializableCallable("createRegion") {
      public Object call()
      {
        Cache cache = getCache();
        
        //Create an async event listener that doesn't dispatch anything
        cache.createAsyncEventQueueFactory().setMaximumQueueMemory(1).setParallel(true).create("parallelQueue", new AsyncEventListener() {
          @Override
          public void close() {
          }

          @Override
          public boolean processEvents(List<AsyncEvent> events) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              return false;
            }
            return false;
          }
          
        });
        AttributesFactory attr = new AttributesFactory();
        attr.addAsyncEventQueueId("parallelQueue");
        
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        paf.setLocalMaxMemory(localMaxMemory);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
        
        return cache.getDistributedSystem().getDistributedMember();
      }
    };
    
    final DistributedMember member1 = (DistributedMember) vm0.invoke(createPrRegion);
    return member1;
  }
  
  public void testCancelOperation() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Integer.valueOf(1), "A");
      }
    });
    
    SerializableRunnable checkLowRedundancy = new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(1, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(1,details.getLowRedundancyBucketCount());
      }
    };
    
    //make sure we can tell that the buckets have low redundancy
    vm0.invoke(checkLowRedundancy);

    //Create the region in the other VM (should have no effect)
    vm1.invoke(createPrRegion);
    
    //Make sure we still have low redundancy
    vm0.invoke(checkLowRedundancy);
    
    //Now do a rebalance, but cancel it in the middle
    vm0.invoke(new SerializableCallable("D rebalance")  {
      
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        InternalResourceManager manager = cache.getResourceManager();
        final CountDownLatch rebalancingCancelled = new CountDownLatch(1);
        final CountDownLatch rebalancingFinished = new CountDownLatch(1);
        InternalResourceManager.setResourceObserver(new ResourceObserverAdapter() {

          @Override
          public void rebalancingOrRecoveryStarted(Region region) {
            try {
              rebalancingCancelled.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          
          @Override
          public void rebalancingOrRecoveryFinished(Region region) {
            rebalancingFinished.countDown();
          }
        });
        RebalanceOperation op = manager.createRebalanceFactory().start();
        assertFalse(op.isCancelled());
        assertFalse(op.isDone());
        assertEquals(Collections.singleton(op), manager.getRebalanceOperations());
        try {
          op.getResults(5, TimeUnit.SECONDS);
          fail("Should have received a timeout exception");
        } catch(TimeoutException expected) {
        }
        assertTrue(op.cancel());
        rebalancingCancelled.countDown();
        assertTrue(op.isCancelled());
        assertTrue(op.isDone());
        
        rebalancingFinished.await();
        try {
          op.getResults(60, TimeUnit.SECONDS);
          fail("Should have received a cancellation exception");
        } catch(CancellationException expected) {
        }
        assertEquals(Collections.emptySet(), manager.getRebalanceOperations());
        
        return null;
      }
    });
    
    //We should still have low redundancy, because the rebalancing was cancelled
    vm0.invoke(checkLowRedundancy);

  }
  
  /**
   * Test that the rebalancing operation picks up on 
   * a concurrent membership change
   */
  public void testMembershipChange() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);

    final SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Integer.valueOf(1), "A");
        region.put(Integer.valueOf(2), "A");
        region.put(Integer.valueOf(3), "A");
        region.put(Integer.valueOf(4), "A");
        region.put(Integer.valueOf(5), "A");
        region.put(Integer.valueOf(6), "A");
      }
    });
    

    //Create the region in the other VM (should have no effect)
    vm1.invoke(createPrRegion);
    
    //Now do a rebalance, but start another member in the middle
    vm0.invoke(new SerializableCallable("D rebalance")  {
      
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        InternalResourceManager manager = cache.getResourceManager();
        final CountDownLatch rebalancingStarted = new CountDownLatch(1);
        final CountDownLatch memberAdded = new CountDownLatch(1);
        InternalResourceManager.setResourceObserver(new ResourceObserverAdapter() {
          boolean firstBucket = true;
          @Override
          public void movingBucket(Region region,
              int bucketId, 
              DistributedMember source, 
              DistributedMember target) {
            if(firstBucket) {
              firstBucket = false;
              vm2.invoke(createPrRegion);
            }
          }
        });
        RebalanceResults results = doRebalance(false, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        assertEquals(4, results.getTotalBucketTransfersCompleted());
        assertTrue(0 < results.getTotalBucketTransferBytes());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(0, details.getBucketCreatesCompleted());
        assertEquals(0, details.getPrimaryTransfersCompleted());
        assertTrue(0 < details.getBucketTransferBytes());
        assertEquals(4, details.getBucketTransfersCompleted());
        
        Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsBefore();
        //there should have only been 2 members when the rebalancing started.
        assertEquals(2, beforeDetails.size());
        
        //if it was done, there should now be 3 members.
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(3, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          assertEquals(2, memberDetails.getBucketCount());
          assertEquals(2, memberDetails.getPrimaryCount());
        }
        verifyStats(manager, results);
        InternalResourceManager mgr  = (InternalResourceManager) manager;
        ResourceManagerStats stats = mgr.getStats();
        assertEquals(1, stats.getRebalanceMembershipChanges());
        return null;
      }
    });
  }
  
  
  public void testMoveBucketsNoRedundancySimulation() {
    moveBucketsNoRedundancy(true);
  }
  
  public void testMoveBucketsNoRedundancy() {
    moveBucketsNoRedundancy(false);
  }
  
  /**
   * Check to make sure that we balance
   * buckets between two hosts with no redundancy.
   * @param simulate
   */
  public void moveBucketsNoRedundancy(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setCacheLoader(new Bug40228Loader());
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Integer.valueOf(1), "A");
        region.put(Integer.valueOf(2), "A");
        region.put(Integer.valueOf(3), "A");
        region.put(Integer.valueOf(4), "A");
        region.put(Integer.valueOf(5), "A");
        region.put(Integer.valueOf(6), "A");
      }
    });
    
    //Create the region in the other VM (should have no effect)
    vm1.invoke(createPrRegion);
    
    //Now simulate a rebalance
    vm0.invoke(new SerializableRunnable("simulateRebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        assertEquals(3, results.getTotalBucketTransfersCompleted());
        assertTrue(0 < results.getTotalBucketTransferBytes());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(0, details.getBucketCreatesCompleted());
        assertEquals(0, details.getPrimaryTransfersCompleted());
        assertTrue(0 < details.getBucketTransferBytes());
        assertEquals(3, details.getBucketTransfersCompleted());
        
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(2, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          assertEquals(3, memberDetails.getBucketCount());
          assertEquals(3, memberDetails.getPrimaryCount());
        }
        if(!simulate) {
          verifyStats(manager, results);
        }
      }
    });

    if(!simulate) {
      SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkRedundancyFixed") {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region1");
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertEquals(6, details.getCreatedBucketCount());
          assertEquals(0,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          assertEquals(2, details.getPartitionMemberInfo().size());
          for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
            assertEquals(3, memberDetails.getBucketCount());
            assertEquals(3, memberDetails.getPrimaryCount());
          }
          
          //check to make sure that moving buckets didn't close the cache loader
          Bug40228Loader loader = (Bug40228Loader) cache.getRegion("region1").getAttributes().getCacheLoader();
          assertFalse(loader.isClosed());
        }
      };

      vm0.invoke(checkRedundancyFixed);
      vm1.invoke(checkRedundancyFixed);
      
      SerializableRunnable checkBug40228Fixed = new SerializableRunnable("checkBug40228Fixed") {

        public void run() {
          Cache cache = getCache();
          Bug40228Loader loader = (Bug40228Loader) cache.getRegion("region1").getAttributes().getCacheLoader();
          assertFalse(loader.isClosed());
          //check to make sure that closing the PR closes the cache loader
          cache.getRegion("region1").close();
          assertTrue(loader.isClosed());
        }
      };
      
      vm0.invoke(checkBug40228Fixed);
      vm1.invoke(checkBug40228Fixed);
    }
  }
  
  public void testFilterRegionsSimulation() {
    filterRegions(true);
  }
  
  public void testFilterRegions() {
    filterRegions(false);
  }
  
  /**
   * Check to make sure that we balance
   * buckets between two hosts with no redundancy.
   * @param simulate
   */
  public void filterRegions(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    final int NUM_REGIONS = 4;
    final Set<String> INCLUDED = new HashSet<String>();
    INCLUDED.add("region0");
    INCLUDED.add("region1");
    final Set<String> EXCLUDED = new HashSet<String>();
    EXCLUDED.add("region0");
    EXCLUDED.add("region3");
    
    final HashSet<String> EXPECTED_REBALANCED = new HashSet<String>();
    EXPECTED_REBALANCED.add("/region0");
    EXPECTED_REBALANCED.add("/region1");

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        for(int i = 0; i < NUM_REGIONS; i++) {
          cache.createRegion("region" + i, attr.create());
        }
      }
    };
    
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        for(int i = 0; i < NUM_REGIONS; i++) {
          Region region = cache.getRegion("region" + i);
          for(int j = 0; j < 6; j++) {
            region.put(Integer.valueOf(j), "A");
          }
        }
      }
    });
    
    //Create the region in the other VM (should have no effect)
    vm1.invoke(createPrRegion);
    
    //Now simulate a rebalance
    vm0.invoke(new SerializableRunnable("simulateRebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager, INCLUDED, EXCLUDED);
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
//        assertEquals(3, detailSet.size());
        Set<String> names = new HashSet<String>();
        for(PartitionRebalanceInfo details: detailSet) {
          assertEquals(0, details.getBucketCreatesCompleted());
          assertEquals(0, details.getPrimaryTransfersCompleted());
          assertTrue(0 < details.getBucketTransferBytes());
          assertEquals(3, details.getBucketTransfersCompleted());
          names.add(details.getRegionPath());
          Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
          assertEquals(2, afterDetails.size());
          for(PartitionMemberInfo memberDetails: afterDetails) {
            assertEquals(3, memberDetails.getBucketCount());
            assertEquals(3, memberDetails.getPrimaryCount());
          }
        }
        
        assertEquals(EXPECTED_REBALANCED, names);

        assertEquals(0, results.getTotalBucketCreatesCompleted());
        assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        assertEquals(6, results.getTotalBucketTransfersCompleted());
        assertTrue(0 < results.getTotalBucketTransferBytes());
        
        if(!simulate) {
          verifyStats(manager, results);
        }
      }
    });

    if(!simulate) {
      SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkRedundancyFixed") {

        public void run() {
          Cache cache = getCache();
          for(String name: EXPECTED_REBALANCED) {
            Region region = cache.getRegion(name);
            PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
            assertEquals(6, details.getCreatedBucketCount());
            assertEquals(0,details.getActualRedundantCopies());
            assertEquals(0,details.getLowRedundancyBucketCount());
            assertEquals(2, details.getPartitionMemberInfo().size());
            for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
              assertEquals(3, memberDetails.getBucketCount());
              assertEquals(3, memberDetails.getPrimaryCount());
            }
          }
          Region region = cache.getRegion("region2");
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertEquals(6, details.getCreatedBucketCount());
          assertEquals(0,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          assertEquals(2, details.getPartitionMemberInfo().size());
          for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
            int bucketCount = memberDetails.getBucketCount();
            int primaryCount = memberDetails.getPrimaryCount();
            assertTrue(
                "Wrong number of buckets on non rebalanced region buckets="
                    + bucketCount + " primarys=" + primaryCount,
                bucketCount == 6 && primaryCount == 6 
                || bucketCount == 0 && primaryCount == 0);
          }
        }
      };

      vm0.invoke(checkRedundancyFixed);
      vm1.invoke(checkRedundancyFixed);
    }
  }
  
  public void testMoveBucketsWithRedundancySimulation() {
    moveBucketsWithRedundancy(true);
  }

  public void testMoveBucketsWithRedundancy() {
    moveBucketsWithRedundancy(false);
  }
  
  /**
   * Test to make sure we balance buckets between three hosts with redundancy
   */
  public void moveBucketsWithRedundancy(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in two VMs
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        for(int i =0; i < 12; i++) {
          region.put(Integer.valueOf(i), "A");
        }
      }
    });
    
    //Create the region in one more VM.
    vm2.invoke(createPrRegion);
    
    //Now simulate a rebalance
    final Long totalSize = (Long) vm0.invoke(new SerializableCallable("simulateRebalance") {
      
      public Object call() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        //We don't know how many primaries will move, it depends on
        //if the move bucket code moves the primary or a redundant bucket
        //assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        assertEquals(8, results.getTotalBucketTransfersCompleted());
        assertTrue(0 < results.getTotalBucketTransferBytes());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(0, details.getBucketCreatesCompleted());
        assertTrue(0 < details.getBucketTransferBytes());
        assertEquals(8, details.getBucketTransfersCompleted());
        
        long totalSize = 0;
        Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsAfter();
        for(PartitionMemberInfo memberDetails: beforeDetails) {
          totalSize += memberDetails.getSize();
        }
        
        long afterSize = 0;
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(3, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          assertEquals(8, memberDetails.getBucketCount());
          assertEquals(4, memberDetails.getPrimaryCount());
          afterSize += memberDetails.getSize();
        }
        assertEquals(totalSize, afterSize);
        if(!simulate) {
          verifyStats(manager, results);
        }
        
        return Long.valueOf(totalSize);
      }
    });

    if(!simulate) {
      SerializableRunnable checkBalance = new SerializableRunnable("checkBalance") {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region1");
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertEquals(12, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          LogWriterUtils.getLogWriter().info("details=" + details.getPartitionMemberInfo());
          long afterSize = 0;
          for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
            assertEquals(8, memberDetails.getBucketCount());
            assertEquals(4, memberDetails.getPrimaryCount());
            afterSize += memberDetails.getSize();
          }
          assertEquals(totalSize.longValue(), afterSize);
        }
      };

      vm0.invoke(checkBalance);
      vm1.invoke(checkBalance);
      vm2.invoke(checkBalance);
    }
  }
  
  /** A test that the stats when overflowing entries to disk
   * are correct and we still rebalance correctly
   */
  public void testMoveBucketsOverflowToDisk() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in two VMs
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        for(int i =0; i < 12; i++) {
          Map m = new HashMap();
          for (int j = 0; j < 200; j++) {
            m.put(Integer.valueOf(i + 113*j), "A");
          }
          region.putAll(m);
        }
      }
    });

    //Do some puts and gets, to trigger eviction
    SerializableRunnable doOps = new SerializableRunnable("doOps") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        
        Random rand = new Random();
        
        for(int count = 0; count < 5000; count++) {
          int bucket = (int) (count % 12);
          int key = rand.nextInt(20);
          region.put(Integer.valueOf(bucket + 113*key), "B");
        }
        
        for(int count = 0; count < 500; count++) {
          int bucket = (int) (count % 12);
          int key = rand.nextInt(20);
          region.get(Integer.valueOf(bucket + 113*key));
        }
      }
    };
    
    //Do some operations
    vm0.invoke(doOps);
    
    //Create the region in one more VM.
    vm2.invoke(createPrRegion);
    
    //Now do a rebalance
    final Long totalSize = (Long) vm0.invoke(new SerializableCallable("simulateRebalance") {
      
      public Object call() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(false, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        //We don't know how many primaries will move, it depends on
        //if the move bucket code moves the primary or a redundant bucket
        //assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        assertEquals(8, results.getTotalBucketTransfersCompleted());
        assertTrue(0 < results.getTotalBucketTransferBytes());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(0, details.getBucketCreatesCompleted());
        assertTrue(0 < details.getBucketTransferBytes());
        assertEquals(8, details.getBucketTransfersCompleted());
        
        long totalSize = 0;
        Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsAfter();
        for(PartitionMemberInfo memberDetails: beforeDetails) {
          totalSize += memberDetails.getSize();
        }
        
        long afterSize = 0;
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(3, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          assertEquals(8, memberDetails.getBucketCount());
          assertEquals(4, memberDetails.getPrimaryCount());
          afterSize += memberDetails.getSize();
        }
        assertEquals(totalSize, afterSize);
        verifyStats(manager, results);
        
        return Long.valueOf(totalSize);
      }
    });

    SerializableRunnable checkBalance = new SerializableRunnable("checkBalance") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(12, details.getCreatedBucketCount());
        assertEquals(1,details.getActualRedundantCopies());
        assertEquals(0,details.getLowRedundancyBucketCount());
        LogWriterUtils.getLogWriter().info("details=" + details.getPartitionMemberInfo());
        long afterSize = 0;
        for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
          assertEquals(8, memberDetails.getBucketCount());
          assertEquals(4, memberDetails.getPrimaryCount());
          afterSize += memberDetails.getSize();
        }
        //assertEquals(totalSize.longValue(), afterSize);
      }
    };

    vm0.invoke(checkBalance);
    vm1.invoke(checkBalance);
    vm2.invoke(checkBalance);
    
    //Create the region in one more VM.
    vm3.invoke(createPrRegion);
    
    //Do another rebalance
    vm0.invoke(new SerializableCallable("simulateRebalance") {
      
      public Object call() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(false, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        //We don't know how many primaries will move, it depends on
        //if the move bucket code moves the primary or a redundant bucket
        //assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        assertEquals(6, results.getTotalBucketTransfersCompleted());
        assertTrue(0 < results.getTotalBucketTransferBytes());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(0, details.getBucketCreatesCompleted());
        assertTrue(0 < details.getBucketTransferBytes());
        assertEquals(6, details.getBucketTransfersCompleted());
        
        long totalSize = 0;
        Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsAfter();
        for(PartitionMemberInfo memberDetails: beforeDetails) {
          totalSize += memberDetails.getSize();
        }
        
        long afterSize = 0;
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(4, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          assertEquals(6, memberDetails.getBucketCount());
//          assertEquals(3, memberDetails.getPrimaryCount());
          afterSize += memberDetails.getSize();
        }
        assertEquals(totalSize, afterSize);
        //TODO - need to fix verifyStats to handle a second rebalance
//        verifyStats(manager, results);
        
        return Long.valueOf(totalSize);
      }
    });

      checkBalance = new SerializableRunnable("checkBalance") {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region1");
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertEquals(12, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          LogWriterUtils.getLogWriter().info("details=" + details.getPartitionMemberInfo());
          long afterSize = 0;
          for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
            assertEquals(6, memberDetails.getBucketCount());
            //            assertEquals(3, memberDetails.getPrimaryCount());
            afterSize += memberDetails.getSize();
          }
          //assertEquals(totalSize.longValue(), afterSize);
        }
      };

      vm0.invoke(checkBalance);
      vm1.invoke(checkBalance);
      vm2.invoke(checkBalance);
  }
  
  /**
   * Test to make sure we balance buckets between three hosts with redundancy
   */
  public void testMoveBucketsNestedPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Region parent;
        Cache cache = getCache();
        {
          AttributesFactory attr = new AttributesFactory();
          attr.setDataPolicy(DataPolicy.REPLICATE);
          parent = cache.createRegion("parent", attr.create());
        }
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        parent.createSubregion("region1", attr.create());
      }
    };
    
    //Create the region in two VMs
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("parent/region1");
        for(int i =0; i < 12; i++) {
          region.put(Integer.valueOf(i), "A");
        }
      }
    });
    
    //Create the region in one more VM.
    vm2.invoke(createPrRegion);
    
    //Now simulate a rebalance
    final Long totalSize = (Long) vm0.invoke(new SerializableCallable("simulateRebalance") {
      
      public Object call() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(false, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        //We don't know how many primaries will move, it depends on
        //if the move bucket code moves the primary or a redundant bucket
        //assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        assertEquals(8, results.getTotalBucketTransfersCompleted());
        assertTrue(0 < results.getTotalBucketTransferBytes());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(0, details.getBucketCreatesCompleted());
        assertTrue(0 < details.getBucketTransferBytes());
        assertEquals(8, details.getBucketTransfersCompleted());
        
        long totalSize = 0;
        Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsAfter();
        for(PartitionMemberInfo memberDetails: beforeDetails) {
          totalSize += memberDetails.getSize();
        }
        
        long afterSize = 0;
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(3, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          assertEquals(8, memberDetails.getBucketCount());
          assertEquals(4, memberDetails.getPrimaryCount());
          afterSize += memberDetails.getSize();
        }
        assertEquals(totalSize, afterSize);
        verifyStats(manager, results);
        
        return Long.valueOf(totalSize);
      }
    });

    SerializableRunnable checkBalance = new SerializableRunnable("checkBalance") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("parent/region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(12, details.getCreatedBucketCount());
        assertEquals(1,details.getActualRedundantCopies());
        assertEquals(0,details.getLowRedundancyBucketCount());
        LogWriterUtils.getLogWriter().info("details=" + details.getPartitionMemberInfo());
        long afterSize = 0;
        for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
          assertEquals(8, memberDetails.getBucketCount());
          assertEquals(4, memberDetails.getPrimaryCount());
          afterSize += memberDetails.getSize();
        }
        assertEquals(totalSize.longValue(), afterSize);
      }
    };

    vm0.invoke(checkBalance);
    vm1.invoke(checkBalance);
    vm2.invoke(checkBalance);
  }
  
  public void testMoveBucketsColocatedRegionsSimulation() {
    moveBucketsColocatedRegions(true);
  }
  
  public void testMoveBucketsColocatedRegions() {
    moveBucketsColocatedRegions(false);
  }
  
  /**
   * Test to make sure that we move buckets to balance between
   * three hosts with a redundancy of 1 and two colocated regions.
   * Makes sure that the buckets stay colocated when we move them.
   * @param simulate
   */
  public void moveBucketsColocatedRegions(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    createPrRegion(vm0, "region1", 200, null);
    createPrRegion(vm0, "region2", 200, "region1");
    createPrRegion(vm1, "region1", 200, null);
    createPrRegion(vm1, "region2", 200, "region1");
    
    //Create some buckets. 
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        Region region2 = cache.getRegion("region2");
        for(int i =0; i< 12; i++) {
          region.put(Integer.valueOf(i), "A");
          region2.put(Integer.valueOf(i), "A");
        }
      }
    });
    
    //create the leader region, but not the colocated
    //region in one of the VMs.
    createPrRegion(vm2, "region1", 200, null);

    
    //Simulate a rebalance, and make sure we don't
    //move any buckets yet, because we don't have
    //the colocated region in the new VMs.
    vm0.invoke(new SerializableRunnable("rebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        assertEquals(0, results.getTotalBucketTransferBytes());
        assertEquals(0, results.getTotalBucketTransfersCompleted());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(2, detailSet.size());
        for(PartitionRebalanceInfo details : detailSet) {
          assertEquals(0, details.getBucketCreatesCompleted());
          assertEquals(0, details.getPrimaryTransfersCompleted());
          assertEquals(0, details.getBucketTransferBytes());
          assertEquals(0, details.getBucketTransfersCompleted());
        }
        
      }
    });
    
    
    //now create the colocated region in the last VM.
    createPrRegion(vm2, "region2", 200, "region1");
    
    vm0.invoke(new SerializableRunnable("rebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        assertEquals(16, results.getTotalBucketTransfersCompleted());
        assertTrue(0 < results.getTotalBucketTransferBytes());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(2, detailSet.size());
        for(PartitionRebalanceInfo details : detailSet) {
          assertEquals(0, details.getBucketCreatesCompleted());
          assertTrue(0 < details.getBucketTransferBytes());
          assertEquals(8, details.getBucketTransfersCompleted());
          
          Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
          assertEquals(3, afterDetails.size());
          for(PartitionMemberInfo memberDetails: afterDetails) {
            assertEquals(8, memberDetails.getBucketCount());
            assertEquals(4, memberDetails.getPrimaryCount());
          }
        }
      }
    });

    
    if(!simulate) {
      SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkLowRedundancy") {

        public void run() {
          Cache cache = getCache();
          PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
          PartitionedRegion region2 = (PartitionedRegion) cache.getRegion("region2");
          ResourceManager manager = cache.getResourceManager();
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(cache.getRegion("region1"));
          assertEquals(12, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          details = PartitionRegionHelper.getPartitionRegionInfo(cache.getRegion("region2"));
          assertEquals(12, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          
          assertEquals(region1.getLocalPrimaryBucketsListTestOnly(), region2.getLocalPrimaryBucketsListTestOnly());
          
          assertEquals(region1.getLocalBucketsListTestOnly(), region2.getLocalBucketsListTestOnly());
        }
      };

      vm0.invoke(checkRedundancyFixed);
      vm1.invoke(checkRedundancyFixed);
      vm2.invoke(checkRedundancyFixed);
    }
  }
  
  /**
   * Test to make sure that moving primaries waits for a put
   * @throws Exception
   */
  public void testWaitForPut() throws Exception {
    runTestWaitForOperation(new Operation() {
      public void execute(Region region, Integer key) {
        region.put(key, "B");
      }
    });
  }
  
  /**
   * Test to make sure that moving primaries waits for a put
   * @throws Exception
   */
  public void testWaitForInvalidate() throws Exception {
    runTestWaitForOperation(new Operation() {
      public void execute(Region region, Integer key) {
        region.invalidate(key);
      }
    });
  }
  
  /**
   * Test to make sure that moving primaries waits for a put
   * @throws Exception
   */
  public void testWaitForDestroy() throws Exception {
    runTestWaitForOperation(new Operation() {
      public void execute(Region region, Integer key) {
        region.destroy(key);
      }
    });
  }
  
  /**
   * Test to make sure that moving primaries waits for a put
   * @throws Exception
   */
  public void testWaitForCacheLoader() throws Exception {
    runTestWaitForOperation(new Operation() {
      public void execute(Region region, Integer key) {
        PartitionedRegion r = (PartitionedRegion) region;
        //get a key that doesn't exist, but is in the same bucket as the given key
        region.get(key + r.getPartitionAttributes().getTotalNumBuckets());
      }
    });
  }
  
  /** 
   * Test to ensure that we wait for
   * in progress write operations before moving a primary.
   * @throws InterruptedException 
   * @throws CancellationException 
   * @throws TimeoutException 
   */
  public void runTestWaitForOperation(final Operation op) throws CancellationException, InterruptedException, TimeoutException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setCacheLoader(new CacheLoader() {
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            return "anobject";
          }

          public void close() {
          }
        });
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        paf.setLocalMaxMemory(100);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create a region in this VM with a cache writer
    //and cache loader
    Cache cache = getCache();
    AttributesFactory attr = new AttributesFactory();
    attr.setCacheLoader(new CacheLoader() {
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        return "anobject";
      }

      public void close() {
      }
    });
    
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setRecoveryDelay(-1);
    paf.setStartupRecoveryDelay(-1);
    paf.setLocalMaxMemory(100);
    PartitionAttributes prAttr = paf.create();
    attr.setPartitionAttributes(prAttr);
    final Region region = cache.createRegion("region1", attr.create());
    
    //create some buckets
    region.put(Integer.valueOf(1), "A");
    region.put(Integer.valueOf(2), "A");

    BlockingCacheListener cacheWriter = new BlockingCacheListener(2);
    region.getAttributesMutator().addCacheListener(cacheWriter);
    
    //start two threads doing operations, one on each bucket
    //the threads will block on the cache writer. The rebalance operation
    //will try to move one of these buckets, but it shouldn't
    //be able to because of the in progress operation.
    Thread thread1 = new Thread() {
      public void run() {
        op.execute(region, Integer.valueOf(1));
      }
    };
    thread1.start();
    
    Thread thread2 = new Thread() {
      public void run() {
        op.execute(region, Integer.valueOf(2));
      }
    };
    thread2.start();
    cacheWriter.waitForOperationsStarted();
    
    SerializableRunnable checkLowRedundancy = new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(2, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(2,details.getLowRedundancyBucketCount());
      }
    };
    
    //make sure we can tell that the buckets have low redundancy
    checkLowRedundancy.run();

    //Create the region in the other VM (should have no effect)
    vm1.invoke(createPrRegion);
    
    //Make sure we still have low redundancy
    checkLowRedundancy.run();
    
    ResourceManager manager = cache.getResourceManager();
    RebalanceOperation rebalance = manager.createRebalanceFactory().start();
    try {
      rebalance.getResults(5, TimeUnit.SECONDS);
      fail("Operation should not have completed");
    } catch(TimeoutException expected) {
      //do nothing
    }
    cacheWriter.release();
   
    LogWriterUtils.getLogWriter().info("starting wait for rebalance.  Will wait for " + MAX_WAIT + " seconds");
    RebalanceResults results = rebalance.getResults(MAX_WAIT, TimeUnit.SECONDS);
    assertEquals(2, results.getTotalBucketCreatesCompleted());
    assertEquals(1, results.getTotalPrimaryTransfersCompleted());
    assertEquals(0, results.getTotalBucketTransferBytes());
    assertEquals(0, results.getTotalBucketTransfersCompleted());
    Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
    assertEquals(1, detailSet.size());
    PartitionRebalanceInfo details = detailSet.iterator().next();
    assertEquals(2, details.getBucketCreatesCompleted());
    assertEquals(1, details.getPrimaryTransfersCompleted());
    assertEquals(0, details.getBucketTransferBytes());
    assertEquals(0, details.getBucketTransfersCompleted());
        
    Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
    assertEquals(2, afterDetails.size());
    for(PartitionMemberInfo memberDetails: afterDetails) {
      assertEquals(2, memberDetails.getBucketCount());
      assertEquals(1, memberDetails.getPrimaryCount());

      SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkRedundancyFixed") {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region1");
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertEquals(2, details.getCreatedBucketCount());
          assertEquals(1,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
        }
      };

      checkRedundancyFixed.run();
      vm1.invoke(checkRedundancyFixed);
    }
  }
  
  public void testRecoverRedundancyWithOfflinePersistenceSimulation() throws Throwable {
    recoverRedundancyWithOfflinePersistence(true, false);
  }
   
  public void testRecoverRedundancyWithOfflinePersistence() throws Throwable {
    recoverRedundancyWithOfflinePersistence(false, false);
  }
  
  public void testRecoverRedundancyWithOfflinePersistenceAccessorSimulation() throws Throwable {
    recoverRedundancyWithOfflinePersistence(true, true);
  }
   
  public void testRecoverRedundancyWithOfflinePersistenceAccessor() throws Throwable {
    recoverRedundancyWithOfflinePersistence(false, true);
  }
  
  public void recoverRedundancyWithOfflinePersistence(final boolean simulate, final boolean useAccessor) throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        DiskStore ds1 = dsf.setDiskDirs(getDiskDirs())
                          .create(getUniqueName());
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        attr.setDiskSynchronous(true);
        attr.setDiskStoreName(getUniqueName());
        cache.createRegion("region1", attr.create());
      }
    };
    
    //Create the region in only 2 VMs
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    
    VM rebalanceVM;
    SerializableRunnable createAccessor = new SerializableRunnable(("createAccessor")) {
      
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        DiskStore ds1 = dsf.setDiskDirs(getDiskDirs())
                          .create("ds1");
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        paf.setLocalMaxMemory(0);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());  
      }
    };
    
    if(useAccessor) {
      //Create an accessor and reblance from that VM
      vm3.invoke(createAccessor);
      rebalanceVM = vm3;
    } else {
      rebalanceVM = vm0;
    }
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Integer.valueOf(1), "A");
        region.put(Integer.valueOf(2), "A");
        region.put(Integer.valueOf(3), "A");
        region.put(Integer.valueOf(4), "A");
        region.put(Integer.valueOf(5), "A");
        region.put(Integer.valueOf(6), "A");
      }
    });
    
    SerializableRunnable closeCache = new SerializableRunnable("close cache") {
      public void run() {
        Cache cache = getCache();
        cache.getRegion("region1").close();
      }
    };
   
    //Close the cache in vm1
    final Set<Integer> vm1Buckets = getBucketList("region1", vm1);
    vm1.invoke(closeCache);
    
    SerializableRunnable checkLowRedundancyBeforeRebalance = new SerializableRunnable("checkLowRedundancyBeforeRebalance") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(6, details.getCreatedBucketCount());
        assertEquals(0,details.getActualRedundantCopies());
        assertEquals(6,details.getLowRedundancyBucketCount());
      }
    };

    SerializableRunnable checkLowRedundancyAfterRebalance = new SerializableRunnable("checkLowRedundancyAfterRebalance") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(6, details.getCreatedBucketCount());
        assertEquals(1,details.getActualRedundantCopies());
        assertEquals(0,details.getLowRedundancyBucketCount());
      }
    };
    
    //make sure we can tell that the buckets have low redundancy
    vm0.invoke(checkLowRedundancyBeforeRebalance);
    
    //Now create the cache in another member
    vm2.invoke(createPrRegion);
    
    //Make sure we still have low redundancy
    vm0.invoke(checkLowRedundancyBeforeRebalance);
    
    /*
     * Simulates a rebalance if simulation flag is set.
     * Otherwise, performs a rebalance.
     * 
     * A rebalance will replace offline buckets, so this
     * should restore redundancy
     */
    rebalanceVM.invoke(new SerializableRunnable("simulateRebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(6, results.getTotalBucketCreatesCompleted());
        assertEquals(3, results.getTotalPrimaryTransfersCompleted());
        assertEquals(0, results.getTotalBucketTransfersCompleted());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(6, details.getBucketCreatesCompleted());
        assertEquals(3, details.getPrimaryTransfersCompleted());
        assertEquals(0, details.getBucketTransfersCompleted());
        
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(2, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          assertEquals(6, memberDetails.getBucketCount());
          assertEquals(3, memberDetails.getPrimaryCount());
        }
        
        if(!simulate) {
          verifyStats(manager, results);
        }
      }
    });
    
    Set<Integer> vm0Buckets = getBucketList("region1", vm0);
    Set<Integer> vm2Buckets = getBucketList("region1", vm2);

    //Make sure redundancy is repaired if not simulated
    if(!simulate) {
      vm0.invoke(checkLowRedundancyAfterRebalance);
    } else {
      // Othewise, we should still have broken redundancy at this point
      vm0.invoke(checkLowRedundancyBeforeRebalance);
    }
    
    vm2.invoke(closeCache);
    vm0.invoke(closeCache);
    
    if(useAccessor) {
      vm3.invoke(closeCache);
    }
    
    //We need to restart both VMs at the same time, because
    //they will wait for each other before allowing operations.
    AsyncInvocation async0 = vm0.invokeAsync(createPrRegion);
    AsyncInvocation async2 = vm2.invokeAsync(createPrRegion);
    async0.getResult(30000);
    async0.getResult(30000);
    
    if(useAccessor) {
      vm3.invoke(createAccessor);
    }
    
    // pause for async bucket recovery threads to finish their work.  Otherwise
    // the rebalance op may think that the other member doesn't have buckets, then
    // ask it to create them and get a negative reply because it actually does
    // have the buckets, causing the test to fail
    Wait.pause(10000);  
    
    //Try to rebalance again. This shouldn't do anything, because
    //we already recovered redundancy earlier.
    //Note that we don't check the primary balance. This rebalance
    //has a race with the redundancy recovery thread, which is also trying
    //to rebalance primaries. So this thread might end up rebalancing primaries,
    //or it might not.
    if(!simulate) {
      rebalanceVM.invoke(new SerializableRunnable("rebalance") {

        public void run() {
          Cache cache = getCache();
          ResourceManager manager = cache.getResourceManager();
          RebalanceResults results = doRebalance(simulate, manager);
          assertEquals(0, results.getTotalBucketCreatesCompleted());
          assertEquals(0, results.getTotalBucketTransfersCompleted());
          Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
          assertEquals(1, detailSet.size());
          PartitionRebalanceInfo details = detailSet.iterator().next();
          assertEquals(0, details.getBucketCreatesCompleted());
          assertEquals(0, details.getBucketTransfersCompleted());

          Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
          assertEquals(2, afterDetails.size());
          for(PartitionMemberInfo memberDetails: afterDetails) {
            assertEquals(6, memberDetails.getBucketCount());
            assertEquals(3, memberDetails.getPrimaryCount());
          }
        }
      });
      
      //Redundancy should be repaired.
      vm0.invoke(checkLowRedundancyAfterRebalance);
    }
    
    vm1.invoke(createPrRegion);
        
    //Look at vm0 buckets.
    assertEquals(vm0Buckets, getBucketList("region1", vm0));
    
    /*
     * Look at vm1 buckets.
     */
    if(!simulate) {
      /*
       * vm1 should have no buckets because offline buckets were recovered
       * when vm0 and vm2 were rebalanced above.
       */
      assertEquals(0,getBucketList("region1",vm1).size());
    } else {
      /*
       * No rebalancing above because the simulation flag is on.
       * Therefore, vm1 will have recovered its buckets.
       * We need to wait for the buckets because they
       * might still be in the middle of creation in the
       * background
       */
      waitForBucketList("region1", vm1, vm1Buckets);      
    }
    
    // look at vm2 buckets
    assertEquals(vm2Buckets, getBucketList("region1", vm2));
  }
  
  public void testMoveBucketsWithUnrecoveredValues() {
    moveBucketsWithUnrecoveredValuesRedundancy(false);
  }
  
  public void testBalanceBucketsByCountSimulation() {
    balanceBucketsByCount(true);
  }
  
  public void testBalanceBucketsByCount() {
    balanceBucketsByCount(false);
  }
  
  /**
   * Check to make sure that we balance
   * buckets between two hosts with no redundancy.
   * @param simulate
   */
  public void balanceBucketsByCount(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    LoadProbe oldProbe = setLoadProbe(vm0, new BucketCountLoadProbe());
    try {
      SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
        public void run()
        {
          Cache cache = getCache();
          AttributesFactory attr = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(0);
          paf.setRecoveryDelay(-1);
          paf.setStartupRecoveryDelay(-1);
          PartitionAttributes prAttr = paf.create();
          attr.setPartitionAttributes(prAttr);
          attr.setCacheLoader(new Bug40228Loader());
          cache.createRegion("region1", attr.create());
        }
      };

      //Create the region in only 1 VM
      vm0.invoke(createPrRegion);

      //Create some buckets with very uneven sizes
      vm0.invoke(new SerializableRunnable("createSomeBuckets") {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region1");
          region.put(Integer.valueOf(1), new byte[1024 * 1024]);
          region.put(Integer.valueOf(2), "A");
          region.put(Integer.valueOf(3), "A");
          region.put(Integer.valueOf(4), "A");
          region.put(Integer.valueOf(5), "A");
          region.put(Integer.valueOf(6), "A");
        }
      });

      //Create the region in the other VM (should have no effect)
      vm1.invoke(createPrRegion);

      //Now simulate a rebalance
      vm0.invoke(new SerializableRunnable("simulateRebalance") {

        public void run() {
          Cache cache = getCache();
          ResourceManager manager = cache.getResourceManager();
          RebalanceResults results = doRebalance(simulate, manager);
          assertEquals(0, results.getTotalBucketCreatesCompleted());
          assertEquals(0, results.getTotalPrimaryTransfersCompleted());
          assertEquals(3, results.getTotalBucketTransfersCompleted());
          assertTrue(0 < results.getTotalBucketTransferBytes());
          Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
          assertEquals(1, detailSet.size());
          PartitionRebalanceInfo details = detailSet.iterator().next();
          assertEquals(0, details.getBucketCreatesCompleted());
          assertEquals(0, details.getPrimaryTransfersCompleted());
          assertTrue(0 < details.getBucketTransferBytes());
          assertEquals(3, details.getBucketTransfersCompleted());

          Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
          assertEquals(2, afterDetails.size());
          for(PartitionMemberInfo memberDetails: afterDetails) {
            assertEquals(3, memberDetails.getBucketCount());
            assertEquals(3, memberDetails.getPrimaryCount());
          }
          if(!simulate) {
            verifyStats(manager, results);
          }
        }
      });

      if(!simulate) {
        SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkRedundancyFixed") {

          public void run() {
            Cache cache = getCache();
            Region region = cache.getRegion("region1");
            PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
            assertEquals(6, details.getCreatedBucketCount());
            assertEquals(0,details.getActualRedundantCopies());
            assertEquals(0,details.getLowRedundancyBucketCount());
            assertEquals(2, details.getPartitionMemberInfo().size());
            for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
              assertEquals(3, memberDetails.getBucketCount());
              assertEquals(3, memberDetails.getPrimaryCount());
            }

            //check to make sure that moving buckets didn't close the cache loader
            Bug40228Loader loader = (Bug40228Loader) cache.getRegion("region1").getAttributes().getCacheLoader();
            assertFalse(loader.isClosed());
          }
        };

        vm0.invoke(checkRedundancyFixed);
        vm1.invoke(checkRedundancyFixed);
      }
    } finally {
      setLoadProbe(vm0, oldProbe);
    }
  }
  
  private LoadProbe setLoadProbe(VM vm, final LoadProbe probe) {
    LoadProbe oldProbe = (LoadProbe) vm.invoke(new SerializableCallable("set load probe") {
      
      public Object call() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        InternalResourceManager mgr = cache.getResourceManager();
        return mgr.setLoadProbe(probe);
      }
    });
    
    return oldProbe;
  }

  /** 
   * Test to ensure that we wait for
   * in progress write operations before moving a primary.
   * @throws CancellationException 
   */
  public void moveBucketsWithUnrecoveredValuesRedundancy(final boolean simulate) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
        try {
        Cache cache = getCache();
        if(cache.findDiskStore("store") == null) {
         cache.createDiskStoreFactory()
         .setDiskDirs(getDiskDirs())
         .setMaxOplogSize(1)
         .create("store");
        }
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        attr.setDiskStoreName("store");
        attr.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setCacheLoader(new Bug40228Loader());
        cache.createRegion("region1", attr.create());
        } finally {
          System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
        }
      }
    };
    
    //Create the region in only 1 VM
    vm0.invoke(createPrRegion);
    
    
    //Create some buckets
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Integer.valueOf(1), "A");
        region.put(Integer.valueOf(2), "A");
        region.put(Integer.valueOf(3), "A");
        region.put(Integer.valueOf(4), "A");
        region.put(Integer.valueOf(5), "A");
        region.put(Integer.valueOf(6), "A");
      }
    });
    
    final long[] bucketSizes = (long[]) vm0.invoke(new SerializableCallable("get sizes and close cache") {

      public Object call() {
        PartitionedRegion region = (PartitionedRegion) getCache().getRegion("region1");
        PartitionedRegionDataStore dataStore = region.getDataStore();
        long[] bucketSizes = new long[7];
        for(int i =1; i <= 6; i++) {
          BucketRegion bucket = dataStore.getLocalBucketById(i);
          bucketSizes[i] = bucket.getTotalBytes();
          assertEquals(0, bucket.getNumOverflowBytesOnDisk());
          assertEquals(0, bucket.getNumOverflowOnDisk());
          assertEquals(1, bucket.getNumEntriesInVM());
        }
        getCache().close();
        
        return bucketSizes;
      }
    });
    
    //now recover the region
    vm0.invoke(createPrRegion);
    
    vm0.invoke(new SerializableRunnable("check sizes") {

      public void run() {
        PartitionedRegion region = (PartitionedRegion) getCache().getRegion("region1");
        PartitionedRegionDataStore dataStore = region.getDataStore();
        for(int i =1; i <= 6; i++) {
          BucketRegion bucket = dataStore.getLocalBucketById(i);
          assertEquals(1, bucket.getNumOverflowOnDisk());
          assertEquals(0, bucket.getNumEntriesInVM());
          //the size recorded on disk is not the same is the in memory size, apparently
          assertTrue("Bucket size was " + bucket.getNumOverflowBytesOnDisk(), 1 < bucket.getNumOverflowBytesOnDisk());
          assertEquals(bucket.getNumOverflowBytesOnDisk(), bucket.getTotalBytes());
        }
      }
    });
    
    //Create the region in the other VM (should have no effect)
    vm1.invoke(createPrRegion);
    
    //Now simulate a rebalance
    vm0.invoke(new SerializableRunnable("simulateRebalance") {
      
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        RebalanceResults results = doRebalance(simulate, manager);
        assertEquals(0, results.getTotalBucketCreatesCompleted());
        assertEquals(0, results.getTotalPrimaryTransfersCompleted());
        assertEquals(3, results.getTotalBucketTransfersCompleted());
        assertTrue("Transfered Bytes = " + results.getTotalBucketTransferBytes(), 0 < results.getTotalBucketTransferBytes());
        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertEquals(1, detailSet.size());
        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertEquals(0, details.getBucketCreatesCompleted());
        assertEquals(0, details.getPrimaryTransfersCompleted());
        assertTrue(0 < details.getBucketTransferBytes());
        assertEquals(3, details.getBucketTransfersCompleted());
        
        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertEquals(2, afterDetails.size());
        for(PartitionMemberInfo memberDetails: afterDetails) {
          assertEquals(3, memberDetails.getBucketCount());
          assertEquals(3, memberDetails.getPrimaryCount());
        }
        if(!simulate) {
          verifyStats(manager, results);
        }
      }
    });

    if(!simulate) {
      SerializableRunnable checkRedundancyFixed = new SerializableRunnable("checkRedundancyFixed") {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region1");
          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertEquals(6, details.getCreatedBucketCount());
          assertEquals(0,details.getActualRedundantCopies());
          assertEquals(0,details.getLowRedundancyBucketCount());
          assertEquals(2, details.getPartitionMemberInfo().size());
          for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
            assertEquals(3, memberDetails.getBucketCount());
            assertEquals(3, memberDetails.getPrimaryCount());
          }
          
          //check to make sure that moving buckets didn't close the cache loader
          Bug40228Loader loader = (Bug40228Loader) cache.getRegion("region1").getAttributes().getCacheLoader();
          assertFalse(loader.isClosed());
        }
      };

      vm0.invoke(checkRedundancyFixed);
      vm1.invoke(checkRedundancyFixed);
      
      SerializableRunnable checkBug40228Fixed = new SerializableRunnable("checkBug40228Fixed") {

        public void run() {
          Cache cache = getCache();
          Bug40228Loader loader = (Bug40228Loader) cache.getRegion("region1").getAttributes().getCacheLoader();
          assertFalse(loader.isClosed());
          //check to make sure that closing the PR closes the cache loader
          cache.getRegion("region1").close();
          assertTrue(loader.isClosed());
        }
      };
      
      vm0.invoke(checkBug40228Fixed);
      vm1.invoke(checkBug40228Fixed);
    }
  }
  
  //TODO test colocated regions where buckets aren't created for all subregions
  
  //TODO test colocated regions where members aren't consistent in which regions they have
  
  private void verifyStats(ResourceManager manager, RebalanceResults results) {
    InternalResourceManager mgr  = (InternalResourceManager) manager;
    ResourceManagerStats stats = mgr.getStats();
    
    assertEquals(0, stats.getRebalancesInProgress());
    assertEquals(1, stats.getRebalancesCompleted());
    assertEquals(0, stats.getRebalanceBucketCreatesInProgress());
    assertEquals(results.getTotalBucketCreatesCompleted(), stats.getRebalanceBucketCreatesCompleted());
    assertEquals(0, stats.getRebalanceBucketCreatesFailed());
    //The time stats may not be exactly the same, because they are not
    //incremented at exactly the same time.
    assertEquals(results.getTotalBucketCreateTime(), TimeUnit.NANOSECONDS.toMillis(stats.getRebalanceBucketCreateTime()), 2000);
    assertEquals(results.getTotalBucketCreateBytes(), stats.getRebalanceBucketCreateBytes());
    assertEquals(0, stats.getRebalanceBucketTransfersInProgress());
    assertEquals(results.getTotalBucketTransfersCompleted(), stats.getRebalanceBucketTransfersCompleted());
    assertEquals(0, stats.getRebalanceBucketTransfersFailed());
    assertEquals(results.getTotalBucketTransferTime(), TimeUnit.NANOSECONDS.toMillis(stats.getRebalanceBucketTransfersTime()), 2000);
    assertEquals(results.getTotalBucketTransferBytes(), stats.getRebalanceBucketTransfersBytes());
    assertEquals(0, stats.getRebalancePrimaryTransfersInProgress());
    assertEquals(results.getTotalPrimaryTransfersCompleted(), stats.getRebalancePrimaryTransfersCompleted());
    assertEquals(0, stats.getRebalancePrimaryTransfersFailed());
    assertEquals(results.getTotalPrimaryTransferTime(), TimeUnit.NANOSECONDS.toMillis(stats.getRebalancePrimaryTransferTime()), 2000);
  }
  
  private Set<Integer> getBucketList(final String regionName, VM vm0) {
    SerializableCallable getBuckets = new SerializableCallable("get buckets") {
      
      public Object call() throws Exception {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        return new TreeSet<Integer>(region.getDataStore().getAllLocalBucketIds());
      }
    };
    
    return (Set<Integer>) vm0.invoke(getBuckets);
  }
  
  private void waitForBucketList(final String regionName, VM vm0, final Collection<Integer> expected) {
    SerializableCallable getBuckets = new SerializableCallable("get buckets") {
      
      public Object call() throws Exception {
        Cache cache = getCache();
        final PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        
        Wait.waitForCriterion(new WaitCriterion() {
          
          @Override
          public boolean done() {
            TreeSet<Integer> results = getBuckets();
            return results.equals(expected);
          }

          protected TreeSet<Integer> getBuckets() {
            TreeSet<Integer> results = new TreeSet<Integer>(region.getDataStore().getAllLocalBucketIds());
            return results;
          }
          
          @Override
          public String description() {
            return "Timeout waiting for buckets to match. Expected " + expected + " but got " + getBuckets();
          }
        }, 60000, 100, true);
        
        return null;
      }
    };
    
    vm0.invoke(getBuckets);
  }

  private static class BlockingCacheListener extends CacheListenerAdapter {
    CountDownLatch operationStartedLatch;
    CountDownLatch resumeOperationLatch = new CountDownLatch(1);
    
    public BlockingCacheListener(int threads) {
      operationStartedLatch = new CountDownLatch(threads);
    }

    public void waitForOperationsStarted() throws InterruptedException {
      operationStartedLatch.await(MAX_WAIT, TimeUnit.SECONDS);
      
    }

    public void close() {
      
    }
    
    private void doWait() {
      operationStartedLatch.countDown();
      try {
        resumeOperationLatch.await(MAX_WAIT, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new CacheWriterException(e);
      }
      
    }

    @Override
    public void afterUpdate(EntryEvent event) throws CacheWriterException {
      doWait();
    }
    
    @Override
    public void afterCreate(EntryEvent event) {
      doWait();
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      doWait();
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      doWait();
    }

    public void release() {
      resumeOperationLatch.countDown();
    }
  }
  
  private static interface Operation {

    void execute(Region region, Integer key);
    
  }
  
  private static class Bug40228Loader implements CacheLoader {
    private volatile boolean closed = false;

    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return null;
    }

    public boolean isClosed() {
      return closed;
    }

    public void close() {
      closed =true;
    }
    
  }
}
