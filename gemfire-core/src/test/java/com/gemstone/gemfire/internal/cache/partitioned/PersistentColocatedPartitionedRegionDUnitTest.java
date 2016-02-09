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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.RequestImageMessage;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserver;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * @author dsmith
 *
 */
public class PersistentColocatedPartitionedRegionDUnitTest extends
    PersistentPartitionedRegionTestBase {

  private static final int NUM_BUCKETS = 15;
  private static final int MAX_WAIT = 30 * 1000;

  /**
   * @param name
   */
  public PersistentColocatedPartitionedRegionDUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    FileUtil.delete(getBackupDir());
  }
  
  public void testColocatedPRAttributes() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(1);
    
    vm0.invoke(new SerializableRunnable("create") {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        
        //Create Persistent region
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion("persistentLeader", af.create());
        
        af.setDataPolicy(DataPolicy.PARTITION);
        af.setDiskStoreName(null);
        cache.createRegion("nonPersistentLeader", af.create());
        
        
        //Create a non persistent PR
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        paf.setColocatedWith("nonPersistentLeader");
        af.setPartitionAttributes(paf.create());
        
        //Try to colocate a persistent PR with the non persistent PR. This should fail.
        IgnoredException exp = IgnoredException.addIgnoredException("IllegalStateException");
        try {
          cache.createRegion("colocated", af.create());
          fail("should not have been able to create a persistent region colocated with a non persistent region");
        } catch(IllegalStateException expected) {
          //do nothing
        } finally {
          exp.remove();
        }
        
        //Try to colocate a persistent PR with another persistent PR. This should work.
        paf.setColocatedWith("persistentLeader");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("colocated", af.create());
        
        //We should also be able to colocate a non persistent region with a persistent region.
        af.setDataPolicy(DataPolicy.PARTITION);
        af.setDiskStoreName(null);
        paf.setColocatedWith("persistentLeader");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("colocated2", af.create());
      }
    });
  }
  
  /**
   * Testing that we can colocate persistent PRs
   */
  public void testColocatedPRs() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    SerializableRunnable createPRs = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
        
        paf.setColocatedWith(PR_REGION_NAME);
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
        paf.setColocatedWith("region2");
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PARTITION);
        af.setDiskStoreName(null);
        cache.createRegion("region3", af.create());
      }
    };
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    vm2.invoke(createPRs);
    
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");
    createData(vm0, 0, NUM_BUCKETS, "c", "region3");
    
    Set<Integer> vm0Buckets = getBucketList(vm0, PR_REGION_NAME);
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    assertEquals(vm0Buckets, getBucketList(vm0, "region3"));
    Set<Integer> vm1Buckets = getBucketList(vm1, PR_REGION_NAME);
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    assertEquals(vm1Buckets, getBucketList(vm1, "region3"));
    Set<Integer> vm2Buckets = getBucketList(vm2, PR_REGION_NAME);
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));
    assertEquals(vm2Buckets, getBucketList(vm2, "region3"));

    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);
    
    AsyncInvocation async0 = vm0.invokeAsync(createPRs);
    AsyncInvocation async1 = vm1.invokeAsync(createPRs);
    AsyncInvocation async2 = vm2.invokeAsync(createPRs);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);
    

    //The secondary buckets can be recovered asynchronously,
    //so wait for them to come back.
    waitForBuckets(vm0, vm0Buckets, PR_REGION_NAME);
    waitForBuckets(vm0, vm0Buckets, "region2");
    waitForBuckets(vm1, vm1Buckets, PR_REGION_NAME);
    waitForBuckets(vm1, vm1Buckets, "region2");

    checkData(vm0, 0, NUM_BUCKETS, "a");
    checkData(vm0, 0, NUM_BUCKETS, "b", "region2");
    
    //region 3 didn't have persistent data, so it nothing should be recovered
    checkData(vm0, 0, NUM_BUCKETS, null, "region3");
    
    //Make sure can do a put in all of the buckets in region 3
    createData(vm0, 0, NUM_BUCKETS, "c", "region3");
    //Now all of those buckets should exist.
    checkData(vm0, 0, NUM_BUCKETS, "c", "region3");
    //The region 3 buckets should be restored in the appropriate places.
    assertEquals(vm0Buckets,getBucketList(vm0, "region3"));
    assertEquals(vm1Buckets, getBucketList(vm1, "region3"));
    assertEquals(vm2Buckets, getBucketList(vm2, "region3"));

  }

  /**
   * Testing what happens we we recreate colocated persistent PRs by creating
   * one PR everywhere and then the other PR everywhere.
   */
  public void testColocatedPRsRecoveryOnePRAtATime() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createParentPR = new SerializableRunnable("createParentPR") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs())
              .create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
      }
    };
    SerializableRunnable createChildPR = getCreateChildPRRunnable();
    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm2.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    vm2.invoke(createChildPR);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, PR_REGION_NAME);
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    Set<Integer> vm1Buckets = getBucketList(vm1, PR_REGION_NAME);
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    Set<Integer> vm2Buckets = getBucketList(vm2, PR_REGION_NAME);
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));
    
    Set<Integer> vm0PrimaryBuckets = getPrimaryBucketList(vm0, PR_REGION_NAME);
    assertEquals(vm0PrimaryBuckets, getPrimaryBucketList(vm0, "region2"));
    Set<Integer> vm1PrimaryBuckets = getPrimaryBucketList(vm1, PR_REGION_NAME);
    assertEquals(vm1PrimaryBuckets, getPrimaryBucketList(vm1, "region2"));
    Set<Integer> vm2PrimaryBuckets = getPrimaryBucketList(vm2, PR_REGION_NAME);
    assertEquals(vm2PrimaryBuckets, getPrimaryBucketList(vm2, "region2"));
    
    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);

    AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
    AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
    AsyncInvocation async2 = vm2.invokeAsync(createParentPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);

    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    vm2.invoke(createChildPR);
    
    Wait.pause(4000);
    
    assertEquals(vm0Buckets, getBucketList(vm0, PR_REGION_NAME));
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    assertEquals(vm1Buckets, getBucketList(vm1, PR_REGION_NAME));
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    assertEquals(vm2Buckets, getBucketList(vm2, PR_REGION_NAME));
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));
    
    //primary can differ
    vm0PrimaryBuckets = getPrimaryBucketList(vm0, PR_REGION_NAME);
    assertEquals(vm0PrimaryBuckets, getPrimaryBucketList(vm0, "region2"));
    vm1PrimaryBuckets = getPrimaryBucketList(vm1, PR_REGION_NAME);
    assertEquals(vm1PrimaryBuckets, getPrimaryBucketList(vm1, "region2"));
    vm2PrimaryBuckets = getPrimaryBucketList(vm2, PR_REGION_NAME);
    assertEquals(vm2PrimaryBuckets, getPrimaryBucketList(vm2, "region2"));
    

    checkData(vm0, 0, NUM_BUCKETS, "a");

    // region 2 didn't have persistent data, so it nothing should be recovered
    checkData(vm0, 0, NUM_BUCKETS, null, "region2");

    // Make sure can do a put in all of the buckets in vm2
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");

    // Now all of those buckets should exist
    checkData(vm0, 0, NUM_BUCKETS, "c", "region2");

    // Now all the buckets should be restored in the appropriate places.
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));
  }

  private SerializableRunnable getCreateChildPRRunnable() {
    return new SerializableRunnable("createChildPR") {
      public void run() {
        Cache cache = getCache();
        
        final CountDownLatch recoveryDone = new CountDownLatch(1);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            if(region.getName().equals("region2")){
              recoveryDone.countDown();
            }
          }
        };
        InternalResourceManager.setResourceObserver(observer );

        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setColocatedWith(PR_REGION_NAME);
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
        
        try {
          recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        } 
      }
    };
  }
  
  public void testColocatedPRsRecoveryOneMemberLater() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createParentPR = new SerializableRunnable("createParentPR") {
      public void run() {
        Cache cache = getCache();

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs())
              .create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
      }
    };
    SerializableRunnable createChildPR =getCreateChildPRRunnable(); 
        
    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm2.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    vm2.invoke(createChildPR);

    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");

    Set<Integer> vm0Buckets = getBucketList(vm0, PR_REGION_NAME);
    assertEquals(vm0Buckets, getBucketList(vm0, "region2"));
    Set<Integer> vm1Buckets = getBucketList(vm1, PR_REGION_NAME);
    assertEquals(vm1Buckets, getBucketList(vm1, "region2"));
    Set<Integer> vm2Buckets = getBucketList(vm2, PR_REGION_NAME);
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));
  
    Set<Integer> vm0PrimaryBuckets = getPrimaryBucketList(vm0, PR_REGION_NAME);
    assertEquals(vm0PrimaryBuckets, getPrimaryBucketList(vm0, "region2"));
    Set<Integer> vm1PrimaryBuckets = getPrimaryBucketList(vm1, PR_REGION_NAME);
    assertEquals(vm1PrimaryBuckets, getPrimaryBucketList(vm1, "region2"));
    Set<Integer> vm2PrimaryBuckets = getPrimaryBucketList(vm2, PR_REGION_NAME);
    assertEquals(vm2PrimaryBuckets, getPrimaryBucketList(vm2, "region2"));
    
    
    closeCache(vm2);
    // Make sure the other members notice that vm2 has gone
    // TODO use a callback for this.
    Thread.sleep(4000);
    
    closeCache(vm0);
    closeCache(vm1);

    // Create the members, but don't initialize
    // VM2 yet
    AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
    AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    waitForBucketRecovery(vm0, vm0Buckets);
    waitForBucketRecovery(vm1, vm1Buckets);
    
    

    checkData(vm0, 0, NUM_BUCKETS, "a");

    // region 2 didn't have persistent data, so it nothing should be recovered
    checkData(vm0, 0, NUM_BUCKETS, null, "region2");

    // Make sure can do a put in all of the buckets in vm2
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");

    // Now all of those buckets should exist
    checkData(vm0, 0, NUM_BUCKETS, "c", "region2");

    // Now we initialize vm2.
    vm2.invoke(createParentPR);
    // Make sure vm2 hasn't created any buckets in the parent PR yet
    // We don't want any buckets until the child PR is created
    assertEquals(Collections.emptySet(), getBucketList(vm2, PR_REGION_NAME));
    vm2.invoke(createChildPR);
    
    // Now vm2 should have created all of the appropriate buckets.
    assertEquals(vm2Buckets, getBucketList(vm2, PR_REGION_NAME));
    assertEquals(vm2Buckets, getBucketList(vm2, "region2"));
    
    vm0PrimaryBuckets = getPrimaryBucketList(vm0, PR_REGION_NAME);
    assertEquals(vm0PrimaryBuckets, getPrimaryBucketList(vm0, "region2"));
    vm1PrimaryBuckets = getPrimaryBucketList(vm1, PR_REGION_NAME);
    assertEquals(vm1PrimaryBuckets, getPrimaryBucketList(vm1, "region2"));
    vm2PrimaryBuckets = getPrimaryBucketList(vm2, PR_REGION_NAME);
    assertEquals(vm2PrimaryBuckets, getPrimaryBucketList(vm2, "region2"));
  }
  
  public void testReplaceOfflineMemberAndRestart() throws Throwable {
    SerializableRunnable createPRs = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        
        final CountDownLatch recoveryDone = new CountDownLatch(2);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            recoveryDone.countDown();
          }
        };
        InternalResourceManager.setResourceObserver(observer );
        
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
        
        paf.setColocatedWith(PR_REGION_NAME);
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
        
        try {
          if(!recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS)) {
            fail("timed out");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };
    
    replaceOfflineMemberAndRestart(createPRs);
  }
  
  /**
   * Test that if we replace an offline member, even if colocated regions are
   * in different disk stores, we still keep our metadata consistent.
   * @throws Throwable
   */
  public void testReplaceOfflineMemberAndRestartTwoDiskStores() throws Throwable {
    SerializableRunnable createPRs = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        
        final CountDownLatch recoveryDone = new CountDownLatch(2);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            recoveryDone.countDown();
          }
        };
        InternalResourceManager.setResourceObserver(observer );
        
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
        
        DiskStore ds2 = cache.findDiskStore("disk2");
        if(ds2 == null) {
          ds2 = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk2");
        }
        
        paf.setColocatedWith(PR_REGION_NAME);
        af.setPartitionAttributes(paf.create());
        af.setDiskStoreName("disk2");
        cache.createRegion("region2", af.create());
        
        try {
          if(!recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS)) {
            fail("timed out");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };
    
    replaceOfflineMemberAndRestart(createPRs);
  }
  
  /**
   * Test for support issue 7870.
   * 1. Run three members with redundancy 1 and recovery delay 0
   * 2. Kill one of the members, to trigger replacement of buckets
   * 3. Shutdown all members and restart.
   * 
   * What was happening is that in the parent PR, we discarded
   * our offline data in one member, but in the child PR the other
   * members ended up waiting for the child bucket to be created
   * in the member that discarded it's offline data.
   * 
   * @throws Throwable 
   */
  public void replaceOfflineMemberAndRestart(SerializableRunnable createPRs) throws Throwable {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    //Create the PR on three members
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    vm2.invoke(createPRs);
    
    //Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");
    
    //Close one of the members to trigger redundancy recovery.
    closeCache(vm2);
    
    //Wait until redundancy is recovered.
    waitForRedundancyRecovery(vm0, 1, PR_REGION_NAME);
    waitForRedundancyRecovery(vm0, 1, "region2");
    
    createData(vm0, 0, NUM_BUCKETS, "b");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");
    IgnoredException expected = IgnoredException.addIgnoredException("PartitionOfflineException");
    try {
    
    //Close the remaining members.
    vm0.invoke(new SerializableCallable() {
      
      public Object call() throws Exception {
        InternalDistributedSystem ds = (InternalDistributedSystem) getCache().getDistributedSystem();
        AdminDistributedSystemImpl.shutDownAllMembers(ds.getDistributionManager(), 600000);
        return null;
      }
    });
    
    //Make sure that vm-1 is completely disconnected
    //The shutdown all asynchronously finishes the disconnect after
    //replying to the admin member.
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        system.disconnect();
      }
    });

    //Recreate the members. Try to make sure that
    //the member with the latest copy of the buckets
    //is the one that decides to throw away it's copy
    //by starting it last.
    AsyncInvocation async0 = vm0.invokeAsync(createPRs);
    AsyncInvocation async1 = vm1.invokeAsync(createPRs);
    Wait.pause(2000);
    AsyncInvocation async2 = vm2.invokeAsync(createPRs);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);
    
    checkData(vm0, 0, NUM_BUCKETS, "b");
    checkData(vm0, 0, NUM_BUCKETS, "b", "region2");
    
    waitForRedundancyRecovery(vm0, 1, PR_REGION_NAME);
    waitForRedundancyRecovery(vm0, 1, "region2");
    waitForRedundancyRecovery(vm1, 1, PR_REGION_NAME);
    waitForRedundancyRecovery(vm1, 1, "region2");
    waitForRedundancyRecovery(vm2, 1, PR_REGION_NAME);
    waitForRedundancyRecovery(vm2, 1, "region2");
    
    //Make sure we don't have any extra buckets after the restart
    int totalBucketCount = getBucketList(vm0).size();
    totalBucketCount += getBucketList(vm1).size();
    totalBucketCount += getBucketList(vm2).size();
    
    assertEquals(2 * NUM_BUCKETS, totalBucketCount);
    
    totalBucketCount = getBucketList(vm0, "region2").size();
    totalBucketCount += getBucketList(vm1, "region2").size();
    totalBucketCount += getBucketList(vm2, "region2").size();
    
    assertEquals(2 * NUM_BUCKETS, totalBucketCount);
    } finally {
      expected.remove();
    }
  }
  
  public void testReplaceOfflineMemberAndRestartCreateColocatedPRLate() throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
      }
    };
    
    SerializableRunnable createChildPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        
        final CountDownLatch recoveryDone = new CountDownLatch(1);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            if(region.getName().contains("region2")) {
              recoveryDone.countDown();
            }
          }
        };
        InternalResourceManager.setResourceObserver(observer );
        
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        paf.setColocatedWith(PR_REGION_NAME);
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
        
        try {
          if(!recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS)) {
            fail("timed out");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };
    
    replaceOfflineMemberAndRestartCreateColocatedPRLate(createParentPR, createChildPR);
  }
  
  public void testReplaceOfflineMemberAndRestartCreateColocatedPRLateTwoDiskStores() throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
      }
    };
    
    SerializableRunnable createChildPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        
        final CountDownLatch recoveryDone = new CountDownLatch(1);
        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            if(region.getName().contains("region2")) {
              recoveryDone.countDown();
            }
          }
        };
        InternalResourceManager.setResourceObserver(observer );
        
        DiskStore ds2 = cache.findDiskStore("disk2");
        if(ds2 == null) {
          ds2 = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk2");
        }
        
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(0);
        paf.setColocatedWith(PR_REGION_NAME);
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk2");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
        
        try {
          if(!recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS)) {
            fail("timed out");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };
    
    replaceOfflineMemberAndRestartCreateColocatedPRLate(createParentPR, createChildPR);
  }
  
  /**
   * Test for support issue 7870.
   * 1. Run three members with redundancy 1 and recovery delay 0
   * 2. Kill one of the members, to trigger replacement of buckets
   * 3. Shutdown all members and restart.
   * 
   * What was happening is that in the parent PR, we discarded
   * our offline data in one member, but in the child PR the other
   * members ended up waiting for the child bucket to be created
   * in the member that discarded it's offline data.
   * 
   * In this test case, we're creating the child PR later,
   * after the parent buckets have already been completely created.
   * 
   * @throws Throwable 
   */
  public void replaceOfflineMemberAndRestartCreateColocatedPRLate(SerializableRunnable createParentPR, SerializableRunnable createChildPR) throws Throwable {
    IgnoredException.addIgnoredException("PartitionOfflineException");
    IgnoredException.addIgnoredException("RegionDestroyedException");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    

    //Create the PRs on three members
    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm2.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    vm2.invoke(createChildPR);
    
    //Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");
    
    //Close one of the members to trigger redundancy recovery.
    closeCache(vm2);
    
    //Wait until redundancy is recovered.
    waitForRedundancyRecovery(vm0, 1, PR_REGION_NAME);
    waitForRedundancyRecovery(vm0, 1, "region2");
    
    createData(vm0, 0, NUM_BUCKETS, "b");
    createData(vm0, 0, NUM_BUCKETS, "b", "region2");
    
    //Close the remaining members.
    vm0.invoke(new SerializableCallable() {
      
      public Object call() throws Exception {
        InternalDistributedSystem ds = (InternalDistributedSystem) getCache().getDistributedSystem();
        AdminDistributedSystemImpl.shutDownAllMembers(ds.getDistributionManager(), 0);
        return null;
      }
    });
    
    //Make sure that vm-1 is completely disconnected
    //The shutdown all asynchronously finishes the disconnect after
    //replying to the admin member.
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        system.disconnect();
      }
    });

    //Recreate the parent region. Try to make sure that
    //the member with the latest copy of the buckets
    //is the one that decides to throw away it's copy
    //by starting it last.
    AsyncInvocation async2 = vm2.invokeAsync(createParentPR);
    AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
    Wait.pause(2000);
    AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);
    
    //Wait for async tasks
    Wait.pause(2000);
    
    //Recreate the child region. 
    async2 = vm2.invokeAsync(createChildPR);
    async1 = vm1.invokeAsync(createChildPR);
    async0 = vm0.invokeAsync(createChildPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);
    
    //Validate the data
    checkData(vm0, 0, NUM_BUCKETS, "b");
    checkData(vm0, 0, NUM_BUCKETS, "b", "region2");
    
    //Make sure we can actually use the buckets in the child region.
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");
    
    waitForRedundancyRecovery(vm0, 1, PR_REGION_NAME);
    waitForRedundancyRecovery(vm0, 1, "region2");
    
    //Make sure we don't have any extra buckets after the restart
    int totalBucketCount = getBucketList(vm0).size();
    totalBucketCount += getBucketList(vm1).size();
    totalBucketCount += getBucketList(vm2).size();
    
    assertEquals(2 * NUM_BUCKETS, totalBucketCount);
    
    totalBucketCount = getBucketList(vm0, "region2").size();
    totalBucketCount += getBucketList(vm1, "region2").size();
    totalBucketCount += getBucketList(vm2, "region2").size();
    
    assertEquals(2 * NUM_BUCKETS, totalBucketCount); 
  }
  
  /**
   * Test what happens when we crash in the middle of
   * satisfying redundancy for a colocated bucket.
   * @throws Throwable
   */
  //This test method is disabled because it is failing
  //periodically and causing cruise control failures
  //See bug #46748
  public void testCrashDuringRedundancySatisfaction() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    SerializableRunnable createPRs = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        //Workaround for 44414 - disable recovery delay so we shutdown
        //vm1 at a predictable point.
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
        
        paf.setColocatedWith(PR_REGION_NAME);
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };
    
    //Create the PR on vm0
    vm0.invoke(createPRs);
    
    
    //Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");
    
    vm1.invoke(createPRs);
    
    //We shouldn't have created any buckets in vm1 yet.
    assertEquals(Collections.emptySet(), getBucketList(vm1));
    
  //Add an observer that will disconnect before allowing the peer to 
    //GII a colocated bucket. This should leave the peer with only the parent
    //bucket
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof RequestImageMessage) {
              if(((RequestImageMessage) message).regionPath.contains("region2")) {
                DistributionMessageObserver.setInstance(null);
                disconnectFromDS();
              }
            }
          }
        });
      }
    });
    
    IgnoredException ex = IgnoredException.addIgnoredException("PartitionOfflineException", vm1);
    try {
      
    //Do a rebalance to create buckets in vm1. THis will cause vm0 to disconnect
    //as we satisfy redundancy with vm1.
      try {
        RebalanceResults rr = rebalance(vm1);
      } catch(Exception expected) {
        //We expect to see a partition offline exception because of the
        //disconnect
        if(!(expected.getCause() instanceof PartitionOfflineException)) {
          throw expected;
        }
      }
    
    
    //Wait for vm0 to be closed by the callback
    vm0.invoke(new SerializableCallable() {
      
      public Object call() throws Exception {
        Wait.waitForCriterion(new WaitCriterion() {
          public boolean done() {
            InternalDistributedSystem ds = system;
            return ds == null || !ds.isConnected();
          }
          
          public String description() {
            return "DS did not disconnect";
          }
        }, MAX_WAIT, 100, true);
        
        return null;
      }
    });
    
    //close the cache in vm1
    SerializableCallable disconnectFromDS = new SerializableCallable() {
      
      public Object call() throws Exception {
        disconnectFromDS();
        return null;
      }
    };
    vm1.invoke(disconnectFromDS);
    
    //Make sure vm0 is disconnected. This avoids a race where we
    //may still in the process of disconnecting even though the our async listener
    //found the system was disconnected
    vm0.invoke(disconnectFromDS);
    } finally {
      ex.remove();
    }
    
    //Create the cache and PRs on both members
    AsyncInvocation async0 = vm0.invokeAsync(createPRs);
    AsyncInvocation async1 = vm1.invokeAsync(createPRs);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    
    //Make sure the data was recovered correctly
    checkData(vm0, 0, NUM_BUCKETS, "a");
    // Workaround for bug 46748.
    checkData(vm0, 0, NUM_BUCKETS, "a", "region2");
  }
  
  /**
   * Test what happens when we restart persistent members while
   * there is an accessor concurrently performing puts. This is for bug 43899
   */
  public void testRecoverySystemWithConcurrentPutter() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    //Define all of the runnables used in this test
    
    //runnable to create accessors
    SerializableRunnable createAccessor = new SerializableRunnable("createAccessor") {
      public void run() {
        Cache cache = getCache();
        
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setLocalMaxMemory(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PARTITION);
        cache.createRegion(PR_REGION_NAME, af.create());
        
        paf.setColocatedWith(PR_REGION_NAME);
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };
    
    //runnable to create PRs
    SerializableRunnable createPRs = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
        
        paf.setColocatedWith(PR_REGION_NAME);
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };
    
    //runnable to close the cache.
    SerializableRunnable closeCache = new SerializableRunnable("region1") {
      public void run() {
        closeCache();
      }
    };
    
    //Runnable to do a bunch of puts handle exceptions
    //due to the fact that member is offline.
    SerializableRunnable doABunchOfPuts = new SerializableRunnable("region1") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(PR_REGION_NAME);
        try {
        for(int i =0;;i++) {
          try {
            region.get(i % NUM_BUCKETS);
          } catch(PartitionOfflineException expected) {
            //do nothing.
          } catch(PartitionedRegionStorageException expected) {
            //do nothing.
          }
          Thread.yield();
        }
        } catch(CacheClosedException expected) {
          //ok, we're done.
        }
      }
    };
    
    
    //Runnable to clean up disk dirs on a members
    SerializableRunnable cleanDiskDirs = new SerializableRunnable("Clean disk dirs") {
      public void run() {
        try {
          cleanDiskDirs();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    
    //Create the PR two members
    vm1.invoke(createPRs);
    vm2.invoke(createPRs);
    
    //create the accessor.
    vm0.invoke(createAccessor);
    
    
    //Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");
    

    //backup the system. We use this to get a snapshot of vm1 and vm2
    //when they both are online. Recovering from this backup simulates
    //a simulataneous kill and recovery.
    backup(vm3);
    
    //close vm1 and vm2.
    vm1.invoke(closeCache);
    vm2.invoke(closeCache);
    
    //restore the backup
    vm1.invoke(cleanDiskDirs);
    vm2.invoke(cleanDiskDirs);
    restoreBackup(2);
    
    //in vm0, start doing a bunch of concurrent puts.
    AsyncInvocation async0 = vm0.invokeAsync(doABunchOfPuts);

    //This recovery should not hang (that's what we're testing for
    //here.
    AsyncInvocation async1 = vm1.invokeAsync(createPRs);
    AsyncInvocation async2 = vm2.invokeAsync(createPRs);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);
  
    //close the cache in vm0 to stop the async puts.
    vm0.invoke(closeCache);
    
    //make sure we didn't get an exception
    async0.getResult(MAX_WAIT);
  }
  
  public void testRebalanceWithOfflineChildRegion() throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
      }
    };
    
    SerializableRunnable createChildPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(0);
        paf.setColocatedWith(PR_REGION_NAME);
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };
    
    rebalanceWithOfflineChildRegion(createParentPR, createChildPR);
    
  }
  
  /**
   * Test that a rebalance will regions are in the middle of recovery
   * doesn't cause issues.
   * 
   * This is slightly different than {{@link #testRebalanceWithOfflineChildRegion()}
   * because in this case all of the regions have been created, but
   * they are in the middle of actually recovering buckets from disk.
   */
  public void testRebalanceDuringRecovery() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    SerializableRunnable createPRs = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
        
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setColocatedWith(PR_REGION_NAME);
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };
    

    //Create the PRs on two members
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    
    //Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");
    
    //Close the members
    closeCache(vm1);
    closeCache(vm0);
    
    SerializableRunnable addHook = new SerializableRunnable() {
      @Override
      public void run() {
        PartitionedRegionObserverHolder.setInstance(new PRObserver());
      }
    };
    
    SerializableRunnable waitForHook = new SerializableRunnable() {
      @Override
      public void run() {
        PRObserver observer = (PRObserver) PartitionedRegionObserverHolder.getInstance();
        try {
          observer.waitForCreate();
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };
    
    SerializableRunnable removeHook = new SerializableRunnable() {
      @Override
      public void run() {
        PRObserver observer = (PRObserver) PartitionedRegionObserverHolder.getInstance();
        observer.release();
        PartitionedRegionObserverHolder.setInstance(new PartitionedRegionObserverAdapter());
      }
    };
    
    vm1.invoke(addHook);
//    vm1.invoke(addHook);
    AsyncInvocation async0;
    AsyncInvocation async1;
    AsyncInvocation async2;
    RebalanceResults rebalanceResults;
    try {
      async0 = vm0.invokeAsync(createPRs);
      async1 = vm1.invokeAsync(createPRs);

      vm1.invoke(waitForHook);
//      vm1.invoke(waitForHook);
      
      //Now create the parent region on vm-2. vm-2 did not
      //previous host the child region.
      vm2.invoke(createPRs);

      //Try to forcibly move some buckets to vm2 (this should not succeed).
      moveBucket(0, vm1, vm2);
      moveBucket(1, vm1, vm2);
    
    } finally {
      vm1.invoke(removeHook);
//      vm1.invoke(removeHook);
    }
    
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    
    //Validate the data
    checkData(vm0, 0, NUM_BUCKETS, "a");
    checkData(vm0, 0, NUM_BUCKETS, "a", "region2");
    
    //Make sure we can actually use the buckets in the child region.
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");
    
    //Make sure the system is recoverable
    //by restarting it
    closeCache(vm2);
    closeCache(vm1);
    closeCache(vm0);
    
    async0 = vm0.invokeAsync(createPRs);
    async1 = vm1.invokeAsync(createPRs);
    async2 = vm2.invokeAsync(createPRs);
    async0.getResult();
    async1.getResult();
    async2.getResult();
  }
  
  public void testRebalanceWithOfflineChildRegionTwoDiskStores() throws Throwable {
    SerializableRunnable createParentPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        cache.createRegion(PR_REGION_NAME, af.create());
      }
    };
    
    SerializableRunnable createChildPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        
        DiskStore ds2 = cache.findDiskStore("disk2");
        if(ds2 == null) {
          ds2 = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk2");
        }
        
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setRecoveryDelay(0);
        paf.setColocatedWith(PR_REGION_NAME);
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk2");
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region2", af.create());
      }
    };
    
    rebalanceWithOfflineChildRegion(createParentPR, createChildPR);
  }
  
  /**
   * Test that a user is not allowed to change the colocation of 
   * a PR with persistent data.
   * @throws Throwable
   */
  public void testModifyColocation() throws Throwable {
    //Create PRs where region3 is colocated with region1.
    createColocatedPRs("region1");
    
    //Close everything
    closeCache();
    
    //Restart colocated with "region2"
    IgnoredException ex = IgnoredException.addIgnoredException("DiskAccessException|IllegalStateException");
    try {
      createColocatedPRs("region2");
      fail("Should have received an illegal state exception");
    } catch(IllegalStateException expected) {
      //do nothing
    } finally {
      ex.remove();
    }
    
    //Close everything
    closeCache();
    
    //Restart colocated with region1. 
    //Make sure we didn't screw anything up.
    createColocatedPRs("/region1");
    
    //Close everything
    closeCache();
    
    //Restart uncolocated. We don't allow changing
    //from uncolocated to colocated.
    ex = IgnoredException.addIgnoredException("DiskAccessException|IllegalStateException");
    try {
      createColocatedPRs(null);
      fail("Should have received an illegal state exception");
    } catch(IllegalStateException expected) {
      //do nothing
    } finally {
      ex.remove();
    }
    
    //Close everything
    closeCache();
  }

  /**
   * Create three PRs on a VM, named region1, region2, and region3.
   * The colocated with attribute describes which region region3 
   * should be colocated with.
   * 
   * @param colocatedWith
   */
  private void createColocatedPRs(final String colocatedWith) {
    Cache cache = getCache();

    DiskStore ds = cache.findDiskStore("disk");
    if(ds == null) {
      ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
    }
    AttributesFactory af = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(0);
    af.setPartitionAttributes(paf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    af.setDiskStoreName("disk");
    cache.createRegion("region1", af.create());

    cache.createRegion("region2", af.create());

    if(colocatedWith != null) {
      paf.setColocatedWith(colocatedWith);
    }
    af.setPartitionAttributes(paf.create());
    cache.createRegion("region3", af.create());
  }
  
  /**
   * Test for bug 43570. Rebalance a persistent parent PR before we recover
   * the persistent child PR from disk.
   * @throws Throwable
   */
  public void rebalanceWithOfflineChildRegion(SerializableRunnable createParentPR, SerializableRunnable createChildPR) throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    

    //Create the PRs on two members
    vm0.invoke(createParentPR);
    vm1.invoke(createParentPR);
    vm0.invoke(createChildPR);
    vm1.invoke(createChildPR);
    
    //Create some buckets.
    createData(vm0, 0, NUM_BUCKETS, "a");
    createData(vm0, 0, NUM_BUCKETS, "a", "region2");
    
    //Close the members
    closeCache(vm1);
    closeCache(vm0);
    
    //Recreate the parent region. Try to make sure that
    //the member with the latest copy of the buckets
    //is the one that decides to throw away it's copy
    //by starting it last.
    AsyncInvocation async0 = vm0.invokeAsync(createParentPR);
    AsyncInvocation async1 = vm1.invokeAsync(createParentPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    

    //Now create the parent region on vm-2. vm-2 did not
    //previous host the child region.
    vm2.invoke(createParentPR);
    
    //Rebalance the parent region.
    //This should not move any buckets, because
    //we haven't recovered the child region
    RebalanceResults rebalanceResults = rebalance(vm2);
    assertEquals(0, rebalanceResults.getTotalBucketTransfersCompleted());
    
    //Recreate the child region. 
    async1 = vm1.invokeAsync(createChildPR);
    async0 = vm0.invokeAsync(createChildPR);
    AsyncInvocation async2 = vm2.invokeAsync(createChildPR);
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);
    
    //Validate the data
    checkData(vm0, 0, NUM_BUCKETS, "a");
    checkData(vm0, 0, NUM_BUCKETS, "a", "region2");
    
    //Make sure we can actually use the buckets in the child region.
    createData(vm0, 0, NUM_BUCKETS, "c", "region2");
  }

  private RebalanceResults rebalance(VM vm) {
    return (RebalanceResults) vm.invoke(new SerializableCallable() {
      
      public Object call() throws Exception {
        RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();
        return op.getResults();
      }
    });
  }
  
  private static class PRObserver extends PartitionedRegionObserverAdapter {
    private CountDownLatch rebalanceDone = new CountDownLatch(1);
    private CountDownLatch bucketCreateStarted  = new CountDownLatch(3);
    
    @Override
    public void beforeBucketCreation(PartitionedRegion region, int bucketId) {
      if(region.getName().contains("region2")) {
        bucketCreateStarted.countDown();
        waitForRebalance();
      }
    }



    private void waitForRebalance() {
      try {
        if(!rebalanceDone.await(MAX_WAIT, TimeUnit.SECONDS)) {
          fail("Failed waiting for the rebalance to start");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    
    public void waitForCreate() throws InterruptedException {
      if(!bucketCreateStarted.await(MAX_WAIT, TimeUnit.SECONDS)) {
        fail("Failed waiting for bucket creation to start");
      }
    }

    public void release() {
      rebalanceDone.countDown();
    }
  }

}
