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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllRequest;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PutAllPartialResultException;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserver;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests the basic use cases for PR persistence.
 * @author xzhou
 *
 */
public class ShutdownAllDUnitTest extends CacheTestCase {
  protected static HangingCacheListener listener;


  /**
   * @param name
   */
  
  final String expectedExceptions = InternalGemFireError.class.getName()+"||ShutdownAllRequest: disconnect distributed without response";

  public ShutdownAllDUnitTest(String name) {
    super(name);
  }
  /**
   * 
   */
  private static final int MAX_WAIT = 600 * 1000;
  
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    //Get rid of any existing distributed systems. We want
    //to make assertions about the number of distributed systems
    //we shut down, so we need to start with a clean slate.
    DistributedTestCase.disconnectAllFromDS();
  }

  public void testShutdownAll2Servers() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int numBuckets = 50;

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    createData(vm0, 0, numBuckets, "a", "region");
    
    Set<Integer> vm0Buckets = getBucketList(vm0, "region");
    Set<Integer> vm1Buckets = getBucketList(vm1, "region");
    assertEquals(vm0Buckets, vm1Buckets);
    
    shutDownAllMembers(vm2, 2);
    
    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());
    
    // restart vm0
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);

    // restart vm1
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);

    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);
    
    assertEquals(vm0Buckets, getBucketList(vm0, "region"));

//    checkRecoveredFromDisk(vm0, 0, true);
//    checkRecoveredFromDisk(vm1, 0, false);

    checkData(vm0, 0, numBuckets, "a", "region");
    checkData(vm1, 0, numBuckets, "a", "region");
    
    createData(vm0, numBuckets, 113, "b", "region");
    checkData(vm0, numBuckets, 113, "b", "region");
  }

  public void testShutdownAllWithEncounterIGE1() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    int numBuckets = 50;
    createRegion(vm0, "region", "disk", true, 1);
    createData(vm0, 0, numBuckets, "a", "region");

    vm0.invoke(addExceptionTag1(expectedExceptions));
    invokeInEveryVM(new SerializableRunnable("set TestInternalGemFireError") {
      public void run() {
        System.setProperty("TestInternalGemFireError", "true");
      }
    });
    shutDownAllMembers(vm0, 1);
    
    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());
    
    invokeInEveryVM(new SerializableRunnable("reset TestInternalGemFireError") {
      public void run() {
        System.setProperty("TestInternalGemFireError", "false");
      }
    });
    vm0.invoke(removeExceptionTag1(expectedExceptions));
  }

  public void testShutdownAllWithEncounterIGE2() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int numBuckets = 50;

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    createData(vm0, 0, numBuckets, "a", "region");
    
    Set<Integer> vm0Buckets = getBucketList(vm0, "region");
    Set<Integer> vm1Buckets = getBucketList(vm1, "region");
    assertEquals(vm0Buckets, vm1Buckets);
    
    vm0.invoke(addExceptionTag1(expectedExceptions));
    vm1.invoke(addExceptionTag1(expectedExceptions));
    invokeInEveryVM(new SerializableRunnable("set TestInternalGemFireError") {
      public void run() {
        System.setProperty("TestInternalGemFireError", "true");
      }
    });
    shutDownAllMembers(vm2, 0);
    
    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());
    
    invokeInEveryVM(new SerializableRunnable("reset TestInternalGemFireError") {
      public void run() {
        System.setProperty("TestInternalGemFireError", "false");
      }
    });
    vm0.invoke(removeExceptionTag1(expectedExceptions));
    vm1.invoke(removeExceptionTag1(expectedExceptions));
  }

  public void testShutdownAllOneServerAndRecover() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);
    
    createRegion(vm0, "region", "disk", true, 0);
    
    createData(vm0, 0, 1, "a", "region");
    
    shutDownAllMembers(vm2, 1);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());
    
    // restart vm0
    createRegion(vm0, "region", "disk", true, 0);
    
    checkPRRecoveredFromDisk(vm0, "region", 0, true);

    createData(vm0, 1, 10, "b", "region");
  }

  public void testPRWithDR() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);
    
    createRegion(vm0, "region_pr", "disk", true, 0);
    createRegion(vm0, "region_dr", "disk", false, 0);
    
    createData(vm0, 0, 1, "a", "region_pr");
    createData(vm0, 0, 1, "c", "region_dr");
    
    shutDownAllMembers(vm2, 1);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());
    
    // restart vm0
    createRegion(vm0, "region_pr", "disk", true, 0);
    createRegion(vm0, "region_dr", "disk", false, 0);
    
    checkPRRecoveredFromDisk(vm0, "region_pr", 0, true);

    checkData(vm0, 0, 1, "a", "region_pr");
    checkData(vm0, 0, 1, "c", "region_dr");
  }
  
  public void testShutdownAllFromServer() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int numBuckets = 50;
    
    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);
    createRegion(vm2, "region", "disk", true, 1);
    
    createData(vm0, 0, numBuckets, "a", "region");
    
    shutDownAllMembers(vm2, 3);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());
    
    // restart vm0, vm1, vm2
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);

    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);

    AsyncInvocation a2 = createRegionAsync(vm2, "region", "disk", true, 1);

    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);
    a2.getResult(MAX_WAIT);
    
    createData(vm0, 0, numBuckets, "a", "region");
    createData(vm1, 0, numBuckets, "a", "region");
    createData(vm2, 0, numBuckets, "a", "region");
    
    createData(vm0, numBuckets, 113, "b", "region");
    checkData(vm0, numBuckets, 113, "b", "region");
  }
  
  // shutdownAll, then restart to verify
  public void testCleanStop() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);
    
    createData(vm0, 0, 1, "a", "region");
    
    shutDownAllMembers(vm2, 2);
    
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);
    //[dsmith] Make sure that vm0 is waiting for vm1 to recover
    //If VM(0) recovers early, that is a problem, because we 
    //are no longer doing a clean recovery.
    Thread.sleep(500);
    assertTrue(a0.isAlive());
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);
    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);
    
    checkData(vm0, 0, 1, "a", "region");
    checkData(vm1, 0, 1, "a", "region");
    
    checkPRRecoveredFromDisk(vm0, "region", 0, true);
    checkPRRecoveredFromDisk(vm1, "region", 0, true);
    
    closeRegion(vm0, "region");
    closeRegion(vm1, "region");
    
    a0 = createRegionAsync(vm0, "region", "disk", true, 1);
    a1 = createRegionAsync(vm1, "region", "disk", true, 1);
    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);

    checkData(vm0, 0, 1, "a", "region");
    checkData(vm1, 0, 1, "a", "region");

    checkPRRecoveredFromDisk(vm0, "region", 0, false);
    checkPRRecoveredFromDisk(vm1, "region", 0, true);
  }

  // shutdownAll, then restart to verify
  public void testCleanStopWithConflictCachePort() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);
    
    // 2 vms use the same port no to conflict
    addCacheServer(vm0, 34505);
    addCacheServer(vm1, 34505);
    
    createData(vm0, 0, 1, "a", "region");
    
    shutDownAllMembers(vm2, 2);
    
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);
    //[dsmith] Make sure that vm0 is waiting for vm1 to recover
    //If VM(0) recovers early, that is a problem, because we 
    //are no longer doing a clean recovery.
    Thread.sleep(500);
    assertTrue(a0.isAlive());
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);
    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);
    
    addCacheServer(vm0, 34505);
    addCacheServer(vm1, 34506);
    
    checkData(vm0, 0, 1, "a", "region");
    checkData(vm1, 0, 1, "a", "region");
    
    checkPRRecoveredFromDisk(vm0, "region", 0, true);
    checkPRRecoveredFromDisk(vm1, "region", 0, true);
  }
  
  /*
  public void testStopNonPersistRegions() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createRegion(vm0, "region", null, true, 1);
    createRegion(vm1, "region", "disk", true, 1);
    
    createData(vm0, 0, 1, "a", "region");
    
    shutDownAllMembers(vm2, 2);

    // restart vms, and let vm0 to do GII from vm0
    createRegion(vm1, "region", "disk", true, 1);
    createRegion(vm0, "region", null, true, 1);
    
    checkData(vm0, 0, 1, "a", "region");
    checkData(vm1, 0, 1, "a", "region");
    
    checkPRRecoveredFromDisk(vm1, "region", 0, true);
    checkPRRecoveredFromDisk(vm0, "region", 0, false);
  }
  */

  public void testMultiPRDR() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);
    
    createRegion(vm0, "region_pr1", "disk1", true, 0);
    createRegion(vm0, "region_pr2", "disk1", true, 0);
    createRegion(vm0, "region_pr3", "disk1", true, 0);
    createRegion(vm0, "region_dr1", "disk2", false, 0);
    createRegion(vm0, "region_dr2", "disk2", false, 0);
    
    createData(vm0, 0, 1, "a", "region_pr1");
    createData(vm0, 0, 1, "b", "region_pr2");
    createData(vm0, 0, 1, "c", "region_pr3");
    createData(vm0, 0, 1, "d", "region_dr1");
    createData(vm0, 0, 1, "e", "region_dr2");
    
    shutDownAllMembers(vm2, 1);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());
    
    // restart vm0
    createRegion(vm0, "region_pr1", "disk1", true, 0);
    createRegion(vm0, "region_pr2", "disk1", true, 0);
    createRegion(vm0, "region_pr3", "disk1", true, 0);
    createRegion(vm0, "region_dr1", "disk2", false, 0);
    createRegion(vm0, "region_dr2", "disk2", false, 0);
    
//    checkPRRecoveredFromDisk(vm0, "region_pr1", 0, true);
//    checkPRRecoveredFromDisk(vm0, "region_pr2", 0, true);
//    checkPRRecoveredFromDisk(vm0, "region_pr3", 0, true);

    checkData(vm0, 0, 1, "a", "region_pr1");
    checkData(vm0, 0, 1, "b", "region_pr2");
    checkData(vm0, 0, 1, "c", "region_pr3");
    checkData(vm0, 0, 1, "d", "region_dr1");
    checkData(vm0, 0, 1, "e", "region_dr2");
  }


  public void testShutdownAllTimeout() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    final int numBuckets = 50;

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    createData(vm0, 0, numBuckets, "a", "region");
    
    Set<Integer> vm0Buckets = getBucketList(vm0, "region");
    Set<Integer> vm1Buckets = getBucketList(vm1, "region");
    assertEquals(vm0Buckets, vm1Buckets);
    
    //Add a cache listener that will cause the system to hang up.
    //Then do some puts to get us stuck in a put.
    AsyncInvocation async1 = vm0.invokeAsync(new SerializableRunnable() {
      public void run() {
        Region<Object, Object> region = getCache().getRegion("region");
        listener = new HangingCacheListener();
        region.getAttributesMutator().addCacheListener(listener);
        
        //get us stuck doing a put.
        for(int i=0; i < numBuckets; i++) {
          region.put(i, "a");
        }
      }
    });
    
    //Make sure the we do get stuck
    async1.join(1000);
    assertTrue(async1.isAlive());
    
    
    //Do a shutdownall with a timeout.
    //This will hit the timeout, because the in progress put will
    //prevent us from gracefully shutting down.
    long start = System.nanoTime();
    shutDownAllMembers(vm2, 0, 2000);
    long end = System.nanoTime();
    
    //Make sure we waited for the timeout.
    assertTrue(end - start > TimeUnit.MILLISECONDS.toNanos(1500));
    
    
    //clean up our stuck thread
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        listener.latch.countDown();
        listener = null;
      }
    });
    
    //wait for shutdown to finish
    pause(10000);
    
    // restart vm0
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);

    // restart vm1
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);

    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);
    
    assertEquals(vm0Buckets, getBucketList(vm0, "region"));

//    checkRecoveredFromDisk(vm0, 0, true);
//    checkRecoveredFromDisk(vm1, 0, false);

    checkData(vm0, 0, numBuckets, "a", "region");
    checkData(vm1, 0, numBuckets, "a", "region");
    
    createData(vm0, numBuckets, 113, "b", "region");
    checkData(vm0, numBuckets, 113, "b", "region");
  }
  
  
//  public void testRepeat() throws Throwable {
//    for (int i=0; i<10; i++) {
//      System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> run #"+i);
//      testShutdownAllWithMembersWaiting();
//      tearDown();
//      setUp();
//    }
//  }
  
  /**
   * Test for 43551. Do a shutdown all with some
   * members waiting on recovery.
   * @throws Throwable
   */
  public void testShutdownAllWithMembersWaiting() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    final int numBuckets = 5;

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    createData(vm0, 0, numBuckets, "a", "region");
    
    Set<Integer> vm0Buckets = getBucketList(vm0, "region");
    Set<Integer> vm1Buckets = getBucketList(vm1, "region");
    
    
    //shutdown all the members
    shutDownAllMembers(vm2, 2);
    
    //restart one of the members (this will hang, waiting for the other members)
    // restart vm0
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);
    
    //Wait a bit for the initialization to get stuck
    pause(20000);
    assertTrue(a0.isAlive());
    
    //Do another shutdown all, with a member offline and another stuck
    shutDownAllMembers(vm2, 1);
    
    //This should complete (but maybe it will throw an exception?)
    try {
      a0.getResult(MAX_WAIT);
      fail("should have received a cache closed exception");
    } catch(Exception e) {
      if(!(e.getCause() instanceof RMIException)) {
        throw e;
      }
      RMIException cause = (RMIException) e.getCause();
      if(!(cause.getCause() instanceof CacheClosedException)) {
        throw e;
      }
    }

    //now restart both members. This should work, but
    //no guarantee they'll do a clean recovery
    a0 = createRegionAsync(vm0, "region", "disk", true, 1);
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);

    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);
    
    assertEquals(vm0Buckets, getBucketList(vm0, "region"));

    checkData(vm0, 0, numBuckets, "a", "region");
    checkData(vm1, 0, numBuckets, "a", "region");
    
    createData(vm0, numBuckets, numBuckets * 2, "b", "region");
    checkData(vm0, numBuckets, numBuckets * 2, "b", "region");
  }

  //TODO prpersist
  // test move bucket
  // test async put
  // test create a new bucket by put async
  

  private void shutDownAllMembers(VM vm, final int expnum) {
    vm.invoke(new SerializableRunnable("Shutdown all the members") {

      public void run() {
        DistributedSystemConfig config;
        AdminDistributedSystemImpl adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = (AdminDistributedSystemImpl)AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          Set members = adminDS.shutDownAllMembers();
          int num = members==null?0:members.size();
          assertEquals(expnum, num);
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });

    // clean up for this vm
    System.setProperty("TestInternalGemFireError", "false");
  }
  
  private SerializableRunnable getCreateDRRunnable(final String regionName, final String diskStoreName) {
    SerializableRunnable createDR = new SerializableRunnable("create DR") {
      Cache cache;
      
      public void run() {
        cache = getCache();
        
        DiskStore ds = cache.findDiskStore(diskStoreName);
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create(diskStoreName);
        }
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        
        af.setDiskStoreName(diskStoreName);
        cache.createRegion(regionName, af.create());
      }
    };
    return createDR;
  }
  
  protected void addCacheServer(VM vm, final int port) {
    vm.invoke(new SerializableRunnable("add Cache Server") {
      public void run() { 
        Cache cache = getCache();
        CacheServer cs = cache.addCacheServer();
        cs.setPort(port);
        try {
          cs.start();
        } catch (IOException e) {
          System.out.println("Received expected "+e.getMessage());
        }
      }
    });
  }
  
  protected void createRegion(VM vm, final String regionName, final String diskStoreName, final boolean isPR, final int redundancy) {
    if (isPR) {
      SerializableRunnable createPR = getCreatePRRunnable(regionName, diskStoreName, redundancy);
      vm.invoke(createPR);
    } else {
      SerializableRunnable createPR = getCreateDRRunnable(regionName, diskStoreName);
      vm.invoke(createPR);
    }
  }

  protected AsyncInvocation createRegionAsync(VM vm, final String regionName, final String diskStoreName, final boolean isPR, final int redundancy) {
    if (isPR) {
      SerializableRunnable createPR = getCreatePRRunnable(regionName, diskStoreName, redundancy);
      return vm.invokeAsync(createPR);
    } else {
      SerializableRunnable createDR = getCreateDRRunnable(regionName, diskStoreName);
      return vm.invokeAsync(createDR);
    }
  }

  private SerializableRunnable getCreatePRRunnable(final String regionName, final String diskStoreName, final int redundancy) {
    SerializableRunnable createPR = new SerializableRunnable("create pr") {
      
      public void run() {
        final CountDownLatch recoveryDone;
        if(redundancy > 0) {
          recoveryDone = new CountDownLatch(1);
        
          ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
            @Override
            public void recoveryFinished(Region region) {
              recoveryDone.countDown();
            }
          };
          InternalResourceManager.setResourceObserver(observer );
        } else {
          recoveryDone = null;
        }
        
        Cache cache = getCache();
  
        if (diskStoreName!=null) {
          DiskStore ds = cache.findDiskStore(diskStoreName);
          if(ds == null) {
            ds = cache.createDiskStoreFactory()
            .setDiskDirs(getDiskDirs()).create(diskStoreName);
          }
        }
        AttributesFactory af = new AttributesFactory();
        af.setDiskSynchronous(false); // use async to trigger flush
        af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy);
        af.setPartitionAttributes(paf.create());
        if (diskStoreName != null) {
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName(diskStoreName);
        } else {
          af.setDataPolicy(DataPolicy.PARTITION);
        }
        cache.createRegion(regionName, af.create());
        if(recoveryDone != null) {
          try {
            recoveryDone.await();
          } catch (InterruptedException e) {
            fail("Interrupted", e);
          }
        }
      }
    };
    return createPR;
  }
  
  protected void createData(VM vm, final int startKey, final int endKey,
      final String value, final String regionName) {
    SerializableRunnable createData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        
        for(int i =startKey; i < endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }
  
  protected Set<Integer> getBucketList(VM vm, final String regionName) {
    SerializableCallable getBuckets = new SerializableCallable("get buckets") {
      
      public Object call() throws Exception {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        if (region instanceof PartitionedRegion) {
          PartitionedRegion pr = (PartitionedRegion)region;
          return new TreeSet<Integer>(pr.getDataStore().getAllLocalBucketIds());
        } else {
          return null;
        }
      }
    };
    
    return (Set<Integer>) vm.invoke(getBuckets);
  }
  
  protected void checkData(VM vm, final int startKey, final int endKey,
      final String value, final String regionName) {
    SerializableRunnable checkData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        
        for(int i =startKey; i < endKey; i++) {
          assertEquals(value, region.get(i));
        }
      }
    };
    
    vm.invoke(checkData);
  }
  
  protected void checkPRRecoveredFromDisk(VM vm, final String regionName, final int bucketId, final boolean recoveredLocally) {
    vm.invoke(new SerializableRunnable("check PR recovered from disk") {
      public void run() { 
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        DiskRegion disk = region.getRegionAdvisor().getBucket(bucketId).getDiskRegion();
        if(recoveredLocally) {
          assertEquals(0, disk.getStats().getRemoteInitializations());
          assertEquals(1, disk.getStats().getLocalInitializations());
        } else {
          assertEquals(1, disk.getStats().getRemoteInitializations());
          assertEquals(0, disk.getStats().getLocalInitializations());
        }
      }
    });
  }
  
  protected void closeRegion(VM vm, final String regionName) {
    SerializableRunnable close = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        region.close();
      }
    };
    
    vm.invoke(close);
  }

  private void shutDownAllMembers(VM vm, final int expnum, final long timeout) {
    vm.invoke(new SerializableRunnable("Shutdown all the members") {

      public void run() {
        DistributedSystemConfig config;
        AdminDistributedSystemImpl adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = (AdminDistributedSystemImpl)AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          Set members = adminDS.shutDownAllMembers(timeout); 
          int num = members==null?0:members.size();
          assertEquals(expnum, num);
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }
  
  private static class HangingCacheListener extends CacheListenerAdapter {
    CountDownLatch latch = new CountDownLatch(1);
    @Override
    public void afterUpdate(EntryEvent event) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
