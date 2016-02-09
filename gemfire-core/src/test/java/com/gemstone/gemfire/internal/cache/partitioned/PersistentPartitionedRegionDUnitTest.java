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

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.Ignore;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.persistence.ConflictingPersistentDataException;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;
import com.gemstone.gemfire.cache.persistence.RevokeFailedException;
import com.gemstone.gemfire.cache.persistence.RevokedPersistentDataException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.RequestImageMessage;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.partitioned.ManageBucketMessage.ManageBucketReplyMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * Tests the basic use cases for PR persistence.
 * @author dsmith
 *
 */
public class PersistentPartitionedRegionDUnitTest extends PersistentPartitionedRegionTestBase {
  private static final int NUM_BUCKETS = 15;
  //This must be bigger than the dunit ack-wait-threshold for the revoke
  //tests. The command line is setting the ack-wait-threshold to be 
  //60 seconds.
  private static final int MAX_WAIT = 65 * 1000;
  
  public PersistentPartitionedRegionDUnitTest(String name) {
    super(name);
  }
  
  /**
   * A simple test case that we are actually
   * persisting with a PR.
   */
  public void testSinglePR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPR(vm0, 0);
    
    createData(vm0, 0, 1, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    
//    closePR(vm0);
    closeCache(vm0);
    
    createPR(vm0, 0);

    assertEquals(vm0Buckets,getBucketList(vm0));
    
    checkData(vm0, 0, 1, "a");
    
    localDestroyPR(vm0);
    
    closeCache(vm0);
    
    createPR(vm0, 0);
    
    //Make sure the data is now missing
    checkData(vm0, 0, 1, null);
  }

  /**
   * Test total-buckets-num getting bigger, which cause exception.
   * but changed to smaller should be ok.
   */
  public void testChangedToalBucketNumberSinglePR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    createPR(vm0, 0, 0, 5);
    createData(vm0, 0, 5, "a");
    closeCache(vm0);
    IgnoredException expect = IgnoredException.addIgnoredException("IllegalStateException", vm0);
    expect = IgnoredException.addIgnoredException("DiskAccessException", vm0);
    try {
      createPR(vm0, 0, 0, 2);
      fail("Expect to get java.lang.IllegalStateException, but it did not");
    } catch (RMIException exp) {
      assertTrue(exp.getCause() instanceof IllegalStateException);
      IllegalStateException ise = (IllegalStateException)exp.getCause();
      Object[] prms = new Object[] { "/"+PR_REGION_NAME, 2, 5 };
      assertTrue(ise.getMessage().contains(LocalizedStrings.PartitionedRegion_FOR_REGION_0_TotalBucketNum_1_SHOULD_NOT_BE_CHANGED_Previous_Configured_2.toString(prms)));
    }
    closeCache(vm0);
    try {
      createPR(vm0, 0, 0, 10);
      fail("Expect to get java.lang.IllegalStateException, but it did not");
    } catch (RMIException exp) {
      assertTrue(exp.getCause() instanceof IllegalStateException);
      IllegalStateException ise = (IllegalStateException)exp.getCause();
      Object[] prms = new Object[] { "/"+PR_REGION_NAME, 10, 5 };
      assertTrue(ise.getMessage().contains(LocalizedStrings.PartitionedRegion_FOR_REGION_0_TotalBucketNum_1_SHOULD_NOT_BE_CHANGED_Previous_Configured_2.toString(prms)));
    }
    expect.remove();
  }

  /**
   * Test for bug 44184
   */
  public void testSinglePRWithCustomExpiry() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(1);
    
    
    SerializableRunnable createPR = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        af.setPartitionAttributes(paf.create());
        af.setCustomEntryIdleTimeout(new TestCustomExpiration());
        af.setEntryIdleTimeout(new ExpirationAttributes(60, ExpirationAction.INVALIDATE));
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        RegionAttributes attr = af.create();
        cache.createRegion(PR_REGION_NAME, attr);
      }
    };
    
    vm0.invoke(createPR);
    
    createData(vm0, 0, 1, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    
//    closePR(vm0);
    closeCache(vm0);
    
    vm0.invoke(createPR);

    assertEquals(vm0Buckets,getBucketList(vm0));
    
    checkData(vm0, 0, 1, "a");
  }
  
  /**
   * Test to make sure that we can recover
   * from a complete system shutdown with redundancy
   * 0
   * @throws Throwable 
   */
  public void testTotalRecoverRedundancy0() throws Throwable {
    totalRecoverTest(0);
  }
  
  /**
   * Test to make sure that we can recover
   * from a complete system shutdown with redundancy
   * 1
   * @throws Throwable 
   */
  public void testTotalRecoverRedundancy1() throws Throwable {
    totalRecoverTest(1);
  }
  
  
  private static boolean FAIL_IN_THIS_VM = false;
  /**
   * Test for bug #49972 - handle a serialization error in the
   * async writer thread.
   */
  @Ignore("Bug 50376")
  public void DISABLED_testBadSerializationInAsyncThread() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    final int numBuckets = 50;
    
    vm0.invoke(new SerializableRunnable() {
      
      @Override
      public void run() {
        FAIL_IN_THIS_VM=true;
      }
    });

    IgnoredException expected1 = IgnoredException.addIgnoredException("Fatal error from asynch");
    IgnoredException expected2 = IgnoredException.addIgnoredException("ToDataException");
    try {
      int redundancy=1;
      createPR(vm0, redundancy, -1, 113, false);
      createPR(vm2, redundancy, -1, 113, false);
      //Trigger bucket creation
      createData(vm0, 0, numBuckets, "a");
      createPR(vm1, redundancy, -1, 113, false);

      //write objects which will fail serialization in async writer thread.

      vm0.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion(PR_REGION_NAME);
          try {
          for(int i=0;i<numBuckets; i++) {
            region.put(i, new BadSerializer());
            //this will trigger a deserialiation (could have also done this put with a function I guess.
            region.get(i);
          }
          } catch (DiskAccessException ex) {
            if (ex.getMessage().contains("the flusher thread had been terminated")) {
              // expected
            } else {
              throw ex;
            }
          }
        }
      });

      //Wait for the thread to get hosed.

      Thread.sleep(2000);

      createData(vm1, 0, numBuckets, "b");
      //Try to do puts from vm1, which doesn't have any buckets
      createData(vm1, numBuckets, numBuckets * 2, "b");
      createData(vm1, numBuckets, numBuckets * 2, "c");

      //make sure everything has settle out (these VM's I suppose may be terminated)
      checkData(vm2, 0, numBuckets, "b");
      checkData(vm2, numBuckets, numBuckets * 2, "c");
    }finally {
      expected1.remove();
      expected2.remove();
    }
  }
  
  public static class BadSerializer implements DataSerializable {

    public BadSerializer() {
      
    }
    
    public void toData(DataOutput out) throws IOException {
      
      if(Thread.currentThread().getName().contains("Asynchronous disk writer") && FAIL_IN_THIS_VM) {
        throw new ConcurrentModificationException();
      }
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      
      
    }
    
    
  }

  public void totalRecoverTest(int redundancy) throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int numBuckets = 50;
    
    createPR(vm0, redundancy);
    createPR(vm1, redundancy);
    createPR(vm2, redundancy);
    
    createData(vm0, 0, numBuckets, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    Set<Integer> vm2Buckets = getBucketList(vm2);
    
    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);
    
    AsyncInvocation a1 = createPRAsync(vm0, redundancy);
    AsyncInvocation a2 = createPRAsync(vm1, redundancy);
    AsyncInvocation a3 = createPRAsync(vm2, redundancy);
    
    a1.getResult(MAX_WAIT);
    a2.getResult(MAX_WAIT);
    a3.getResult(MAX_WAIT);
    
    assertEquals(vm0Buckets,getBucketList(vm0));
    assertEquals(vm1Buckets,getBucketList(vm1));
    assertEquals(vm2Buckets,getBucketList(vm2));
    
    checkData(vm0, 0, numBuckets, "a");
    createData(vm0, numBuckets, 113, "b");
    checkData(vm0, numBuckets, 113, "b");
    

    //Test for bug 43476 - make sure a destroy
    //cleans up proxy bucket regions.
    destroyPR(vm0);
    destroyPR(vm1);
    destroyPR(vm2);
    
    a1 = createPRAsync(vm0, redundancy);
    a2 = createPRAsync(vm1, redundancy);
    a3 = createPRAsync(vm2, redundancy);
    
    a1.getResult(MAX_WAIT);
    a2.getResult(MAX_WAIT);
    a3.getResult(MAX_WAIT);
    
    checkData(vm0, 0, numBuckets, null);
  }
  
  public void testRevokeAfterStartup() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int numBuckets = 50;
    
    createPR(vm0, 1);
    createPR(vm1, 1);
    
    createData(vm0, 0, numBuckets, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    assertEquals(vm0Buckets, vm1Buckets);
    
    
    closeCache(vm0);
    createData(vm1, 0, numBuckets, "b");
    
    closeCache(vm1);
    
    AsyncInvocation a1 = createPRAsync(vm0, 1);
    //[dsmith] Make sure that vm0 is waiting for vm1 to recover
    //If VM(0) recovers early, that is a problem, because vm1 
    //has newer data
    Thread.sleep(500);
    assertTrue(a1.isAlive());

    revokeKnownMissingMembers(vm2, 1);
    
    a1.getResult(MAX_WAIT);
    
    assertEquals(vm0Buckets,getBucketList(vm0));
    
    checkData(vm0, 0, numBuckets, "a");
    createData(vm0, numBuckets, 113, "b");
    checkData(vm0, numBuckets, 113, "b");
    
    IgnoredException ex = IgnoredException.addIgnoredException(RevokedPersistentDataException.class.getName(), vm1);
    try {
      createPR(vm1, 1);
      fail("Should have recieved a SplitDistributedSystemException");
    } catch(RMIException e) {
      //This should throw a split distributed system exception, because
      //We revoked this member.
      if(!(e.getCause() instanceof RevokedPersistentDataException)) {
        throw e;
      }
    }
    ex.remove();
  }
  
  public void testRevokeBeforeStartup() throws Throwable {
    IgnoredException.addIgnoredException("RevokeFailedException");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int numBuckets = 50;
    
    createPR(vm0, 1);
    createPR(vm1, 1);
    
    createData(vm0, 0, numBuckets, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    assertEquals(vm0Buckets, vm1Buckets);
    
    //This should fail with a revocation failed message
    try {
      revokeAllMembers(vm2);
      fail("The revoke should have failed, because members are running");
    } catch(RMIException e) {
      if(!(e.getCause() instanceof ReplyException && e.getCause().getCause() instanceof RevokeFailedException)) {
        throw e;
      }
    }
    
    
    closeCache(vm0);
    createData(vm1, 0, numBuckets, "b");
    
    File vm1Directory = getDiskDirectory(vm1);
    closeCache(vm1);
    
    
    vm0.invoke(new SerializableRunnable("get cache") {
      
      public void run() {
        getCache();
      }
    });
    
    revokeMember(vm2, vm1Directory);
    
    AsyncInvocation a1 = createPRAsync(vm0, 1);
    
    a1.getResult(MAX_WAIT);
    
    assertEquals(vm0Buckets,getBucketList(vm0));
    
    checkData(vm0, 0, numBuckets, "a");
    createData(vm0, numBuckets, 113, "b");
    checkData(vm0, numBuckets, 113, "b");
    
    IgnoredException ex = IgnoredException.addIgnoredException(RevokedPersistentDataException.class.getName(), vm1);
    try {
      createPR(vm1, 1);
      fail("Should have recieved a SplitDistributedSystemException");
    } catch(RMIException e) {
      //This should throw a split distributed system exception, because
      //We revoked this member.
      if(!(e.getCause() instanceof RevokedPersistentDataException)) {
        throw e;
      }
    }
    ex.remove();
  }

  private File getDiskDirectory(VM vm0) {
    return (File) vm0.invoke(new SerializableCallable() {
      
      @Override
      public Object call() throws Exception {
        return getDiskDirs()[0];
      }
    });
  }
  
  /**
   * Test that we wait for missing data to come back
   * if the redundancy was 0.
   */
  public void testMissingMemberRedundancy0() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createPR(vm0, 0);
    createPR(vm1, 0);
    
    createData(vm0, 0, NUM_BUCKETS, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    
    final int aVM0Bucket = vm0Buckets.iterator().next();
    final int aVM1Bucket = vm1Buckets.iterator().next();
    closeCache(vm1);

    IgnoredException ex = IgnoredException.addIgnoredException("PartitionOfflineException");
    try { 
      checkReadWriteOperationsWithOfflineMember(vm0, aVM0Bucket, aVM1Bucket);
      //Make sure that a newly created member is informed about the offline member
      createPR(vm2,0);
      checkReadWriteOperationsWithOfflineMember(vm2, aVM0Bucket, aVM1Bucket);
    } finally {
      ex.remove();
    }
    
    //This should work, because these are new buckets
    createData(vm0, NUM_BUCKETS, 113, "a");
    
    createPR(vm1, 0);

    //The data should be back online now.
    checkData(vm0, 0, 113, "a");
    
    closeCache(vm0);
    closeCache(vm1);
  }

  private void checkReadWriteOperationsWithOfflineMember(VM vm0,
      final int aVM0Bucket, final int aVM1Bucket) {
    //This should work, because this bucket is still available.
    checkData(vm0, aVM0Bucket, aVM0Bucket + 1, "a");

    try {
      checkData(vm0, aVM1Bucket, aVM1Bucket + 1, null);
      fail("Should not have been able to read from missing buckets!");
    } catch (RMIException e) {
      //We expect a PartitionOfflineException
      if(!(e.getCause() instanceof PartitionOfflineException)) {
        throw e;
      }
    }

    IgnoredException expect = IgnoredException.addIgnoredException("PartitionOfflineException", vm0);
    //Try a function execution
    vm0.invoke(new SerializableRunnable("Test ways to read") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(PR_REGION_NAME);

        try {
          FunctionService.onRegion(region).execute(new TestFunction());
          fail("Should not have been able to read from missing buckets!");
        } catch (PartitionOfflineException e) {
          //expected
        }

        //This should work, because this bucket is still available.
        FunctionService.onRegion(region).withFilter(Collections.singleton(aVM0Bucket)).execute(new TestFunction());

        //This should fail, because this bucket is offline
        try {
          FunctionService.onRegion(region).withFilter(Collections.singleton(aVM1Bucket)).execute(new TestFunction());
          fail("Should not have been able to read from missing buckets!");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
        //This should fail, because a bucket is offline   
        try {
          HashSet filter = new HashSet();
          filter.add(aVM0Bucket);
          filter.add(aVM1Bucket);
          FunctionService.onRegion(region).withFilter(filter).execute(new TestFunction());
          fail("Should not have been able to read from missing buckets!");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
        //This should fail, because a bucket is offline
        try {
          FunctionService.onRegion(region).execute(new TestFunction());
          fail("Should not have been able to read from missing buckets!");
        } catch (PartitionOfflineException e) {
          //expected
        }

        try {
          cache.getQueryService().newQuery("select * from /"+PR_REGION_NAME).execute();
          fail("Should not have been able to read from missing buckets!");
        } catch (PartitionOfflineException e) {
          //expected
        } catch (QueryException e) {
          throw new RuntimeException(e);
        }
        
        try {
          Set keys = region.keySet();
          //iterate over all of the keys
          for(Object key : keys) {
          }
          fail("Should not have been able to iterate over keyset");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
        try {
          //iterate over all of the keys
          for(Object key : region.values()) {
          }
          fail("Should not have been able to iterate over set");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
        try {
          //iterate over all of the keys
          for(Object key : region.entrySet()) {
          }
          fail("Should not have been able to iterate over set");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
        try {
          region.get(aVM1Bucket);
          fail("Should not have been able to get an offline key");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
        try {
          region.containsKey(aVM1Bucket);
          fail("Should not have been able to get an offline key");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
        try {
          region.getEntry(aVM1Bucket);
          fail("Should not have been able to get an offline key");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
        try {
          region.invalidate(aVM1Bucket);
          fail("Should not have been able to get an offline key");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
        try {
          region.destroy(aVM1Bucket);
          fail("Should not have been able to get an offline key");
        } catch (PartitionOfflineException e) {
          //expected
        }
        
      }
    });

    try {
      createData(vm0, aVM1Bucket, aVM1Bucket + 1, "b");
      fail("Should not have been able to write to missing buckets!");
    } catch (RMIException e) {
      //We expect to see a partition offline exception here.
      if(!(e.getCause() instanceof PartitionOfflineException)) {
        throw e;
      }
    }
    expect.remove();
  }
  
  /**Test to make sure that we recreate
  * a bucket if a member is destroyed
  */
  public void testDestroyedMemberRedundancy0() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPR(vm0, 0);
    createPR(vm1, 0);
    
    createData(vm0, 0, NUM_BUCKETS, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    
    int aVM0Bucket = vm0Buckets.iterator().next();
    int aVM1Bucket = vm1Buckets.iterator().next();
    localDestroyPR(vm1);
    
    //This should work, because this bucket is still available.
    checkData(vm0, aVM0Bucket, aVM0Bucket + 1, "a");
    
    //This should find that the data is missing, because we destroyed that bucket
    checkData(vm0, aVM1Bucket, aVM1Bucket + 1, null);
    
    //We should be able to recreate that bucket
    createData(vm0, aVM1Bucket, aVM1Bucket + 1, "b");
    
    createPR(vm1, 0);

    //The data should still be available
    checkData(vm0, aVM0Bucket, aVM0Bucket + 1, "a");
    checkData(vm0, aVM1Bucket, aVM1Bucket + 1, "b");
    
    //This bucket should now be in vm0, because we recreated it there
    assertTrue(getBucketList(vm0).contains(aVM1Bucket));
  }
  
  
  /**Test to make sure that we recreate
   * a bucket if a member is destroyed
   */
   public void testDestroyedMemberRedundancy1() {
     Host host = Host.getHost(0);
     VM vm0 = host.getVM(0);
     VM vm1 = host.getVM(1);
     VM vm2 = host.getVM(2);
     
     createPR(vm0, 1);
     createPR(vm1, 1);
     
     createData(vm0, 0, NUM_BUCKETS, "a");
     
     Set<Integer> vm0Buckets = getBucketList(vm0);
     Set<Integer> vm1Buckets = getBucketList(vm1);
     
     assertEquals(vm0Buckets, vm1Buckets);
     
     int aVM0Bucket = vm0Buckets.iterator().next();
     localDestroyPR(vm1);
     
     //This should work, because this bucket is still available.
     checkData(vm0, aVM0Bucket, aVM0Bucket + 1, "a");
     
     createPR(vm2, 1);
     
     Set<Integer> vm2Buckets = getBucketList(vm2);

     //VM 2 should have created a copy of all of the buckets
     assertEquals(vm0Buckets, vm2Buckets);
   }
   
   
   /**Test to make sure that we recreate
    * a bucket if a member is revoked
    */
    public void testRevokedMemberRedundancy0() {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      VM vm2 = host.getVM(2);
      
      createPR(vm0, 0);
      createPR(vm1, 0);
      
      createData(vm0, 0, NUM_BUCKETS, "a");
      
      Set<Integer> vm0Buckets = getBucketList(vm0);
      Set<Integer> vm1Buckets = getBucketList(vm1);
      
      int aVM0Bucket = vm0Buckets.iterator().next();
      int aVM1Bucket = vm1Buckets.iterator().next();
      closeCache(vm1);
      
      //This should work, because this bucket is still available.
      checkData(vm0, aVM0Bucket, aVM0Bucket + 1, "a");
      
      IgnoredException expect = IgnoredException.addIgnoredException("PartitionOfflineException", vm0);
      try {
        checkData(vm0, aVM1Bucket, aVM1Bucket + 1, "a");
        fail("Should not have been able to read from missing buckets!");
      } catch (RMIException e) {
        if(!(e.getCause() instanceof PartitionOfflineException)) {
          throw e;
        }
      }
      
      try {
        createData(vm0, aVM1Bucket, aVM1Bucket + 1, "b");
        fail("Should not have been able to write to missing buckets!");
      } catch (RMIException e) {
        //We expect to see a partition offline exception here.
        if(!(e.getCause() instanceof PartitionOfflineException)) {
          throw e;
        }
      }
      expect.remove();
      
      //This should work, because these are new buckets
      createData(vm0, NUM_BUCKETS, 113, "a");
      
      revokeKnownMissingMembers(vm2, 1);
      
      createPR(vm2, 0);

      //We should be able to use that missing bucket now
      checkData(vm2, aVM1Bucket, aVM1Bucket + 1, null);
      createData(vm2, aVM1Bucket, aVM1Bucket + 1, "a");
      checkData(vm2, aVM1Bucket, aVM1Bucket + 1, "a");
      
      IgnoredException ex = IgnoredException.addIgnoredException(RevokedPersistentDataException.class.getName(), vm1);
      try {
        createPR(vm1, 0);
        fail("Should have recieved a RevokedPersistentDataException");
      } catch(RMIException e) {
        //This should throw a split distributed system exception, because
        //We revoked this member.
        if(!(e.getCause() instanceof RevokedPersistentDataException)) {
          throw e;
        }
      }
      ex.remove();
    }
    
    /**Test to make sure that we recreate
     * a bucket if a member is revoked
     * @throws Throwable 
     */
    public void testRevokedMemberRedundancy1() throws Throwable {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      VM vm2 = host.getVM(2);
      createPR(vm0, 1);
      createPR(vm1, 1);
      
      createData(vm0, 0, NUM_BUCKETS, "a");

      Set<Integer> vm0Buckets = getBucketList(vm0);
      Set<Integer> vm1Buckets = getBucketList(vm1);
      assertEquals(vm0Buckets, vm1Buckets);

      closeCache(vm1);

      //This should work, because this bucket is still available.
      checkData(vm0, 0, NUM_BUCKETS, "a");
      
      createData(vm0, 0, NUM_BUCKETS, "b");
      
      revokeKnownMissingMembers(vm2, 1);
      
      //This should make a copy of all of the buckets,
      //because we have revoked VM1.
      createPR(vm2, 1);
      
      Set<Integer> vm2Buckets = getBucketList(vm2);
      assertEquals(vm1Buckets, vm2Buckets);
      
      IgnoredException ex = IgnoredException.addIgnoredException(RevokedPersistentDataException.class.getName(), vm1);
      try {
        createPR(vm1, 1);
        fail("Should have recieved a SplitDistributedSystemException");
      } catch(RMIException e) {
        //This should throw a RevokedPersistentDataException exception, because
        //We revoked this member.
        if(!(e.getCause() instanceof RevokedPersistentDataException)) {
          throw e;
        }
      }
      
      
      //Test that we can bounce vm0 and vm1, and still get a RevokedPersistentDataException
      //when vm1 tries to recover
      closeCache(vm0);
      closeCache(vm2);
      AsyncInvocation async0 = createPRAsync(vm0, 1);
      AsyncInvocation async2 = createPRAsync(vm2, 1);
      
      async0.getResult();
      async2.getResult();
      
      try {
        createPR(vm1, 1);
        fail("Should have recieved a RevokedPersistentDataException");
      } catch(RMIException e) {
        //This should throw a RevokedPersistentDataException exception, because
        //We revoked this member.
        if(!(e.getCause() instanceof RevokedPersistentDataException)) {
          throw e;
        }
      }
      
      ex.remove();

      //The data shouldn't be affected.
      checkData(vm2, 0, NUM_BUCKETS, "b");
    }
    
    /**Test to make sure that we recreate
     * a bucket if a member is revoked, and
     * that we do it immediately if recovery delay
     * is set to 0.
     * @throws Throwable 
     */
    public void testRevokedMemberRedundancy1ImmediateRecovery() throws Throwable {
      disconnectAllFromDS(); // I see this test failing because it finds the ds disconnected. Trying this as a fix.
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      final VM vm2 = host.getVM(2);
      createPR(vm0, 1, 0);
      createPR(vm1, 1, 0);
      
      createData(vm0, 0, NUM_BUCKETS, "a");
      
      //This should do nothing because we have satisfied redundancy.
      createPR(vm2, 1, 0);
      assertEquals(Collections.emptySet(), getBucketList(vm2));
      
      Set<Integer> vm0Buckets = getBucketList(vm0);
      final Set<Integer> lostBuckets = getBucketList(vm1);
      
      closeCache(vm1);
      
      //VM2 should pick up the slack
      
      Wait.waitForCriterion(new WaitCriterion() {
        
        public boolean done() {
          Set<Integer> vm2Buckets = getBucketList(vm2);
          return lostBuckets.equals(vm2Buckets);
        }
        
        public String description() {
          return "expected to recover " + lostBuckets + " buckets, now have " + getBucketList(vm2);
        }
      }, 30000, 500, true);
      
      createData(vm0, 0, NUM_BUCKETS, "b");
      
      //VM1 should recover, but it shouldn't host the bucket anymore
      createPR(vm1, 1, 0);

      //The data shouldn't be affected.
      checkData(vm1, 0, NUM_BUCKETS, "b");
      
      //restart everything, and make sure it comes back correctly.
      
      closeCache(vm1);
      closeCache(vm0);
      closeCache(vm2);
      
      AsyncInvocation async1 = createPRAsync(vm1, 1);
      AsyncInvocation async0 = createPRAsync(vm0, 1);
      
      //Make sure we wait for vm2, because it's got the latest copy of the bucket
      async1.join(50);
      //FAILED On this line
      assertTrue(async1.isAlive());
      
      AsyncInvocation async2 = createPRAsync(vm2, 1);
      
      async2.getResult(MAX_WAIT);
      async0.getResult(MAX_WAIT);
      async1.getResult(MAX_WAIT);
      
      //The data shouldn't be affected.
      checkData(vm1, 0, NUM_BUCKETS, "b");
      assertEquals(Collections.emptySet(), getBucketList(vm1));
      assertEquals(vm0Buckets, getBucketList(vm0));
      assertEquals(vm0Buckets, getBucketList(vm2));
    }
    
    /**
     * This test this case
     *   we replace buckets where are offline on A by creating them on C
     *   We then shutdown C and restart A, which recovers those buckets
     */
    public void testBug41340() throws Throwable {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      final VM vm2 = host.getVM(2);
      createPR(vm0, 1, 0);
      createPR(vm1, 1, 0);
      
      createData(vm0, 0, NUM_BUCKETS, "a");
      
      //This should do nothing because we have satisfied redundancy.
      createPR(vm2, 1, 0);
      assertEquals(Collections.emptySet(), getBucketList(vm2));
      
      Set<Integer> vm0Buckets = getBucketList(vm0);
      final Set<Integer> lostBuckets = getBucketList(vm1);
      
      closeCache(vm1);
      
      //VM2 should pick up the slack
      waitForBucketRecovery(vm2, lostBuckets);
      
      createData(vm0, 0, NUM_BUCKETS, "b");
      
      //VM1 should recover, but it shouldn't host the bucket anymore
      createPR(vm1, 1, 0);
      
      //The data shouldn't be affected.
      checkData(vm1, 0, NUM_BUCKETS, "b");
      
      closeCache(vm2);
      
      //The buckets should move back to vm1.
      waitForBucketRecovery(vm1, lostBuckets);
      
      assertEquals(vm0Buckets, getBucketList(vm0));
      assertEquals(vm0Buckets, getBucketList(vm1));
      
      //The data shouldn't be affected.
      checkData(vm1, 0, NUM_BUCKETS, "b");
      
      //restart everything, and make sure it comes back correctly.
      
      closeCache(vm0);
      closeCache(vm1);
      
      AsyncInvocation async1 = createPRAsync(vm1, 1);
      AsyncInvocation async0 = createPRAsync(vm0, 1);
      
      async0.getResult(MAX_WAIT);
      async1.getResult(MAX_WAIT);
      
      //The data shouldn't be affected.
      checkData(vm1, 0, NUM_BUCKETS, "b");
      assertEquals(vm0Buckets, getBucketList(vm0));
      assertEquals(vm0Buckets, getBucketList(vm1));
    }
    
    
  
  /** Test the with redundancy
   * 1, we restore the same buckets when the
   * missing member comes back online.
   */
  public void testMissingMemberRedundancy1() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createPR(vm0, 1);
    createPR(vm1, 1);
    
    createData(vm0, 0, NUM_BUCKETS, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    
    closeCache(vm1);
    
    //This should work, because this bucket is still available.
    checkData(vm0, 0, NUM_BUCKETS, "a");
    
    removeData(vm0, 0, NUM_BUCKETS/2);
    createData(vm0, NUM_BUCKETS/2, NUM_BUCKETS, "b");
    
    //This shouldn't create any buckets, because we know there are offline copies
    createPR(vm2, 1);
    
    Set<Integer> vm2Buckets = getBucketList(vm2);
    assertEquals(Collections.emptySet(), vm2Buckets);
    
    createPR(vm1, 1);

    //The data should be back online now.
    //and vm1 should have received the latest copy
    //of the data.
    checkData(vm1, 0, NUM_BUCKETS/2, null);
    checkData(vm1, NUM_BUCKETS/2, NUM_BUCKETS, "b");
    
    
    //Make sure we restored the buckets in the right 
    //place
    assertEquals(vm0Buckets, getBucketList(vm0));
    assertEquals(vm1Buckets, getBucketList(vm1));
    assertEquals(Collections.emptySet(), getBucketList(vm2));
  }
  
  /** 
   * Test that we don't record our old
   * member ID as offline, preventing redundancy
   * recovery in the future.
   */
  public void testBug41341() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createPR(vm0, 1);
    createPR(vm1, 1);
    
    createData(vm0, 0, 1, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    
    assertEquals(Collections.singleton(0), vm0Buckets);
    assertEquals(Collections.singleton(0), vm1Buckets);
    
    closeCache(vm1);
    
    //This shouldn't create any buckets, because we know there are offline copies
    createPR(vm2, 1);
    
    assertEquals(1, getOfflineMembers(0, vm0).size());
    //Note, vm2 will consider vm1 as "online" because vm2 doesn't host the bucket
    assertEquals(2, getOnlineMembers(0, vm2).size());
    
    Set<Integer> vm2Buckets = getBucketList(vm2);
    assertEquals(Collections.emptySet(), vm2Buckets);
    
    createPR(vm1, 1);

    //Make sure we restored the buckets in the right 
    //place
    assertEquals(vm0Buckets, getBucketList(vm0));
    assertEquals(vm1Buckets, getBucketList(vm1));
    assertEquals(Collections.emptySet(), getBucketList(vm2));
    
    assertEquals(Collections.emptySet(), getOfflineMembers(0, vm0));
    assertEquals(Collections.emptySet(), getOfflineMembers(0, vm1));
    
    moveBucket(0, vm1, vm2);
    
    assertEquals(Collections.emptySet(), getOfflineMembers(0, vm0));
    assertEquals(Collections.emptySet(), getOfflineMembers(0, vm1));
    
    assertEquals(Collections.emptySet(), getOfflineMembers(0, vm2));
    
    assertEquals(Collections.singleton(0), getBucketList(vm0));
    assertEquals(Collections.emptySet(), getBucketList(vm1));
    assertEquals(Collections.singleton(0), getBucketList(vm2));
    
    //Destroy VM2
    destroyPR(vm2);
    
    assertEquals(Collections.emptySet(), getOfflineMembers(0, vm0));
    assertEquals(Collections.emptySet(), getOfflineMembers(0, vm1));
    
    //Close down VM 1
    closeCache(vm1);
    
    assertEquals(0, getOfflineMembers(0, vm0).size());
    
    //This should recover redundancy, because vm2 was destroyed
    createPR(vm1, 1);
    
    assertEquals(Collections.singleton(0), getBucketList(vm0));
    assertEquals(Collections.singleton(0), getBucketList(vm1));
    
    assertEquals(Collections.emptySet(), getOfflineMembers(0, vm0));
    assertEquals(Collections.emptySet(), getOfflineMembers(0, vm1));
  }
  
  /**
   * Test that we throw away a bucket 
   * if we restored redundancy while 
   * that bucket was offline.
   */
  public void z_testThrowAwayUneededBucket() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createPR(vm0, 1);
    createPR(vm1, 1);
    
    createData(vm0, 0, NUM_BUCKETS, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    assertEquals(vm0Buckets, vm1Buckets);
    assertEquals(NUM_BUCKETS, vm0Buckets.size());
    
    closeCache(vm1);
    createPR(vm2, 1);
    
    checkData(vm0, 0, NUM_BUCKETS, "a");
    
    vm0Buckets = getBucketList(vm0);
    Set<Integer> vm2Buckets = getBucketList(vm2);
    
    //Each node should have a full copy of everything
    assertEquals(vm0Buckets, vm2Buckets);
    assertEquals(NUM_BUCKETS, vm0Buckets.size());
    
    createPR(vm1, 1);
    
    assertEquals(Collections.emptySet(), getBucketList(vm1));
    
    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);
    
    createPR(vm0, 1);
    createPR(vm1, 1);
    createPR(vm2, 1);
    
    assertEquals(vm0Buckets,getBucketList(vm0));
    assertEquals(Collections.emptySet(), getBucketList(vm1));
    assertEquals(vm2Buckets,getBucketList(vm2));
    
    checkData(vm0, 0, NUM_BUCKETS, "a");
  }
  
  public void testMoveBucket() throws Throwable {
    int redundancy = 0;
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createPR(vm0, redundancy);
    
    createData(vm0, 0, 2, "a");
    
    createPR(vm1, redundancy);
    createPR(vm2, redundancy);
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    Set<Integer> vm2Buckets = getBucketList(vm2);
    
    moveBucket(0, vm0, vm1);
    moveBucket(0, vm1, vm2);
    createData(vm0, 113, 114, "a");
    moveBucket(0, vm2, vm0);
    
    createData(vm0, 226, 227, "a");
    
    assertEquals(vm0Buckets,getBucketList(vm0));
    assertEquals(vm1Buckets,getBucketList(vm1));
    assertEquals(vm2Buckets,getBucketList(vm2));
    
    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);
    
    AsyncInvocation a1 = createPRAsync(vm0, redundancy);
    AsyncInvocation a2 = createPRAsync(vm1, redundancy);
    AsyncInvocation a3 = createPRAsync(vm2, redundancy);
    
    a1.getResult(MAX_WAIT);
    a2.getResult(MAX_WAIT);
    a3.getResult(MAX_WAIT);
    
    assertEquals(vm2Buckets,getBucketList(vm2));
    assertEquals(vm1Buckets,getBucketList(vm1));
    assertEquals(vm0Buckets,getBucketList(vm0));
    
    checkData(vm0, 0, 2, "a");
    checkData(vm0, 113, 114, "a");
    checkData(vm0, 226, 227, "a");
  }
  
  public void testCleanStop() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    createPR(vm0, 1);
    createPR(vm1, 1);
    
    createData(vm0, 0, 1, "a");
    
    fakeCleanShutdown(vm1, 0);
    fakeCleanShutdown(vm0, 0);
    
    AsyncInvocation async1 = createPRAsync(vm0, 1);
    //[dsmith] Make sure that vm0 is waiting for vm1 to recover
    //If VM(0) recovers early, that is a problem, because
    //we can now longer do a clean restart
    AsyncInvocation async2 = createPRAsync(vm1, 1);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);
    
    checkData(vm0, 0, 1, "a");
    checkData(vm1, 0, 1, "a");
    
    checkRecoveredFromDisk(vm0, 0, true);
    checkRecoveredFromDisk(vm1, 0, true);
    
    closePR(vm0);
    closePR(vm1);
    
    async1 = createPRAsync(vm0, 1);
    async2 = createPRAsync(vm1, 1);
    async1.getResult(MAX_WAIT);
    async2.getResult(MAX_WAIT);
    
    checkData(vm0, 0, 1, "a");
    checkData(vm1, 0, 1, "a");
    
    checkRecoveredFromDisk(vm0, 0, false);
    checkRecoveredFromDisk(vm1, 0, true);
  }
  
  
  
  public void testRegisterInterestNoDataStores() {
    //Closing the client may log a warning on the server
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("SocketTimeoutException");
    IgnoredException.addIgnoredException("ServerConnectivityException");
    IgnoredException.addIgnoredException("Socket Closed");
    IgnoredException.addIgnoredException("Unexpected IOException");
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    final Integer serverPort = (Integer) vm0.invoke(new SerializableCallable("create per") {
      
      public Object call () {
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        paf.setLocalMaxMemory(0);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PARTITION);
        cache.createRegion(PR_REGION_NAME, af.create());
        
        CacheServer server = cache.addCacheServer();
        server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
        server.setNotifyBySubscription(true);
        try {
          server.start();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return server.getPort();
        
      }
    });
    
    vm1.invoke(new SerializableRunnable("create client") {
      
      public void run () {
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", "");
        getSystem(props );
        try {
          Cache cache = getCache();

          PoolFactory pf = PoolManager.createFactory();
          pf.addServer(NetworkUtils.getServerHostName(host), serverPort);
          pf.setSubscriptionEnabled(true);
          pf.create("pool");
          AttributesFactory af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          af.setScope(Scope.LOCAL);
          af.setPoolName("pool");
          Region region = cache.createRegion(PR_REGION_NAME, af.create());
          try {
            region.registerInterestRegex(".*");
          } catch(ServerOperationException e) {
            if(!(e.getCause() instanceof PartitionedRegionStorageException)) {
              throw e;
            }
          }
        } finally {
          disconnectFromDS();
        }
      }
    });
  }
  
  /**
   * This test is in here just to test to make
   * sure that we don't get a suspect string
   * with an exception during cache closure.
   */
  public void testOverflowCacheClose() {
    Cache cache = getCache();
    RegionFactory rf = new RegionFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    rf.setPartitionAttributes(paf.create());
    rf.setDataPolicy(DataPolicy.PARTITION);
    rf.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(50, EvictionAction.OVERFLOW_TO_DISK));
    rf.setDiskDirs(getDiskDirs());
    
    Region region = rf.create(PR_REGION_NAME);
    region.get(0);
    cache.getDistributedSystem().disconnect();
//    cache.close();
  }
  
  /**
   * Test for bug 41336
   */
  public void testCrashDuringBucketCreation() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    vm0.invoke(new SerializableRunnable("Install observer") {
      
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeSendMessage(DistributionManager dm,
              DistributionMessage msg) {
            if(msg instanceof ManageBucketReplyMessage) {
              Cache cache = getCache();
              DistributedTestCase.disconnectFromDS();
              
              await().atMost(30, SECONDS).until(() -> {return (cache == null || cache.isClosed());});
              LogWriterUtils.getLogWriter().info("Cache is confirmed closed");
            }
          }
        });
        
      }
    });
    createPR(vm0, 0);
    createPR(vm1, 0);
    
    createData(vm1, 0, 4, "a");
    
    Set<Integer> vm1Buckets = getBucketList(vm1);

    //Make sure the test hook ran
    vm0.invoke(new SerializableRunnable("Check for no distributed system") {
      
      public void run() {
        assertEquals(null,GemFireCacheImpl.getInstance());
      }
    });
    
    checkData(vm1, 0, 4, "a");
    assertEquals(4, vm1Buckets.size());
    
    createPR(vm0, 0);
    checkData(vm0, 0, 4, "a");
    assertEquals(vm1Buckets, getBucketList(vm1));
    assertEquals(Collections.emptySet(), getBucketList(vm0));
    
    closeCache(vm0);
    closeCache(vm1);
    
    AsyncInvocation async0 = createPRAsync(vm0, 0);
    AsyncInvocation async1 = createPRAsync(vm1, 0);
    async0.getResult();
    async1.getResult();
    
    checkData(vm0, 0, 4, "a");
    assertEquals(vm1Buckets, getBucketList(vm1));
    assertEquals(Collections.emptySet(), getBucketList(vm0));
  }
  
  public void testNestedPRRegions() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int numBuckets = 50;
    
    createNestedPR(vm0);
    createNestedPR(vm1);
    createNestedPR(vm2);
    
    createData(vm0, 0, numBuckets, "a", "parent1/"+PR_REGION_NAME);
    createData(vm0, 0, numBuckets, "b", "parent2/"+PR_REGION_NAME);
    checkData(vm2, 0, numBuckets, "a", "parent1/"+PR_REGION_NAME);
    checkData(vm2, 0, numBuckets, "b", "parent2/"+PR_REGION_NAME);
    
    Set<Integer> vm1_0Buckets = getBucketList(vm0, "parent1/"+PR_REGION_NAME);
    Set<Integer> vm1_1Buckets = getBucketList(vm1, "parent1/"+PR_REGION_NAME);
    Set<Integer> vm1_2Buckets = getBucketList(vm2, "parent1/"+PR_REGION_NAME);
    
    Set<Integer> vm2_0Buckets = getBucketList(vm0, "parent2/"+PR_REGION_NAME);
    Set<Integer> vm2_1Buckets = getBucketList(vm1, "parent2/"+PR_REGION_NAME);
    Set<Integer> vm2_2Buckets = getBucketList(vm2, "parent2/"+PR_REGION_NAME);
    
    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);
    
    AsyncInvocation async0 = createNestedPRAsync(vm0);
    //[dsmith] Make sure that vm0 is waiting for vm1 and vm2 to recover
    //If VM(0) recovers early, that is a problem, because vm1 
    //has newer data
    Thread.sleep(50);
    assertTrue(async0.isAlive());
    AsyncInvocation async1 = createNestedPRAsync(vm1);
    AsyncInvocation async2 = createNestedPRAsync(vm2);
    async0.getResult();
    async1.getResult();
    async2.getResult();
    
    assertEquals(vm1_0Buckets,getBucketList(vm0, "parent1/"+PR_REGION_NAME));
    assertEquals(vm1_1Buckets,getBucketList(vm1, "parent1/"+PR_REGION_NAME));
    assertEquals(vm1_2Buckets,getBucketList(vm2, "parent1/"+PR_REGION_NAME));
    
    assertEquals(vm2_0Buckets,getBucketList(vm0, "parent2/"+PR_REGION_NAME));
    assertEquals(vm2_1Buckets,getBucketList(vm1, "parent2/"+PR_REGION_NAME));
    assertEquals(vm2_2Buckets,getBucketList(vm2, "parent2/"+PR_REGION_NAME));
    
    checkData(vm0, 0, numBuckets, "a", "parent1/"+PR_REGION_NAME);
    checkData(vm0, 0, numBuckets, "b", "parent2/"+PR_REGION_NAME);
    createData(vm1, numBuckets, 113, "c", "parent1/"+PR_REGION_NAME);
    createData(vm1, numBuckets, 113, "d", "parent2/"+PR_REGION_NAME);
    checkData(vm2, numBuckets, 113, "c", "parent1/"+PR_REGION_NAME);
    checkData(vm2, numBuckets, 113, "d", "parent2/"+PR_REGION_NAME);
  }
  
  public void testCloseDuringRegionOperation() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPR(vm0, 1, -1, 1);
    createPR(vm1, 1, -1, 1);
    
    //Make sure we create a bucket
    createData(vm1, 0, 1, "a");
    
    //Try to make sure there are some operations in flight while closing the cache
    SerializableCallable createData = new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(PR_REGION_NAME);
        
        int i =0;
        while(true) {
          try {
            region.put(0, i);
            i++;
          } catch(CacheClosedException e) {
            break;
          }
        }
        return i-1;
      }
    };
    
    AsyncInvocation asyncCreate = vm0.invokeAsync(createData);
    
    Thread.sleep(100);
    
    AsyncInvocation close0 = closeCacheAsync(vm0);
    AsyncInvocation close1 = closeCacheAsync(vm1);
    
    //wait for the close to finish
    close0.getResult();
    close1.getResult();
    
    Integer lastSuccessfulInt = (Integer) asyncCreate.getResult();
    System.err.println("Cache was closed on integer " + lastSuccessfulInt);
    
    AsyncInvocation create1 = createPRAsync(vm0, 1, -1, 1);
    AsyncInvocation create2 = createPRAsync(vm1, 1, -1, 1);
    
    create1.getResult(MAX_WAIT);
    create2.getResult(MAX_WAIT);
    
    
    SerializableCallable getValue = new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(PR_REGION_NAME);
        int value = (Integer) region.get(0);
        return value;
      }
    };
    
    int vm1Value  = (Integer) vm0.invoke(getValue);
    int vm2Value = (Integer) vm1.invoke(getValue);
    assertEquals(vm1Value, vm2Value);
    assertTrue("value = " + vm1Value + ", lastSuccessfulInt=" + lastSuccessfulInt, 
        vm1Value == lastSuccessfulInt || vm1Value == lastSuccessfulInt+1);
  }
  
  /**
   * Test for bug 4226.
   * 1. Member A has the bucket
   * 2. Member B starts creating the bucket. It tells member A that it hosts the bucket
   * 3. Member A crashes
   * 4. Member B destroys the bucket and throws a partition offline exception, because it wasn't able to complete initialization.
   * 5. Member A recovers, and gets stuck waiting for member B.
   * @throws Throwable 
  */
  public void testBug42226() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    //Add a hook which will disconnect from the distributed
    //system when the initial image message shows up.
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
           if(message instanceof RequestImageMessage) {
             RequestImageMessage rim = (RequestImageMessage) message;
             //Don't disconnect until we see a bucket
             if(rim.regionPath.contains("_B_")) {
               DistributionMessageObserver.setInstance(null);
               disconnectFromDS();
             }
           }
          }
          
          @Override
          public void afterProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            
          }
        });
      }
    });
    
    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    createPR(vm0, 1, 0, 1);
    
    //Make sure we create a bucket
    createData(vm0, 0, 1, "a");
    
    //This should recover redundancy, which should cause vm0 to disconnect
    
    IgnoredException ex = IgnoredException.addIgnoredException("PartitionOfflineException");
    try { 
    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    createPR(vm1, 1, 0, 1);
    
    //Make sure get a partition offline exception
    try {
      createData(vm1, 0, 1, "a");
    } catch (RMIException e) {
      //We expect a PartitionOfflineException
      if(!(e.getCause() instanceof PartitionOfflineException)) {
        throw e;
      }
    }
    
    } finally {
      ex.remove();
    }
    
    //Make sure vm0 is really disconnected (avoids a race with the observer).
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        disconnectFromDS();
      }
    });
    
    //This should recreate the bucket
    AsyncInvocation async1 = createPRAsync(vm0, 1, 0, 1);
    async1.getResult(MAX_WAIT);
    
    checkData(vm1, 0, 1, "a");
  }
  
  /**
   * A test to make sure that we allow the PR to be used
   * after at least one copy of every bucket is recovered,
   * but before the secondaries are initialized.
   * 
   * @throws Throwable
   */
  public void testAllowRegionUseBeforeRedundancyRecovery() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    final int redundancy = 1;
    int numBuckets = 20;
    
    createPR(vm0, redundancy);
    createPR(vm1, redundancy);
    createPR(vm2, redundancy);
    
    createData(vm0, 0, numBuckets, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    Set<Integer> vm2Buckets = getBucketList(vm2);
    
    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);
    
    SerializableRunnable slowGII = new SerializableRunnable("Slow down GII") {
      
      @SuppressWarnings("synthetic-access")
      public void run() {
        InternalResourceManager.setResourceObserver(new RecoveryObserver());
        DistributionMessageObserver.setInstance(new BlockGIIMessageObserver());
      }
    };
    
    SerializableRunnable resetSlowGII = new SerializableRunnable("Unset the slow GII") {

      public void run() {

        BlockGIIMessageObserver messageObserver = (BlockGIIMessageObserver) DistributionMessageObserver.setInstance(null);
        RecoveryObserver recoveryObserver = (RecoveryObserver) InternalResourceManager.getResourceObserver();
        messageObserver.cdl.countDown();
        try {
          recoveryObserver.recoveryDone.await();
        } catch (InterruptedException e) {
          Assert.fail("Interrupted", e);
        }
        InternalResourceManager.setResourceObserver(null);
      }
    };

    try {
      vm0.invoke(slowGII);
      vm1.invoke(slowGII);
      vm2.invoke(slowGII);

      SerializableRunnable createPR = new SerializableRunnable("create PR") {

        public void run() {

          Cache cache = getCache();
          RegionAttributes attr = getPersistentPRAttributes(redundancy,
              -1, cache, 113, true);
          cache.createRegion(PR_REGION_NAME, attr);
        }
      };

      AsyncInvocation a1 = vm0.invokeAsync(createPR);
      AsyncInvocation a2 = vm1.invokeAsync(createPR);
      AsyncInvocation a3 = vm2.invokeAsync(createPR);

      a1.getResult(MAX_WAIT);
      a2.getResult(MAX_WAIT);
      a3.getResult(MAX_WAIT);


      //Make sure all of the primary are available.
      checkData(vm0, 0, numBuckets, "a");
      createData(vm0, 113, 113 + numBuckets, "b");

      //But none of the secondaries
      Set<Integer> vm0InitialBuckets = getBucketList(vm0);
      Set<Integer> vm1InitialBuckets = getBucketList(vm1);
      Set<Integer> vm2InitialBuckets = getBucketList(vm2);
      assertEquals("vm0=" + vm0InitialBuckets + ",vm1=" + vm1InitialBuckets
          + "vm2=" + vm2InitialBuckets, numBuckets, vm0InitialBuckets.size()
          + vm1InitialBuckets.size() + vm2InitialBuckets.size());
    } finally {
      //Reset the slow GII flag, and wait for the redundant buckets
      //to be recovered.
      AsyncInvocation reset0 = vm0.invokeAsync(resetSlowGII);
      AsyncInvocation reset1 = vm1.invokeAsync(resetSlowGII);
      AsyncInvocation reset2 = vm2.invokeAsync(resetSlowGII);
      reset0.getResult(MAX_WAIT);
      reset1.getResult(MAX_WAIT);
      reset2.getResult(MAX_WAIT);
    }
    
    //Now we better have all of the buckets
    assertEquals(vm0Buckets,getBucketList(vm0));
    assertEquals(vm1Buckets,getBucketList(vm1));
    assertEquals(vm2Buckets,getBucketList(vm2));
    
    //Make sure the members see the data recovered from disk
    //in those secondary buckets
    checkData(vm0, 0, numBuckets, "a");
    checkData(vm1, 0, numBuckets, "a");
    
    //Make sure the members see the new updates
    //in those secondary buckets
    checkData(vm0, 113, 113 + numBuckets, "b");
    checkData(vm1, 113, 113 + numBuckets, "b");
  }
  
  /**
   * A test for bug 41436. If the GII source
   * crashes before the GII is complete, we need
   * to make sure that later we can recover redundancy.
   */
  public void testCrashDuringBucketGII() {
    IgnoredException.addIgnoredException("PartitionOfflineException");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createPR(vm0, 1);
    
    createData(vm0, 0, 1, "value");
    
    //Add an observer which will close the cache when the GII starts
    vm0.invoke(new SerializableRunnable("Set crashing observer") {
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof RequestImageMessage) {
              RequestImageMessage rim = (RequestImageMessage) message;
              if(rim.regionPath.contains("_0")) {
                DistributionMessageObserver.setInstance(null);
                getCache().close();
              }
            }
          }
          
        });
      }
    });
    
    createPR(vm1, 1);
    
    //Make sure vm1 didn't create the bucket
    assertEquals(Collections.emptySet(), getBucketList(vm1));
    
    createPR(vm0, 1, 0);
    
    //Make sure vm0 recovers the bucket
    assertEquals(Collections.singleton(0), getBucketList(vm0));
    
    //vm1 should satisfy redundancy for the bucket as well
    assertEquals(Collections.singleton(0), getBucketList(vm1));
  }
  
  /**
   * Another test for bug 41436. If the GII source
   * crashes before the GII is complete, we need
   * to make sure that later we can recover redundancy.
   * 
   * In this test case, we bring the GII down before we 
   * bring the source back up, to make sure the source still
   * discovers that the GII target is no longer hosting the bucket.
   * @throws InterruptedException 
   */
  public void testCrashDuringBucketGII2() throws InterruptedException {
    IgnoredException.addIgnoredException("PartitionOfflineException");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createPR(vm0, 1);
    
    createData(vm0, 0, 1, "value");
    
    //Add an observer which will close the cache when the GII starts
    vm0.invoke(new SerializableRunnable("Set crashing observer") {
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof RequestImageMessage) {
              RequestImageMessage rim = (RequestImageMessage) message;
              if(rim.regionPath.contains("_0")) {
                DistributionMessageObserver.setInstance(null);
                getCache().close();
              }
            }
          }
          
        });
      }
    });
    
    createPR(vm1, 1);
    
    //Make sure vm1 didn't create the bucket
    assertEquals(Collections.emptySet(), getBucketList(vm1));
    
    closeCache(vm1);
    
    AsyncInvocation async0 = createPRAsync(vm0, 1, 0, 113);
    
    async0.join(500);
    
    //vm0 should get stuck waiting for vm1 to recover from disk,
    //because vm0 thinks vm1 has the bucket
    assertTrue(async0.isAlive());
    
    createPR(vm1, 1, 0);
    
    //Make sure vm0 recovers the bucket
    assertEquals(Collections.singleton(0), getBucketList(vm0));
    
    //vm1 should satisfy redundancy for the bucket as well
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return(Collections.singleton(0).equals(getBucketList(vm1)));
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 30 * 1000, 200, true);
    assertEquals(Collections.singleton(0), getBucketList(vm1));
  }
  
  public void testCleanupAfterConflict() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    createPR(vm0, 0);
    //create some buckets
    createData(vm0, 0, 2, "a");
    closePR(vm0);
    createPR(vm1, 0);
    //create an overlapping bucket

    
    //TODO - this test hangs if vm1 has some buckets that vm0
    //does not have. The problem is that when vm0 starts up and gets a conflict
    //on some buckets, it updates it's view for other buckets.
//  createData(vm1, 1, 3, "a");
    createData(vm1, 1, 2, "a");
    
    //this should throw a conflicting data exception.
    IgnoredException expect = IgnoredException.addIgnoredException("ConflictingPersistentDataException", vm0);
    try {
      createPR(vm0, 0);
      fail("should have seen a conflicting data exception");
    } catch(Exception e) {
      if(!(e.getCause() instanceof ConflictingPersistentDataException)) {
        throw e;  
      }
    } finally {
      expect.remove();
    }
    
    //This will hang, if this test fails.
    //TODO - DAN - I'm not even sure what this means here?
    //It seems like if anything, vm1 should not have updated it's persistent
    //view from vm0 because vm0 was in conflict!
    //In fact, this is a bit of a problem, because now vm1 is dependent
    //on vm vm0.
    expect = IgnoredException.addIgnoredException("PartitionOfflineException", vm1);
    try {
      createData(vm1, 0, 1, "a");
      fail("Should have seen a PartitionOfflineException for bucket 0");
    } catch(Exception e) {
      if(!(e.getCause() instanceof PartitionOfflineException)) {
        throw e;  
      }
    } finally {
      expect.remove();
    }
    
    closePR(vm1);
    
    //This should succeed, vm0 should not have persisted any view
    //information from vm1
    createPR(vm0, 0);
    checkData(vm0, 0, 2, "a");
    checkData(vm0, 2, 3, null);
  }
  
  /**
   * Test to make sure that primaries are rebalanced after recovering from
   * disk.
   */
  public void testPrimaryBalanceAfterRecovery() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int numBuckets = 30;
    
    createPR(vm0, 1);
    createPR(vm1, 1);
    createPR(vm2, 1);
    
    createData(vm0, 0, numBuckets, "a");
    
    Set<Integer> vm0Buckets = getBucketList(vm0);
    Set<Integer> vm1Buckets = getBucketList(vm1);
    Set<Integer> vm2Buckets = getBucketList(vm2);
    
    //We expect to see 10 primaries on each node since we have 30 buckets
    Set<Integer> vm0Primaries = getPrimaryBucketList(vm0);
    assertEquals("Expected 10 primaries " + vm0Primaries, 10, vm0Primaries.size());
    Set<Integer> vm1Primaries = getPrimaryBucketList(vm1);
    assertEquals("Expected 10 primaries " + vm1Primaries, 10, vm1Primaries.size());
    Set<Integer> vm2Primaries = getPrimaryBucketList(vm2);
    assertEquals("Expected 10 primaries " + vm2Primaries, 10, vm2Primaries.size());
    
    //bounce vm0
    closeCache(vm0);
    createPR(vm0, 1);
    
    waitForBucketRecovery(vm0, vm0Buckets);
    assertEquals(vm0Buckets,getBucketList(vm0));
    assertEquals(vm1Buckets,getBucketList(vm1));
    assertEquals(vm2Buckets,getBucketList(vm2));
    
    //The primaries should be evenly distributed after recovery.
    vm0Primaries = getPrimaryBucketList(vm0);
    assertEquals("Expected 10 primaries " + vm0Primaries, 10, vm0Primaries.size());
    vm1Primaries = getPrimaryBucketList(vm1);
    assertEquals("Expected 10 primaries " + vm1Primaries, 10, vm1Primaries.size());
    vm2Primaries = getPrimaryBucketList(vm2);
    assertEquals("Expected 10 primaries " + vm2Primaries, 10, vm2Primaries.size());
  }

  public void testConcurrencyChecksEnabled() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final String regionName = getName();
    
    SerializableCallable createPR = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        RegionFactory<Integer, String> rf = getCache().createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
        Region<Integer, String> r = rf.create(regionName);
        assertTrue(r.getAttributes().getConcurrencyChecksEnabled());
        return null;
      }
    };
    
    SerializableCallable createPRProxy = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        RegionFactory<Integer, String> rf = getCache().createRegionFactory(RegionShortcut.PARTITION_PROXY);
        Region<Integer, String> r = rf.create(regionName);
        return null;
      }
    };
    vm0.invoke(createPRProxy);
    vm1.invoke(createPR);
    vm2.invoke(createPR);
    vm3.invoke(createPRProxy);

    SerializableCallable verifyConcurrenyChecks = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        assertTrue(r.getAttributes().getConcurrencyChecksEnabled());
        return null;
      }
    };
    vm0.invoke(verifyConcurrenyChecks);
    vm3.invoke(verifyConcurrenyChecks);
  }

  public void testNonPersistentProxy() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    final String regionName = getName();
    
    SerializableCallable createAccessor = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        getCache().createRegionFactory(RegionShortcut.PARTITION_PROXY).create(regionName);
        return null;
      }
    };
    vm1.invoke(createAccessor);
    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).create(regionName);
        assertTrue(r.getAttributes().getConcurrencyChecksEnabled());
        return null;
      }
    });
    vm3.invoke(createAccessor);
    
    SerializableCallable verifyConcurrencyChecks = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        assertTrue(r.getAttributes().getConcurrencyChecksEnabled());
        return null;
      }
    };
    vm1.invoke(verifyConcurrencyChecks);
    vm3.invoke(verifyConcurrencyChecks);
  }
  
  public void testReplicateAfterPersistent() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    
    final String regionName = getName();
    
    SerializableCallable createPersistentReplicate = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).create(regionName);
        return null;
      }
    };
    
    SerializableCallable createNonPersistentReplicate = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        return null;
      }
    };
    
    vm1.invoke(createPersistentReplicate);
    vm2.invoke(createNonPersistentReplicate);
    vm3.invoke(createPersistentReplicate);
  }

  private static final class RecoveryObserver extends
      InternalResourceManager.ResourceObserverAdapter {
    final CountDownLatch recoveryDone = new CountDownLatch(1);

    @Override
    public void rebalancingOrRecoveryFinished(Region region) {
      if(region.getName().equals(PR_REGION_NAME)) {
        recoveryDone.countDown();
      }
    }
  }

  private static class TestFunction implements Function, Serializable {

    public void execute(FunctionContext context) {
      context.getResultSender().lastResult(null);
    }

    public String getId() {
      return TestFunction.class.getSimpleName();
    }

    public boolean hasResult() {
      return true;
    }

    public boolean optimizeForWrite() {
      return false;
    }

    public boolean isHA() {
      return false;
    }
  }
  
  private static class BlockGIIMessageObserver extends DistributionMessageObserver {
    CountDownLatch cdl = new CountDownLatch(1);

    @Override
    public void beforeSendMessage(DistributionManager dm,
        DistributionMessage message) {
      if(message instanceof RequestImageMessage) {
        RequestImageMessage rim = (RequestImageMessage) message;
        //make sure this is a bucket region doing a GII
        if(rim.regionPath.contains("B_")) {
          try {
            cdl.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    
  }

  private static class TestCustomExpiration implements CustomExpiry {

    public void close() {
      //do nothing
    }

    public ExpirationAttributes getExpiry(Entry entry) {
      return new ExpirationAttributes((entry.getKey().hashCode() + entry.getValue().hashCode()) % 100, ExpirationAction.INVALIDATE); 
    }
  }
}
