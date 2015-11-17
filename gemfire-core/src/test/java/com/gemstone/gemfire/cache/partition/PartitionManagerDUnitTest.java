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
package com.gemstone.gemfire.cache.partition;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.RMIException;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Test of the PartitionManager.createPrimaryBucket method.
 * @author dsmith
 *
 */
public class PartitionManagerDUnitTest extends CacheTestCase {
  
  private static final int BUCKET_ID = 5;
  protected static final long CONCURRENT_TIME = 10;
  protected static final long MAX_WAIT = 60 * 1000;
  

  public PartitionManagerDUnitTest(String name) {
    super(name);
  }

  public void testDestroyExistingRemote() {
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
        cache.createRegion("region1", attr.create());
      }
    };
    
    //create the PR
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    

    //Move the bucket around a couple times. Make sure it moves
    createPrimaryBucket(vm0, true, false);
    assertNotPrimary(vm1);
    createPrimaryBucket(vm1, true, false);
    assertNotPrimary(vm0);
    
    //Put something in the bucket
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        region.put(BUCKET_ID, "B");
      }
    });
    
    //Make sure we don't mess with the if it's already local
    createPrimaryBucket(vm1, true, false);
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        assertEquals("B", region.get(BUCKET_ID));
      }
    });

    //Make sure we drop the data when we move the bucket
    createPrimaryBucket(vm0, true, false);
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        assertEquals(null, region.get(BUCKET_ID));
      }
    });
  }
  
  public void testDestroyExistingBoth() {
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
        cache.createRegion("region1", attr.create());
      }
    };
    
    //create the PR
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    

    //Move the bucket around a couple times. Make sure it moves
    createPrimaryBucket(vm0, true, true);
    createPrimaryBucket(vm1, true, true);
    
    //Put something in the bucket
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        region.put(BUCKET_ID, "B");
      }
    });
    
    //Make sure we do drop the data even if the bucket is already local
    createPrimaryBucket(vm1, true, true);
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        assertEquals(null, region.get(BUCKET_ID));
      }
    });
  }
  
  public void testDestroyExistingLocal() {
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
        cache.createRegion("region1", attr.create());
      }
    };
    
    //create the PR
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    

    createPrimaryBucket(vm0, false, true);
    
    //This should throw an exception
    try {
      createPrimaryBucket(vm1, false, false);
    } catch(RMIException e) {
      if(!(e.getCause() instanceof IllegalStateException)) {
        throw e;
      }
    }
    
    //Put something in the bucket
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        region.put(BUCKET_ID, "B");
      }
    });
    
    //Make sure we do drop the data even if the bucket is already local
    createPrimaryBucket(vm0, false, true);
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        assertEquals(null, region.get(BUCKET_ID));
      }
    });
  }
  
  public void testDestroyExistingNeither() {
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
        cache.createRegion("region1", attr.create());
      }
    };
    
    //create the PR
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    //Move the bucket around a couple times. Make sure it moves
    createPrimaryBucket(vm0, false, false);
    
    //This should throw an exception
    try {
      createPrimaryBucket(vm1, false, false);
    } catch(RMIException e) {
      if(!(e.getCause() instanceof IllegalStateException)) {
        throw e;
      }
    }
    
    //So should this
    try {
      createPrimaryBucket(vm0, false, false);
    } catch(RMIException e) {
      if(!(e.getCause() instanceof IllegalStateException)) {
        throw e;
      }
    }
    
    //Put something in the bucket
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        region.put(BUCKET_ID, "B");
      }
    });
    
    //Make sure we do drop the data even if the bucket is already local
    createPrimaryBucket(vm0, false, true);
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        assertEquals(null, region.get(BUCKET_ID));
      }
    });
  }

  private void createPrimaryBucket(VM vm0, final boolean destroyRemote, final boolean destroyLocal) {
    vm0.invoke(new SerializableRunnable("Move PR") {
      
      public void run() {
        Cache cache = getCache();
        final PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        PartitionManager.createPrimaryBucket(region, BUCKET_ID, destroyRemote, destroyLocal);
       
        //There is a race here that someone else on this same node could
        //trigger the bucket creation. If that happens, we won't create the bucket,
        //but getPrimary may not wait for the bucket either.
        //if we were doing a put, it would reattempt the put until the bucket was
        //created. In this case, we just want to wait for the bucket to have a primary
        waitForCriterion(new WaitCriterion() {
          
          public boolean done() {
            return region.getBucketPrimary(BUCKET_ID) != null;
          }
          
          public String description() {
            return null;
          }
        }, 10000, 100, false);
        
        //Make sure we really are the primary now
        assertEquals(cache.getDistributedSystem().getDistributedMember(), region.getBucketPrimary(BUCKET_ID));
        
        //Make sure there is only 1 bucket owner
        Set owners = region.getDataStore().getLocalBucketById(BUCKET_ID).getBucketOwners();
        assertEquals(1, owners.size());
      }
    });
  }
  
  private void assertNotPrimary(VM vm0) {
    vm0.invoke(new SerializableRunnable("Move PR") {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        //Make sure we're not the primary
        assertFalse(region.getBucketPrimary(BUCKET_ID).equals(cache.getDistributedSystem().getDistributedMember()));
      }
    });
  }
  
//  public void testLoop() throws Throwable {
//    for(int i=0 ;i < 50; i++) {
//      getLogWriter().info("test iteration #" + i);
//      doTestConcurrent();
//      tearDown();
//      setUp();
//    }
//  }
  
  public void testConcurrent() throws Throwable {
    
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
        cache.createRegion("region1", attr.create());
      }
    };
    
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    
    SerializableRunnable hungryHungryPrimary = new SerializableRunnable("Try to grab the primary a lot") {
      public void run()
      {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        
        long start = System.nanoTime();
        
        while(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < CONCURRENT_TIME) {
          PartitionManager.createPrimaryBucket(region, BUCKET_ID, true, false);
        }
      }
    };
    
    SerializableRunnable lotsOfPuts= new SerializableRunnable("A bunch of puts") {
      public void run()
      {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        
        long start = System.nanoTime();
        
        while(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < CONCURRENT_TIME) {
          //Do a put which might trigger bucket creation
          try {
            region.put(BUCKET_ID, "B");
          } catch (InternalGemFireError e) {
            if (e.getMessage().contains("recordVersion invoked for a local version tag that is higher")) {
              // bug #50566 encountered.  This can happen in this test that aggressively moves
              // buckets around w/o persistence, causing a node to create, destroy and recreate
              // the same bucket while the operation is in progress.  Not something likely
              // to happen to a customer.
            }
          }
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            fail("", e);
          }
        }
      }
    };
    
    AsyncInvocation async0 = vm0.invokeAsync(hungryHungryPrimary);
    AsyncInvocation async1 = vm1.invokeAsync(hungryHungryPrimary);
    
    AsyncInvocation async0_2 = vm0.invokeAsync(lotsOfPuts);
    AsyncInvocation async1_2 = vm1.invokeAsync(lotsOfPuts);
    
    
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    async0_2.getResult(MAX_WAIT);
    async1_2.getResult(MAX_WAIT);
    
    vm0.invoke(new SerializableRunnable("Check the number of owners") {
      public void run()
      {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        List owners;
        try {
          owners = region.getBucketOwnersForValidation(BUCKET_ID);
          assertEquals(1, owners.size());
        } catch (ForceReattemptException e) {
          fail("shouldn't have seen force reattempt", e);
        }
      }
    });
    
  }
}
