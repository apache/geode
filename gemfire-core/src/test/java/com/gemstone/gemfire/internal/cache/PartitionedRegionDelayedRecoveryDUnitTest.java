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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author dsmith
 *
 */
@SuppressWarnings("synthetic-access")
public class PartitionedRegionDelayedRecoveryDUnitTest extends CacheTestCase {
  
  public PartitionedRegionDelayedRecoveryDUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        InternalResourceManager.setResourceObserver(null);
      }
    });
    InternalResourceManager.setResourceObserver(null);
  }

  public void testNoRecovery() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPrRegions = new SerializableRunnable("createRegions") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        paf.setRedundantCopies(1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //create the region in 2 VMS
    vm0.invoke(createPrRegions);
    vm1.invoke(createPrRegions);
    
    //Do 1 put, which should create 1 bucket on both Vms
    vm0.invoke(new SerializableRunnable("putData") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        region1.put("A", "B");
      }
    });
    
    //create the PR on another region, which won't have the bucket
    vm2.invoke(createPrRegions);

    //destroy the region in 1 of the VM's that's hosting the bucket
    vm1.invoke(new SerializableRunnable("Destroy region") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        region1.localDestroyRegion();
      }
    });
    


    //check to make sure we didn't make a copy of the low redundancy bucket
    SerializableRunnable checkNoBucket = new SerializableRunnable("Check for bucket") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        assertEquals(0,region1.getDataStore().getBucketsManaged());
      }
    };
    
    //Wait for a bit, maybe the region will try to make a copy of the bucket
    Thread.sleep(1000);
    
    vm2.invoke(checkNoBucket);
    
    //recreate the region on VM1
    vm1.invoke(createPrRegions);
    
    //Wait for a bit, maybe the region will try to make a copy of the bucket
    Thread.sleep(1000);
    
    vm1.invoke(checkNoBucket);
    vm2.invoke(checkNoBucket);
  }

  public void testDelay() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPrRegions = new SerializableRunnable("createRegions") {
      public void run()
      {
        final CountDownLatch rebalancingFinished = new CountDownLatch(1);
        InternalResourceManager.setResourceObserver(new ResourceObserverAdapter(){
          @Override
          public void rebalancingOrRecoveryFinished(Region region) {
            rebalancingFinished.countDown();
          }
        });
        try {
          Cache cache = getCache();
          AttributesFactory attr = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRecoveryDelay(5000);
          paf.setRedundantCopies(1);
          PartitionAttributes prAttr = paf.create();
          attr.setPartitionAttributes(prAttr);
          cache.createRegion("region1", attr.create());
          if(!rebalancingFinished.await(60000, TimeUnit.MILLISECONDS)) {
            fail("Redundancy recovery did not happen within 60 seconds");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        } finally {
          InternalResourceManager.setResourceObserver(null);
        }
      }
    };
    
    //create the region in 2 VMS
    vm0.invoke(createPrRegions);
    vm1.invoke(createPrRegions);
    
    //Do 1 put, which should create 1 bucket
    vm0.invoke(new SerializableRunnable("putData") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        region1.put("A", "B");
      }
    });
    
    //create the region in a third VM, which won't have any buckets
    vm2.invoke(createPrRegions);

    //close 1 cache, which should make the bucket drop below
    //the expected redundancy level.
    vm1.invoke(new SerializableRunnable("close cache") {
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    });
    
    long elapsed = waitForBucketRecovery(vm2, 1);
    assertTrue("Did not wait at least 5 seconds to create the bucket. Elapsed=" + elapsed, elapsed >= 5000);
  }
  
  public void testStartupDelay() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPrRegions = new SerializableRunnable("createRegions") {
      public void run()
      {
        Cache cache = getCache();
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setStartupRecoveryDelay(5000);
        paf.setRedundantCopies(1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    //create the region in 2 VMS
    vm0.invoke(createPrRegions);
    vm1.invoke(createPrRegions);
    
    //Do 1 put, which should create 1 bucket
    vm0.invoke(new SerializableRunnable("putData") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        region1.put(Integer.valueOf(1), "B");
        region1.put(Integer.valueOf(2), "B");
        region1.put(Integer.valueOf(3), "B");
        region1.put(Integer.valueOf(4), "B");
      }
    });
    

    //close 1 cache, which should make the bucket drop below
    //the expected redundancy level.
    vm1.invoke(new SerializableRunnable("close cache") {
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    });
    
    final long begin = System.currentTimeMillis();
    //create the region in a third VM, which won't have any buckets
    vm2.invoke(createPrRegions);
    long elapsed = System.currentTimeMillis() - begin;
    assertTrue(
        "Create region should not have waited to recover redundancy. Elapsed="
            + elapsed, elapsed < 5000);
    
    //wait for the bucket to be copied
    elapsed = waitForBucketRecovery(vm2, 4);
    assertTrue("Did not wait at least 5 seconds to create the bucket. Elapsed=" + elapsed, elapsed >= 5000);
    
    vm2.invoke(new SerializableCallable("wait for primary move") {

      public Object call() throws Exception {
        Cache cache = getCache();
        MyResourceObserver observer = (MyResourceObserver) InternalResourceManager.getResourceObserver();
        observer.waitForRecovery(30, TimeUnit.SECONDS);
        
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        assertEquals(2,region1.getDataStore().getNumberOfPrimaryBucketsManaged());
        return null;
      }
      
    });
  }
  
  private long waitForBucketRecovery(VM vm2, final int numBuckets) {
    final long begin = System.currentTimeMillis();
    //wait for the bucket to be copied
    Long elapsed = (Long) vm2.invoke(new SerializableCallable("putData") {
      public Object call() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        while(System.currentTimeMillis() - begin < 30000) {
          int bucketsManaged = region1.getDataStore().getBucketsManaged();
          if(bucketsManaged == numBuckets) {
            break;
          } else {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
        assertEquals("Did not start managing the bucket within 30 seconds", numBuckets,
            region1.getDataStore().getBucketsManaged());
        long elapsed = System.currentTimeMillis() - begin;
        return Long.valueOf(elapsed);
      }
    });
    return elapsed.longValue();
  }
  
  private static class MyResourceObserver extends ResourceObserverAdapter {

    CountDownLatch recoveryComplete = new CountDownLatch(1);
    
    public void waitForRecovery(long time, TimeUnit unit) throws InterruptedException {
      recoveryComplete.await(time, unit);
    }
    @Override
    public void rebalancingOrRecoveryFinished(Region region) {
      recoveryComplete.countDown();
      
    }
    
  }
}
