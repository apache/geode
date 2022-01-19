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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

@SuppressWarnings("synthetic-access")

public class PartitionedRegionDelayedRecoveryDUnitTest extends JUnit4CacheTestCase {

  public PartitionedRegionDelayedRecoveryDUnitTest() {
    super();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        InternalResourceManager.setResourceObserver(null);
      }
    });
    InternalResourceManager.setResourceObserver(null);
  }

  @Test
  public void testNoRecovery() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPrRegions = new SerializableRunnable("createRegions") {
      @Override
      public void run() {
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

    // create the region in 2 VMS
    vm0.invoke(createPrRegions);
    vm1.invoke(createPrRegions);

    // Do 1 put, which should create 1 bucket on both Vms
    vm0.invoke(new SerializableRunnable("putData") {
      @Override
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        region1.put("A", "B");
      }
    });

    // create the PR on another region, which won't have the bucket
    vm2.invoke(createPrRegions);

    // destroy the region in 1 of the VM's that's hosting the bucket
    vm1.invoke(new SerializableRunnable("Destroy region") {
      @Override
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        region1.localDestroyRegion();
      }
    });



    // check to make sure we didn't make a copy of the low redundancy bucket
    SerializableRunnable checkNoBucket = new SerializableRunnable("Check for bucket") {
      @Override
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        assertEquals(0, region1.getDataStore().getBucketsManaged());
      }
    };

    // Wait for a bit, maybe the region will try to make a copy of the bucket
    Thread.sleep(1000);

    vm2.invoke(checkNoBucket);

    // recreate the region on VM1
    vm1.invoke(createPrRegions);

    // Wait for a bit, maybe the region will try to make a copy of the bucket
    Thread.sleep(1000);

    vm1.invoke(checkNoBucket);
    vm2.invoke(checkNoBucket);
  }

  @Test
  public void testDelay() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPrRegions = new SerializableRunnable("createRegions") {
      @Override
      public void run() {
        final CountDownLatch rebalancingFinished = new CountDownLatch(1);
        InternalResourceManager.setResourceObserver(new ResourceObserverAdapter() {
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
          if (!rebalancingFinished.await(60000, TimeUnit.MILLISECONDS)) {
            fail("Redundancy recovery did not happen within 60 seconds");
          }
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        } finally {
          InternalResourceManager.setResourceObserver(null);
        }
      }
    };

    // create the region in 2 VMS
    vm0.invoke(createPrRegions);
    vm1.invoke(createPrRegions);

    // Do 1 put, which should create 1 bucket
    vm0.invoke(new SerializableRunnable("putData") {
      @Override
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        region1.put("A", "B");
      }
    });

    // create the region in a third VM, which won't have any buckets
    vm2.invoke(createPrRegions);

    final long begin = System.currentTimeMillis();

    // close 1 cache, which should make the bucket drop below
    // the expected redundancy level.
    vm1.invoke(new SerializableRunnable("close cache") {
      @Override
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    });

    long elapsed = waitForBucketRecovery(vm2, 1, begin);
    assertTrue("Did not wait at least 5 seconds to create the bucket. Elapsed=" + elapsed,
        elapsed >= 5000);
  }

  @Test
  public void testStartupDelay() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPrRegions = new SerializableRunnable("createRegions") {
      @Override
      public void run() {
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

    // create the region in 2 VMS
    vm0.invoke(createPrRegions);
    vm1.invoke(createPrRegions);

    // Do 1 put, which should create 1 bucket
    vm0.invoke(new SerializableRunnable("putData") {
      @Override
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        region1.put(1, "B");
        region1.put(2, "B");
        region1.put(3, "B");
        region1.put(4, "B");
      }
    });


    // close 1 cache, which should make the bucket drop below
    // the expected redundancy level.
    vm1.invoke(new SerializableRunnable("close cache") {
      @Override
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    });

    final long begin = System.currentTimeMillis();
    // create the region in a third VM, which won't have any buckets
    vm2.invoke(createPrRegions);
    long elapsed = System.currentTimeMillis() - begin;
    assertTrue("Create region should not have waited to recover redundancy. Elapsed=" + elapsed,
        elapsed < 5000);

    // wait for the bucket to be copied
    elapsed = waitForBucketRecovery(vm2, 4, begin);
    assertTrue("Did not wait at least 5 seconds to create the bucket. Elapsed=" + elapsed,
        elapsed >= 5000);

    vm2.invoke(new SerializableCallable("wait for primary move") {

      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        MyResourceObserver observer =
            (MyResourceObserver) InternalResourceManager.getResourceObserver();
        observer.waitForRecovery(30, TimeUnit.SECONDS);

        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        assertEquals(2, region1.getDataStore().getNumberOfPrimaryBucketsManaged());
        return null;
      }

    });
  }

  private long waitForBucketRecovery(VM vm2, final int numBuckets, final long begin) {
    // wait for the bucket to be copied
    Long elapsed = (Long) vm2.invoke(new SerializableCallable("putData") {
      @Override
      public Object call() {
        Cache cache = getCache();
        PartitionedRegion region1 = (PartitionedRegion) cache.getRegion("region1");
        while (System.currentTimeMillis() - begin < 30000) {
          int bucketsManaged = region1.getDataStore().getBucketsManaged();
          if (bucketsManaged == numBuckets) {
            break;
          } else {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) { // TODO: don't catch InterruptedException -- let test
                                               // fail!
              e.printStackTrace();
            }
          }
        }
        assertEquals("Did not start managing the bucket within 30 seconds", numBuckets,
            region1.getDataStore().getBucketsManaged());
        long elapsed = System.currentTimeMillis() - begin;
        return elapsed;
      }
    });
    return elapsed;
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
