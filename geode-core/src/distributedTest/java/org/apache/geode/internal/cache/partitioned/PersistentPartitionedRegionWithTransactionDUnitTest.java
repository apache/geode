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
package org.apache.geode.internal.cache.partitioned;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.RegionsTest;

/**
 * Tests the basic use cases for PR persistence.
 *
 */
@Category({RegionsTest.class})
public class PersistentPartitionedRegionWithTransactionDUnitTest
    extends PersistentPartitionedRegionTestBase {

  private static final long MAX_WAIT = 0;

  public PersistentPartitionedRegionWithTransactionDUnitTest() {
    super();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = false;
        System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "false");
      }
    });
  }

  @Override
  protected final void postSetUpPersistentPartitionedRegionTestBase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {

      public void run() {
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = true;
        System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "true");
      }
    });
  }

  @Test
  public void testRollback() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int redundancy = 1;

    int numBuckets = 50;

    createPR(vm0, redundancy);
    createPR(vm1, redundancy);
    createPR(vm2, redundancy);

    createData(vm0, 0, numBuckets, "a");

    createDataWithRollback(vm0, 0, numBuckets, "b");

    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);

    AsyncInvocation a1 = createPRAsync(vm0, redundancy);
    AsyncInvocation a2 = createPRAsync(vm1, redundancy);
    AsyncInvocation a3 = createPRAsync(vm2, redundancy);

    a1.getResult(MAX_WAIT);
    a2.getResult(MAX_WAIT);
    a3.getResult(MAX_WAIT);


    checkData(vm0, 0, numBuckets, "a");
    checkData(vm1, 0, numBuckets, "a");
    checkData(vm2, 0, numBuckets, "a");

    createData(vm0, 0, numBuckets, "b");

    checkData(vm0, 0, numBuckets, "b");
    checkData(vm1, 0, numBuckets, "b");
    checkData(vm2, 0, numBuckets, "b");
  }

  private void createDataWithRollback(VM vm, final int startKey, final int endKey,
      final String value) {
    SerializableRunnable createData = new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();

        CacheTransactionManager tx = cache.getCacheTransactionManager();
        Region region = cache.getRegion(getPartitionedRegionName());

        for (int i = startKey; i < endKey; i++) {
          tx.begin();
          region.put(i, value);
          region.destroy(i + 113, value);
          region.invalidate(i + 113 * 2, value);
          tx.rollback();
        }
      }
    };
    vm.invoke(createData);

  }

  @Override
  protected void createData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    LogWriterUtils.getLogWriter().info("creating runnable to create data for region " + regionName);
    SerializableRunnable createData = new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();
        LogWriterUtils.getLogWriter().info("getting region " + regionName);
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
          CacheTransactionManager tx = cache.getCacheTransactionManager();
          tx.begin();
          region.put(i, value);
          region.put(i + 113, value);
          region.put(i + 113 * 2, value);
          tx.commit();
        }
        { // add a destroy to make sure bug 43063 is fixed
          CacheTransactionManager tx = cache.getCacheTransactionManager();
          tx.begin();
          region.put(endKey + 113 * 3, value);
          tx.commit();
          tx.begin();
          region.remove(endKey + 113 * 3);
          tx.commit();
        }
      }
    };
    vm.invoke(createData);
  }

  @Override
  protected void checkData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable checkData = new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();
        LogWriterUtils.getLogWriter().info("checking data in " + regionName);
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
          assertEquals(value, region.get(i));
          assertEquals(value, region.get(i + 113));
          assertEquals(value, region.get(i + 113 * 2));
        }
        assertEquals(null, region.get(endKey + 113 * 3));
      }
    };

    vm.invoke(checkData);
  }

  void checkRecoveredFromDisk(VM vm, final int bucketId, final boolean recoveredLocally) {
    vm.invoke(new SerializableRunnable("check recovered from disk") {
      @Override
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(getPartitionedRegionName());
        DiskRegion disk = region.getRegionAdvisor().getBucket(bucketId).getDiskRegion();
        if (recoveredLocally) {
          assertEquals(0, disk.getStats().getRemoteInitializations());
          assertEquals(1, disk.getStats().getLocalInitializations());
        } else {
          assertEquals(1, disk.getStats().getRemoteInitializations());
          assertEquals(0, disk.getStats().getLocalInitializations());
        }
      }
    });
  }

  @Test
  public void NoDestroyInvocationIfCreateEntryAndDestroyItInTransaction() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int redundancy = 2;

    vm0.invoke(() -> getCacheSetAlwaysFireLocalListeners());
    vm1.invoke(() -> getCacheSetAlwaysFireLocalListeners());
    vm2.invoke(() -> getCacheSetAlwaysFireLocalListeners());

    createPR(vm0, redundancy);
    createPR(vm1, redundancy);
    createPR(vm2, redundancy);

    vm0.invoke(() -> addListener());
    vm1.invoke(() -> addListener());
    vm2.invoke(() -> addListener());

    vm0.invoke(() -> {
      Cache cache = getCache();
      TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
      Region region = cache.getRegion(getPartitionedRegionName());
      txManager.begin();
      region.create(1, "toBeDestroyed");
      region.destroy(1);
      txManager.commit();
    });

    vm0.invoke(() -> verifyNoDestroyInvocation());
    vm1.invoke(() -> verifyNoDestroyInvocation());
    vm2.invoke(() -> verifyNoDestroyInvocation());
  }

  private void getCacheSetAlwaysFireLocalListeners() {
    System.setProperty("gemfire.BucketRegion.alwaysFireLocalListeners", "true");
    getCache();
  }

  private void addListener() {
    Cache cache = getCache();
    Region region = cache.getRegion(getPartitionedRegionName());
    CacheListener listener = spy(new CacheListenerAdapter() {});
    region.getAttributesMutator().addCacheListener(listener);
  }

  private void verifyNoDestroyInvocation() throws Exception {
    Cache cache = getCache();
    Region region = cache.getRegion(getPartitionedRegionName());
    CacheListener listener = region.getAttributes().getCacheListeners()[0];
    verify(listener, never()).afterDestroy(any());
  }
}
