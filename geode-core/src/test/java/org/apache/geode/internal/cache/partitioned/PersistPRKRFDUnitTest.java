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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.Properties;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;

/**
 * Tests the basic use cases for PR persistence.
 *
 */
@Category(DistributedTest.class)
public class PersistPRKRFDUnitTest extends PersistentPartitionedRegionTestBase {
  private static final int NUM_BUCKETS = 15;
  private static final int MAX_WAIT = 30 * 1000;
  static Object lockObject = new Object();

  /**
   * do a put/modify/destroy while closing disk store
   * 
   * to turn on debug, add following parameter in local.conf: hydra.VmPrms-extraVMArgs +=
   * "-Ddisk.KRF_DEBUG=true";
   */
  @Test
  public void testCloseDiskStoreWhenPut() {
    final String title = "testCloseDiskStoreWhenPut:";
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    createPR(vm0, 0);
    createData(vm0, 0, 10, "a");
    vm0.invoke(new CacheSerializableRunnable(title + "server add writer") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        // let the region to hold on the put until diskstore is closed
        if (!DiskStoreImpl.KRF_DEBUG) {
          region.getAttributesMutator().setCacheWriter(new MyWriter());
        }
      }
    });

    // create test
    AsyncInvocation async1 = vm0.invokeAsync(new CacheSerializableRunnable(title + "async create") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        IgnoredException expect = IgnoredException.addIgnoredException("CacheClosedException");
        try {
          region.put(10, "b");
          fail("Expect CacheClosedException here");
        } catch (CacheClosedException cce) {
          System.out.println(title + cce.getMessage());
          if (DiskStoreImpl.KRF_DEBUG) {
            assert cce.getMessage().contains("The disk store is closed.");
          } else {
            assert cce.getMessage().contains("The disk store is closed");
          }
        } finally {
          expect.remove();
        }
      }
    });
    vm0.invoke(new CacheSerializableRunnable(title + "close disk store") {
      public void run2() throws CacheException {
        GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
        Wait.pause(500);
        gfc.closeDiskStores();
        synchronized (lockObject) {
          lockObject.notify();
        }
      }
    });
    ThreadUtils.join(async1, MAX_WAIT);
    closeCache(vm0);

    // update
    createPR(vm0, 0);
    vm0.invoke(new CacheSerializableRunnable(title + "server add writer") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        // let the region to hold on the put until diskstore is closed
        if (!DiskStoreImpl.KRF_DEBUG) {
          region.getAttributesMutator().setCacheWriter(new MyWriter());
        }
      }
    });
    async1 = vm0.invokeAsync(new CacheSerializableRunnable(title + "async update") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        IgnoredException expect = IgnoredException.addIgnoredException("CacheClosedException");
        try {
          region.put(1, "b");
          fail("Expect CacheClosedException here");
        } catch (CacheClosedException cce) {
          System.out.println(title + cce.getMessage());
          if (DiskStoreImpl.KRF_DEBUG) {
            assert cce.getMessage().contains("The disk store is closed.");
          } else {
            assert cce.getMessage().contains("The disk store is closed");
          }
        } finally {
          expect.remove();
        }
      }
    });
    vm0.invoke(new CacheSerializableRunnable(title + "close disk store") {
      public void run2() throws CacheException {
        GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
        Wait.pause(500);
        gfc.closeDiskStores();
        synchronized (lockObject) {
          lockObject.notify();
        }
      }
    });
    ThreadUtils.join(async1, MAX_WAIT);
    closeCache(vm0);

    // destroy
    createPR(vm0, 0);
    vm0.invoke(new CacheSerializableRunnable(title + "server add writer") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        // let the region to hold on the put until diskstore is closed
        if (!DiskStoreImpl.KRF_DEBUG) {
          region.getAttributesMutator().setCacheWriter(new MyWriter());
        }
      }
    });
    async1 = vm0.invokeAsync(new CacheSerializableRunnable(title + "async destroy") {
      public void run2() throws CacheException {
        Region region = getRootRegion(PR_REGION_NAME);
        IgnoredException expect = IgnoredException.addIgnoredException("CacheClosedException");
        try {
          region.destroy(2, "b");
          fail("Expect CacheClosedException here");
        } catch (CacheClosedException cce) {
          System.out.println(title + cce.getMessage());
          if (DiskStoreImpl.KRF_DEBUG) {
            assert cce.getMessage().contains("The disk store is closed.");
          } else {
            assert cce.getMessage().contains("The disk store is closed");
          }
        } finally {
          expect.remove();
        }
      }
    });
    vm0.invoke(new CacheSerializableRunnable(title + "close disk store") {
      public void run2() throws CacheException {
        GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
        Wait.pause(500);
        gfc.closeDiskStores();
        synchronized (lockObject) {
          lockObject.notify();
        }
      }
    });
    ThreadUtils.join(async1, MAX_WAIT);

    checkData(vm0, 0, 10, "a");
    checkData(vm0, 10, 11, null);
    closeCache(vm0);
  }

  private static class MyWriter extends CacheWriterAdapter implements Declarable {
    public MyWriter() {}

    public void init(Properties props) {}

    public void beforeCreate(EntryEvent event) {
      try {
        synchronized (lockObject) {
          lockObject.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    public void beforeUpdate(EntryEvent event) {
      try {
        synchronized (lockObject) {
          lockObject.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    public void beforeDestroy(EntryEvent event) {
      try {
        synchronized (lockObject) {
          lockObject.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
