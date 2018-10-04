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
package org.apache.geode.internal.offheap;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.junit.categories.OffHeapTest;

/**
 * Test to reproduce GEODE-2097.
 */
@Category({OffHeapTest.class})
public class OffHeapLRURecoveryRegressionTest {

  static final String DS_NAME = "OffHeapLRURecoveryRegressionTestDS";

  /**
   * Test populates an offheap heaplru persistent region that contains more data than can fit in
   * offheap memory. It then recovers the region to demonstrate that recovering this data will not
   * try to put everything into offheap but instead leave some of it on disk.
   */
  @Test
  public void recoveringTooMuchDataDoesNotRunOutOfOffHeapMemory() {
    final int ENTRY_COUNT = 40;
    GemFireCacheImpl gfc = createCache();
    try {
      Region<Object, Object> r = createRegion(gfc);
      byte[] v = new byte[1024 * 1024];
      for (int i = 0; i < ENTRY_COUNT; i++) {
        r.put(i, v);
      }
    } finally {
      closeCache(gfc);
    }
    await().until(() -> {
      return MemoryAllocatorImpl.getAllocator().getStats().getObjects() == 0;
    });
    System.setProperty("gemfire.disk.recoverValuesSync", "true");
    System.setProperty("gemfire.disk.recoverLruValues", "true");
    try {
      gfc = createCache();
      try {
        createDiskStore(gfc);
        try {
          int offHeapObjects = MemoryAllocatorImpl.getAllocator().getStats().getObjects();
          if (offHeapObjects < 10) {
            fail("expected at least 10 offheap values to be recovered but only did "
                + offHeapObjects);
          }
          if (offHeapObjects == ENTRY_COUNT) {
            // Why "== ENTRY_COUNT" instead of "== 10"?
            // As we recover values we asynchronously notify the resource manager
            // of how much off-heap memory was consumed. Once it sees we are over
            // the LRU limit then recovery of values will stop happening.
            // Since it is async it can allow us to exceed the LRU limit.
            // So far this test usually sees 10 but sometimes sees 11.
            // I allow up to ENTRY_COUNT-1 just to prevent intermittent test failures.
            // We should consider changing this async notification to be sync so
            // that the limit can not be exceeded.
            fail("expected at most " + (ENTRY_COUNT - 1)
                + " offheap values to be recovered but actually did " + offHeapObjects);
          }
        } finally {
          DiskStore ds = gfc.findDiskStore(DS_NAME);
          ds.destroy();
        }
      } finally {
        closeCache(gfc);
      }
    } finally {
      System.clearProperty("gemfire.disk.recoverValuesSync");
      System.clearProperty("gemfire.disk.recoverLruValues");
    }
  }

  private GemFireCacheImpl createCache() {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, "20m");
    GemFireCacheImpl result = (GemFireCacheImpl) new CacheFactory(props).create();
    result.getResourceManager().setEvictionOffHeapPercentage(50.0f);
    return result;
  }

  private void createDiskStore(GemFireCacheImpl gfc) {
    DiskStoreFactory dsf = gfc.createDiskStoreFactory();
    dsf.create(DS_NAME);
  }

  private Region<Object, Object> createRegion(GemFireCacheImpl gfc) {
    createDiskStore(gfc);
    RegionFactory<Object, Object> rf =
        gfc.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT_OVERFLOW);
    rf.setOffHeap(true);
    rf.setDiskStoreName(DS_NAME);
    Region<Object, Object> r = rf.create("OffHeapLRURecoveryRegressionTestRegion");
    return r;
  }

  private void closeCache(GemFireCacheImpl gfc) {
    gfc.close();
  }
}
