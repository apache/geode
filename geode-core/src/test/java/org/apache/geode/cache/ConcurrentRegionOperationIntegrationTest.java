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
package org.apache.geode.cache;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;
import org.apache.geode.internal.util.concurrent.ConcurrentMapWithReusableEntries;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class ConcurrentRegionOperationIntegrationTest {

  private Cache cache;
  private MemoryAllocator offHeapStore;

  @Before
  public void createCache() {
    cache = new CacheFactory().set(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, "100m").create();
    offHeapStore = MemoryAllocatorImpl.getAllocator();

    assertEquals(0, offHeapStore.getStats().getObjects());
  }

  @After
  public void closeCache() {
    cache.close();
  }

  @Test
  @Ignore("GEODE-5292: Test is failing in CI")
  public void replaceWithClearAndDestroy() throws RegionClearedException {
    Region<Integer, String> region = createRegion();

    region.put(1, "value");
    region.put(2, "value");

    ConcurrentMapWithReusableEntries<Object, Object> underlyingMap =
        ((LocalRegion) region).getRegionMap().getCustomEntryConcurrentHashMap();
    RegionEntry spyEntry = spy((RegionEntry) underlyingMap.get(1));
    underlyingMap.remove(1);
    underlyingMap.put(1, spyEntry);

    // we want to have spies that cause a clear and destroy in the middle of `region.replace()`
    doAnswer(invocation -> {
      // Execute the clear in a separate thread and wait for it to finish.
      // We are trying to test the case where clear happens concurrently with replace.
      // If we invoke clear in the replace thread, it can get locks which it will not
      // be able to get in a separate thread.
      CompletableFuture.runAsync(region::clear).get();
      throw new RegionDestroyedException("Fake Exception", "/region");
    }).when(spyEntry).setValueWithTombstoneCheck(any(), any());

    assertThatExceptionOfType(RegionDestroyedException.class)
        .isThrownBy(() -> region.replace(1, "value", "newvalue"))
        .withMessageContaining("Fake Exception");

    Awaitility.await().pollDelay(0, TimeUnit.MICROSECONDS)
        .pollInterval(1, TimeUnit.MILLISECONDS)
        .atMost(10, TimeUnit.SECONDS).until(() -> {
          assertEquals(0, offHeapStore.getStats().getObjects());
        });
  }

  private Region<Integer, String> createRegion() {
    return cache.<Integer, String>createRegionFactory()
        .setDataPolicy(DataPolicy.PRELOADED)
        .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
        .setOffHeap(true)
        .setDiskSynchronous(false)
        .create("region");
  }
}
