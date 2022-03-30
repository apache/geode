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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.statistics.LocalStatisticsFactory;

public class OffHeapStorageNonRuntimeStatsJUnitTest {

  @Test
  public void testUpdateNonRealTimeOffHeapStorageStats() {
    StatisticsFactory localStatsFactory = new LocalStatisticsFactory(null);
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    MemoryAllocator ma =
        OffHeapStorage.basicCreateOffHeapStorage(localStatsFactory, 1024 * 1024, ooohml, 100);
    try {
      OffHeapMemoryStats stats = ma.getStats();

      assertThat(stats.getFreeMemory()).isEqualTo(1024 * 1024);
      assertThat(stats.getMaxMemory()).isEqualTo(1024 * 1024);
      assertThat(stats.getUsedMemory()).isEqualTo(0);
      assertThat(stats.getFragments()).isEqualTo(1);
      assertThat(stats.getLargestFragment()).isEqualTo(1024 * 1024);
      assertThat(stats.getFreedChunks()).isEqualTo(0);

      FreeListManager freeListManager = ((MemoryAllocatorImpl) ma).getFreeListManager();
      StoredObject storedObject1 = ma.allocate(10);
      // Release the memory of the object so that the next allocation reuses the freed chunk
      ReferenceCounter.release(storedObject1.getAddress(), freeListManager);
      StoredObject storedObject2 = ma.allocate(10);
      StoredObject storedObject3 = ma.allocate(10);

      assertThat(stats.getFreeMemory()).isLessThan(1024 * 1024);
      assertThat(stats.getUsedMemory()).isGreaterThan(0);
      await().untilAsserted(() -> assertThat(stats.getLargestFragment()).isLessThan(1024 * 1024));
      await().untilAsserted(() -> assertThat(stats.getFreedChunks()).isEqualTo(0));

      ReferenceCounter.release(storedObject3.getAddress(), freeListManager);
      ReferenceCounter.release(storedObject2.getAddress(), freeListManager);

      assertThat(stats.getFreeMemory()).isEqualTo(1024 * 1024);
      assertThat(stats.getUsedMemory()).isEqualTo(0);
      await().untilAsserted(() -> assertThat(stats.getLargestFragment()).isLessThan(1024 * 1024));
      await().untilAsserted(() -> assertThat(stats.getFreedChunks()).isEqualTo(2));
    } finally {
      System.setProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "true");
      try {
        ma.close();
      } finally {
        System.clearProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY);
      }
    }
  }
}
