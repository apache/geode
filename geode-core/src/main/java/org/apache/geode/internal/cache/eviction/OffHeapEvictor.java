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
package org.apache.geode.internal.cache.eviction;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.offheap.MemoryAllocator;

/**
 * Triggers centralized eviction(asynchronously) when the ResourceManager sends an eviction event
 * for off-heap regions. This is registered with the ResourceManager.
 *
 * @since Geode 1.0
 */
public class OffHeapEvictor extends HeapEvictor {

  private static final String EVICTOR_THREAD_NAME = "OffHeapEvictorThread";

  private long bytesToEvictWithEachBurst;

  public OffHeapEvictor(final InternalCache cache) {
    super(cache, EVICTOR_THREAD_NAME);
    calculateEvictionBurst();
  }

  private void calculateEvictionBurst() {
    MemoryAllocator allocator = cache().getOffHeapStore();

    /*
     * Bail if there is no off-heap memory to evict.
     */
    if (null == allocator) {
      throw new IllegalStateException(
          "Cannot initialize the off-heap evictor.  There is no off-heap memory available for eviction.");
    }

    float evictionBurstPercentage = Float.parseFloat(System.getProperty(
        DistributionConfig.GEMFIRE_PREFIX + "HeapLRUCapacityController.evictionBurstPercentage",
        "0.4"));
    bytesToEvictWithEachBurst =
        (long) (allocator.getTotalMemory() * 0.01 * evictionBurstPercentage);
  }

  @Override
  protected int getEvictionLoopDelayTime() {
    if (numEvictionLoopsCompleted() < Math.max(3, numFastLoops())) {
      return 250;
    }

    return 1000;
  }

  @Override
  protected boolean includePartitionedRegion(PartitionedRegion region) {
    return region.getEvictionAttributes().getAlgorithm().isLRUHeap()
        && region.getDataStore() != null && region.getAttributes().getOffHeap();
  }

  @Override
  protected boolean includeLocalRegion(LocalRegion region) {
    return region.getEvictionAttributes().getAlgorithm().isLRUHeap()
        && region.getAttributes().getOffHeap();
  }

  @Override
  public long getTotalBytesToEvict() {
    return bytesToEvictWithEachBurst;
  }

  @Override
  protected ResourceType getResourceType() {
    return ResourceType.OFFHEAP_MEMORY;
  }
}
