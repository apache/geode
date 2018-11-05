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

import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.persistence.DiskRegionView;

/**
 * A {@code HeapLRUController} controls the contents of {@link Region} based on the percentage of
 * the JVM heap that is currently being used. If the percentage of heap in use exceeds the given
 * percentage, then one or more entries are evicted from the region.
 *
 * <p>
 * For heap regions: We have found that {@code HeapLRUController} is most useful on a JVM that is
 * launched with both the {@code -Xmx} and {@code -Xms} switches used. Many java virtual machine
 * implementations have additional JVM switches to control the behavior of the garbage collector. We
 * suggest that you investigate tuning the garbage collector when using {@code HeapLRUController}.
 * In particular, we have found that when running with Sun's HotSpot JVM, the
 * {@code -XX:+UseConcMarkSweepGC} and {@code -XX:+UseParNewGC} options improve the behavior of
 * {@code HeapLRUController}.
 *
 * @since GemFire 3.2
 */
public class HeapLRUController extends SizeLRUController {
  /**
   * The default percentage of VM heap usage over which LRU eviction occurs
   */
  public static final String TOP_UP_HEAP_EVICTION_PERCENTAGE_PROPERTY =
      GEMFIRE_PREFIX + "topUpHeapEvictionPercentage";

  public HeapLRUController(EvictionCounters evictionCounters, EvictionAction evictionAction,
      ObjectSizer sizer, EvictionAlgorithm algorithm) {
    super(evictionCounters, evictionAction, sizer, algorithm);
  }

  @Override
  public void setLimit(int maximum) {
    // nothing
  }

  @Override
  public long getLimit() {
    return 0;
  }

  /**
   * Returns a brief description of this eviction controller.
   *
   * @since GemFire 4.0
   */
  @Override
  public String toString() {
    return String.format(
        "HeapLRUCapacityController with a capacity of %s%s of memory and eviction action %s.",
        getLimit(), "%", getEvictionAction());
  }

  @Override
  public int entrySize(Object key, Object value) {
    // value is null only after eviction occurs. A change in size is
    // required for eviction stats, bug 30974

    if (value == Token.TOMBSTONE) {
      return 0;
    }

    int size = getPerEntryOverhead();
    size += sizeof(key);
    size += sizeof(value);
    return size;
  }

  /**
   * Okay, deep breath. Instead of basing the LRU calculation on the number of entries in the region
   * or on their "size" (which turned out to be incorrectly estimated in the general case), we use
   * the amount of memory currently in use. If the amount of memory current in use
   * {@linkplain Runtime#maxMemory} - {@linkplain Runtime#freeMemory} is greater than the overflow
   * threshold, then we evict the LRU entry.
   */
  @Override
  public boolean mustEvict(EvictionCounters stats, InternalRegion region, int delta) {
    InternalCache cache = (InternalCache) region.getRegionService();
    boolean offheap = region.getAttributes().getOffHeap();
    boolean shouldEvict =
        cache.getInternalResourceManager().getMemoryMonitor(offheap).getState().isEviction();

    if (region instanceof BucketRegion) {
      return shouldEvict && ((BucketRegion) region).getSizeForEviction() > 0;
    }
    return shouldEvict && ((LocalRegion) region).getRegionMap().sizeInVM() > 0;
  }

  @Override
  public boolean lruLimitExceeded(EvictionCounters stats, DiskRegionView diskRegionView) {
    InternalResourceManager resourceManager =
        diskRegionView.getDiskStore().getCache().getInternalResourceManager();
    return resourceManager.getMemoryMonitor(diskRegionView.getOffHeap()).getState().isEviction();
  }

}
