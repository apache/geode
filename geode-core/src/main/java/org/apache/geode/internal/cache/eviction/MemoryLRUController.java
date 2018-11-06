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

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.persistence.DiskRegionView;


/**
 * An {@code EvictionController} that will evict one more entries from a region once the region
 * reaches a certain byte {@linkplain #setMaximumMegabytes capacity}. Capacity is determined by
 * monitoring the size of entries. Capacity is specified in terms of megabytes. GemFire uses an
 * efficient algorithm to determine the amount of space a region entry occupies in the JVM. However,
 * this algorithm may not yield optimal results for all kinds of data. The user may provide their
 * own algorithm for determining the size of objects by implementing an {@link ObjectSizer}.
 *
 * @since GemFire 2.0.2
 */
public class MemoryLRUController extends SizeLRUController {

  private long limit;
  private final boolean isOffHeap;

  private static final long ONE_MEG = 1024L * 1024L;

  /**
   * Create an instance of the capacity controller the given settings.
   *
   *
   * @param megabytes the amount of memory allowed in this region specified in megabytes.<br>
   *        <p>
   *        For a region with {@link org.apache.geode.cache.DataPolicy#PARTITION}, it is overridden
   *        by {@link org.apache.geode.cache.PartitionAttributesFactory#setLocalMaxMemory(int) "
   *        local max memory "} specified for the
   *        {@link org.apache.geode.cache.PartitionAttributes}. It signifies the amount of memory
   *        allowed in the region, collectively for its primary buckets and redundant copies for
   *        this VM. It can be different for the same region in different VMs.
   * @param sizer classname of a class that implements ObjectSizer, used to compute object sizes for
   *        MemLRU
   * @param isOffHeap true if the region that owns this cc is stored off heap
   */
  public MemoryLRUController(EvictionCounters evictionCounters, int megabytes, ObjectSizer sizer,
      EvictionAction evictionAction, boolean isOffHeap, EvictionAlgorithm algorithm) {
    super(evictionCounters, evictionAction, sizer, algorithm);
    this.isOffHeap = isOffHeap;
    setMaximumMegabytes(megabytes);
  }

  /**
   * Reset the maximum allowed limit on memory to use for this region. This change takes effect on
   * next region operation that could increase the region's byte size. If the region is shared, this
   * change is seen by all members in the cluster.
   */
  private void setMaximumMegabytes(int megabytes) {
    if (megabytes <= 0) {
      throw new IllegalArgumentException(
          String.format("MemLRUController limit must be postive: %s",
              megabytes));
    }
    this.limit = megabytes * ONE_MEG;
    getCounters().setLimit(this.limit);
  }

  @Override
  public void setLimit(int maximum) {
    setMaximumMegabytes(maximum);
  }

  @Override
  public long getLimit() {
    return this.limit;
  }

  /**
   * compute the size of storing a key/value pair in the cache..
   */
  @Override
  public int entrySize(Object key, Object value) {

    if (value == Token.TOMBSTONE) {
      return 0;
    }

    int size = 0;
    int keySize = 0;
    if (!this.isOffHeap) {
      size += getPerEntryOverhead();
      keySize = sizeof(key);
    }
    int valueSize = sizeof(value);
    size += keySize;
    size += valueSize;
    return size;
  }

  @Override
  public boolean mustEvict(EvictionCounters counters, InternalRegion region, int delta) {
    return counters.getCounter() + delta > counters.getLimit();
  }

  @Override
  public boolean lruLimitExceeded(EvictionCounters counters, DiskRegionView diskRegionView) {
    return counters.getCounter() > counters.getLimit();
  }

  /**
   * Returns a brief description of this capacity controller.
   *
   * @since GemFire 4.0
   */
  @Override
  public String toString() {
    return "MemLRUCapacityController with a capacity of " + this.getLimit()
        + " megabytes and and eviction action " + this.getEvictionAction();
  }
}
