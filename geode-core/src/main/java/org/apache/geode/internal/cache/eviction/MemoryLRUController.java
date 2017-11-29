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

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;


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

  private static final int OVERHEAD_PER_ENTRY = 250;

  private static final long ONE_MEG = 1024L * 1024L;

  protected static final StatisticsType statType;

  static {
    // create the stats type for MemLRU.
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    final String bytesAllowedDesc = "Number of total bytes allowed in this region.";
    final String byteCountDesc = "Number of bytes in region.";
    final String lruEvictionsDesc = "Number of total entry evictions triggered by LRU.";
    final String lruDestroysDesc =
        "Number of entries destroyed in the region through both destroy cache operations and eviction. Reset to zero each time it exceeds lruDestroysLimit.";
    final String lruDestroysLimitDesc =
        "Maximum number of entry destroys triggered by LRU before scan occurs.";
    final String lruEvaluationsDesc = "Number of entries evaluated during LRU operations.";
    final String lruGreedyReturnsDesc = "Number of non-LRU entries evicted during LRU operations";

    statType = f.createType("MemLRUStatistics",
        "Statistics about byte based Least Recently Used region entry disposal",
        new StatisticDescriptor[] {f.createLongGauge("bytesAllowed", bytesAllowedDesc, "bytes"),
            f.createLongGauge("byteCount", byteCountDesc, "bytes"),
            f.createLongCounter("lruEvictions", lruEvictionsDesc, "entries"),
            f.createLongCounter("lruDestroys", lruDestroysDesc, "entries"),
            f.createLongGauge("lruDestroysLimit", lruDestroysLimitDesc, "entries"),
            f.createLongCounter("lruEvaluations", lruEvaluationsDesc, "entries"),
            f.createLongCounter("lruGreedyReturns", lruGreedyReturnsDesc, "entries"),});
  }

  private long limit = (EvictionAttributes.DEFAULT_MEMORY_MAXIMUM) * ONE_MEG;

  private int perEntryOverHead = OVERHEAD_PER_ENTRY;

  private final boolean isOffHeap;

  /**
   * Create an instance of the capacity controller with default settings. The default settings are 0
   * {@code maximum-megabytes} and a default {@code sizer}, requiring either the {@link #init}
   * method to be called, or the {@link #setMaximumMegabytes} method.
   */
  public MemoryLRUController(Region region) {
    this(EvictionAttributes.DEFAULT_MEMORY_MAXIMUM, region);
  }

  /**
   * Create an instance of the capacity controller the given settings.
   *
   * @param megabytes the amount of memory allowed in this region specified in megabytes.<br>
   *        <p>
   *        For a region with {@link org.apache.geode.cache.DataPolicy#PARTITION}, it is overridden
   *        by {@link org.apache.geode.cache.PartitionAttributesFactory#setLocalMaxMemory(int) "
   *        local max memory "} specified for the
   *        {@link org.apache.geode.cache.PartitionAttributes}. It signifies the amount of memory
   *        allowed in the region, collectively for its primary buckets and redundant copies for
   *        this VM. It can be different for the same region in different VMs.
   */
  public MemoryLRUController(int megabytes, Region region) {
    this(megabytes, null /* sizerImpl */, region);
  }

  /**
   * Create an instance of the capacity controller the given settings.
   *
   * @param megabytes the amount of memory allowed in this region specified in megabytes.<br>
   *        <p>
   *        For a region with {@link org.apache.geode.cache.DataPolicy#PARTITION}, it is overridden
   *        by {@link org.apache.geode.cache.PartitionAttributesFactory#setLocalMaxMemory(int) "
   *        local max memory "} specified for the
   *        {@link org.apache.geode.cache.PartitionAttributes}. It signifies the amount of memory
   *        allowed in the region, collectively for its primary buckets and redundant copies for
   *        this VM. It can be different for the same region in different VMs.
   * @param sizerImpl classname of a class that implements ObjectSizer, used to compute object sizes
   *        for MemLRU
   */
  public MemoryLRUController(int megabytes, ObjectSizer sizerImpl, Region region) {
    this(megabytes, sizerImpl, EvictionAction.DEFAULT_EVICTION_ACTION, region, false);
  }

  /**
   * Create an instance of the capacity controller the given settings.
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
  public MemoryLRUController(int megabytes, ObjectSizer sizer, EvictionAction evictionAction,
      Region region, boolean isOffHeap) {
    super(evictionAction, region, sizer);
    this.isOffHeap = isOffHeap;
    setMaximumMegabytes(megabytes);
  }

  /**
   * Reset the maximum allowed limit on memory to use for this region. This change takes effect on
   * next region operation that could increase the region's byte size. If the region is shared, this
   * change is seen by all members in the cluster.
   */
  public void setMaximumMegabytes(int megabytes) {
    if (megabytes <= 0) {
      throw new IllegalArgumentException(
          LocalizedStrings.MemLRUCapacityController_MEMLRUCONTROLLER_LIMIT_MUST_BE_POSTIVE_0
              .toLocalizedString(megabytes));
    }
    this.limit = (megabytes) * ONE_MEG;
    if (bucketRegion != null) {
      bucketRegion.setLimit(this.limit);
    } else if (this.stats != null) {
      this.stats.setLimit(this.limit);
    }
  }

  @Override
  public void setLimit(int maximum) {
    setMaximumMegabytes(maximum);
  }

  /**
   * Sets the the number of bytes of overhead each object occupies in the VM. This value may vary
   * between VM implementations.
   */
  public void setEntryOverHead(int entryOverHead) {
    this.perEntryOverHead = entryOverHead;
  }

  @Override
  public long getLimit() {
    return this.limit;
  }

  /**
   * Indicate what kind of {@code AbstractEvictionController} this helper implements
   */
  @Override
  public EvictionAlgorithm getEvictionAlgorithm() {
    return EvictionAlgorithm.LRU_MEMORY;
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
    if (!MemoryLRUController.this.isOffHeap) {
      size += MemoryLRUController.this.getPerEntryOverhead();
      keySize = sizeof(key);
    }
    int valueSize = sizeof(value);
    size += keySize;
    size += valueSize;
    return size;
  }

  @Override
  public StatisticsType getStatisticsType() {
    return statType;
  }

  @Override
  public String getStatisticsName() {
    return "MemLRUStatistics";
  }

  @Override
  public int getLimitStatId() {
    return statType.nameToId("bytesAllowed");
  }

  @Override
  public int getCountStatId() {
    return statType.nameToId("byteCount");
  }

  @Override
  public int getEvictionsStatId() {
    return statType.nameToId("lruEvictions");
  }

  @Override
  public int getDestroysStatId() {
    return statType.nameToId("lruDestroys");
  }

  @Override
  public int getDestroysLimitStatId() {
    return statType.nameToId("lruDestroysLimit");
  }

  @Override
  public int getEvaluationsStatId() {
    return statType.nameToId("lruEvaluations");
  }

  @Override
  public int getGreedyReturnsStatId() {
    return statType.nameToId("lruGreedyReturns");
  }

  @Override
  public boolean mustEvict(EvictionStatistics stats, InternalRegion region, int delta) {
    return stats.getCounter() + delta > stats.getLimit();
  }

  @Override
  public boolean lruLimitExceeded(EvictionStatistics stats, DiskRegionView diskRegionView) {
    return stats.getCounter() > stats.getLimit();
  }

  public int getPerEntryOverhead() {
    return perEntryOverHead;
  }

  @Override
  public boolean equals(Object cc) {
    if (!super.equals(cc))
      return false;
    MemoryLRUController other = (MemoryLRUController) cc;
    if (this.limit != other.limit)
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result += this.limit;
    return result;
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
