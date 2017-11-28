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

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

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

  private static final int PER_ENTRY_OVERHEAD = 250;

  private int perEntryOverhead = PER_ENTRY_OVERHEAD;

  private static final StatisticsType statType;

  static {
    // create the stats type for MemLRU.
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    final String entryBytesDesc =
        "The amount of memory currently used by regions configured for eviction.";
    final String lruEvictionsDesc = "Number of total entry evictions triggered by LRU.";
    final String lruDestroysDesc =
        "Number of entries destroyed in the region through both destroy cache operations and eviction. Reset to zero each time it exceeds lruDestroysLimit.";
    final String lruDestroysLimitDesc =
        "Maximum number of entry destroys triggered by LRU before scan occurs.";
    final String lruEvaluationsDesc = "Number of entries evaluated during LRU operations.";
    final String lruGreedyReturnsDesc = "Number of non-LRU entries evicted during LRU operations";

    statType = f.createType("HeapLRUStatistics",
        "Statistics about byte based Least Recently Used region entry disposal",
        new StatisticDescriptor[] {f.createLongGauge("entryBytes", entryBytesDesc, "bytes"),
            f.createLongCounter("lruEvictions", lruEvictionsDesc, "entries"),
            f.createLongCounter("lruDestroys", lruDestroysDesc, "entries"),
            f.createLongGauge("lruDestroysLimit", lruDestroysLimitDesc, "entries"),
            f.createLongCounter("lruEvaluations", lruEvaluationsDesc, "entries"),
            f.createLongCounter("lruGreedyReturns", lruGreedyReturnsDesc, "entries"),});
  }

  public HeapLRUController(EvictionAction evictionAction, Region region, ObjectSizer sizer) {
    super(evictionAction, region, sizer);
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
    return LocalizedStrings.HeapLRUCapacityController_HEAPLRUCAPACITYCONTROLLER_WITH_A_CAPACITY_OF_0_OF_HEAP_AND_AN_THREAD_INTERVAL_OF_1_AND_EVICTION_ACTION_2
        .toLocalizedString(getLimit(), getEvictionAction());
  }

  /**
   * Indicate what kind of {@code AbstractEvictionController} this helper implements
   */
  @Override
  public org.apache.geode.cache.EvictionAlgorithm getEvictionAlgorithm() {
    return org.apache.geode.cache.EvictionAlgorithm.LRU_HEAP;
  }

  /**
   * As far as we're concerned all entries have the same size
   */
  @Override
  public int entrySize(Object key, Object value) {
    // value is null only after eviction occurs. A change in size is
    // required for eviction stats, bug 30974

    if (value == Token.TOMBSTONE) {
      return 0;
    }

    int size = HeapLRUController.this.getPerEntryOverhead();
    size += sizeof(key);
    size += sizeof(value);
    return size;
  }

  @Override
  public EvictionStatistics initStats(Object region, StatisticsFactory statsFactory) {
    setRegionName(region);
    InternalEvictionStatistics stats =
        new EvictionStatisticsImpl(statsFactory, getRegionName(), this);
    setStatistics(stats);
    return stats;
  }

  @Override
  public StatisticsType getStatisticsType() {
    return statType;
  }

  @Override
  public String getStatisticsName() {
    return "HeapLRUStatistics";
  }

  @Override
  public int getLimitStatId() {
    throw new UnsupportedOperationException("Limit not used with this LRU type");
  }

  @Override
  public int getCountStatId() {
    return statType.nameToId("entryBytes");
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

  /**
   * Okay, deep breath. Instead of basing the LRU calculation on the number of entries in the region
   * or on their "size" (which turned out to be incorrectly estimated in the general case), we use
   * the amount of memory currently in use. If the amount of memory current in use
   * {@linkplain Runtime#maxMemory} - {@linkplain Runtime#freeMemory} is greater than the overflow
   * threshold, then we evict the LRU entry.
   */
  @Override
  public boolean mustEvict(EvictionStatistics stats, InternalRegion region, int delta) {
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
  public boolean lruLimitExceeded(EvictionStatistics stats, DiskRegionView diskRegionView) {
    InternalResourceManager resourceManager =
        diskRegionView.getDiskStore().getCache().getInternalResourceManager();
    return resourceManager.getMemoryMonitor(diskRegionView.getOffHeap()).getState().isEviction();
  }


  public int getPerEntryOverhead() {
    return perEntryOverhead;
  }

  public void setEntryOverHead(int entryOverHead) {
    this.perEntryOverhead = entryOverHead;
  }
}
