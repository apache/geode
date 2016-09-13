/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.lru;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.util.Properties;

/**
 * A <code>HeapLRUCapacityController</code> controls the contents of
 * {@link Region} based on the percentage of memory that is
 * currently being used. If the percentage of memory in use exceeds
 * the given percentage, then the least recently used entry of the region is
 * evicted.
 * 
 * <P>
 * 
 * For heap regions:
 * GemStone has found that the <code>HeapLRUCapacityController</code> has the
 * most effect on a VM that is lauched with both the <code>-Xmx</code> and
 * <code>-Xms</code> switches used. Many virtual machine implementations have
 * additional VM switches to control the behavior of the garbage collector. We
 * suggest that you investigate tuning the garbage collector when using a
 * <code>HeapLRUCapacityController</code>. In particular, we have found that
 * when running with Sun's <A
 * href="http://java.sun.com/docs/hotspot/gc/index.html">HotSpot</a> VM, the
 * <code>-XX:+UseConcMarkSweepGC</code> and <code>-XX:+UseParNewGC</code>
 * options improve the behavior of the <code>HeapLRUCapacityController</code>.
 * 
 * 
 * @since GemFire 3.2
 */
@SuppressWarnings("synthetic-access")
public class HeapLRUCapacityController extends LRUAlgorithm {
  private static final long serialVersionUID = 4970685814429530675L;
  /**
   * The default percentage of VM heap usage over which LRU eviction occurs
   */
  public static final String TOP_UP_HEAP_EVICTION_PERCENTAGE_PROPERTY = DistributionConfig.GEMFIRE_PREFIX + "topUpHeapEvictionPercentage";
  
  public static final float DEFAULT_TOP_UP_HEAP_EVICTION_PERCENTAGE = 4.0f;
  
  public static final int DEFAULT_HEAP_PERCENTAGE = 75;

  public static final int PER_ENTRY_OVERHEAD = 250;

  private int perEntryOverhead = PER_ENTRY_OVERHEAD;


  /**
   * The default number of milliseconds the evictor thread should wait before
   * evicting the LRU entry.
   */
  public static final int DEFAULT_EVICTOR_INTERVAL = 500;

  protected static final StatisticsType statType;

  static {
    // create the stats type for MemLRU.
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    final String entryBytesDesc = "The amount of memory currently used by regions configured for eviction.";
    final String lruEvictionsDesc = "Number of total entry evictions triggered by LRU.";
    final String lruDestroysDesc = "Number of entries destroyed in the region through both destroy cache operations and eviction. Reset to zero each time it exceeds lruDestroysLimit.";
    final String lruDestroysLimitDesc = "Maximum number of entry destroys triggered by LRU before scan occurs.";
    final String lruEvaluationsDesc = "Number of entries evaluated during LRU operations.";
    final String lruGreedyReturnsDesc = "Number of non-LRU entries evicted during LRU operations";

    statType = f
        .createType(
            "HeapLRUStatistics",
            "Statistics about byte based Least Recently Used region entry disposal",
            new StatisticDescriptor[] {
                f.createLongGauge("entryBytes", entryBytesDesc, "bytes"),
                f.createLongCounter("lruEvictions", lruEvictionsDesc, "entries"),
                f.createLongCounter("lruDestroys", lruDestroysDesc, "entries"),
                f.createLongGauge("lruDestroysLimit", lruDestroysLimitDesc, "entries"),
                f.createLongCounter("lruEvaluations", lruEvaluationsDesc, "entries"),
                f.createLongCounter("lruGreedyReturns", lruGreedyReturnsDesc, "entries"), });
  }

  // //////////////////// Instance Fields /////////////////////

  // ////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>HeapLRUCapacityController</code> with the given eviction action.
   * 
   * @param evictionAction
   *          The action that will occur when an entry is evicted
   * 
   */
  public HeapLRUCapacityController(EvictionAction evictionAction, Region region) {
    super(evictionAction, region);
  }

  public HeapLRUCapacityController(ObjectSizer sizerImpl,EvictionAction evictionAction, Region region) {
    super(evictionAction, region);
    setSizer(sizerImpl);
  }
  // ///////////////////// Instance Methods ///////////////////////

  @Override
  public void setLimit(int maximum) {
  }

  // Candidate for removal since capacity controller is no longer part of
  // cache.xml
  @Override
  public Properties getProperties() {
    throw new IllegalStateException("Unused properties");
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public long getLimit() {
    return 0;
  }

  @Override
  public boolean equals(Object cc) {
    if (!super.equals(cc))
      return false;
    return true;
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#hashCode()
   * 
   * Note that we just need to make sure that equal objects return equal
   * hashcodes; nothing really elaborate is done here.
   */
  @Override
  public int hashCode() {
    int result = super.hashCode();
    return result;
  }
  
  /**
   * Returns a brief description of this eviction controller.
   * 
   * @since GemFire 4.0
   */
  @Override
  public String toString() {
    return LocalizedStrings.HeapLRUCapacityController_HEAPLRUCAPACITYCONTROLLER_WITH_A_CAPACITY_OF_0_OF_HEAP_AND_AN_THREAD_INTERVAL_OF_1_AND_EVICTION_ACTION_2.toLocalizedString(new Object[] { Long.valueOf(this.getLimit()), this.getEvictionAction()});
  }

  /**
   * Sets the {@link ObjectSizer} used to calculate the size of
   * objects placed in the cache.
   *
   * @param sizer
   *        The name of the sizer class
   */
  private void setSizer(ObjectSizer sizer) {
    this.sizer = sizer;
  }
  
  @Override
  protected EnableLRU createLRUHelper() {
    return new AbstractEnableLRU() {

      /**
       * Indicate what kind of <code>EvictionAlgorithm</code> this helper
       * implements
       */
      public EvictionAlgorithm getEvictionAlgorithm() {
        return EvictionAlgorithm.LRU_HEAP;
      }

      /**
       * As far as we're concerned all entries have the same size
       */
      public int entrySize(Object key, Object value)
          throws IllegalArgumentException {
        // value is null only after eviction occurs. A change in size is
        // required for eviction stats, bug 30974
        /*
         * if (value != null) { return 1; } else { return 0; }
         */
        if (value == Token.TOMBSTONE) {
          return 0;
        }
        
        int size = HeapLRUCapacityController.this.getPerEntryOverhead();
        size += sizeof(key);
        size += sizeof(value);
        return size;
      }

      /**
       * In addition to initializing the statistics, create an evictor thread to
       * periodically evict the LRU entry.
       */
      @Override
      public LRUStatistics initStats(Object region, StatisticsFactory sf) {
        setRegionName(region);
        final LRUStatistics stats
          = new HeapLRUStatistics(sf, getRegionName(), this);
        setStats(stats);
        return stats;
      }

      public StatisticsType getStatisticsType() {
        return statType;
      }

      public String getStatisticsName() {
        return "HeapLRUStatistics";
      }

      public int getLimitStatId() {
        throw new UnsupportedOperationException("Limit not used with this LRU type");
      }

      public int getCountStatId() {
        return statType.nameToId("entryBytes");
      }

      public int getEvictionsStatId() {
        return statType.nameToId("lruEvictions");
      }

      public int getDestroysStatId() {
        return statType.nameToId("lruDestroys");
      }

      public int getDestroysLimitStatId() {
        return statType.nameToId("lruDestroysLimit");
      }

      public int getEvaluationsStatId() {
        return statType.nameToId("lruEvaluations");
      }

      public int getGreedyReturnsStatId() {
        return statType.nameToId("lruGreedyReturns");
      }
      
      /**
       * Okay, deep breath. Instead of basing the LRU calculation on the number
       * of entries in the region or on their "size" (which turned out to be
       * incorrectly estimated in the general case), we use the amount of
       * memory currently in use. If the amount of memory current in use
       * {@linkplain Runtime#maxMemory max memory} -
       * {@linkplain Runtime#freeMemory free memory} is greater than the
       * overflow threshold, then we evict the LRU entry.
       */
      public boolean mustEvict(LRUStatistics stats, Region region, int delta) {
        final GemFireCacheImpl cache;
        if (region != null) {
          cache = (GemFireCacheImpl) region.getRegionService();
        } else {
          cache = GemFireCacheImpl.getInstance();
        }
        InternalResourceManager resourceManager = cache.getResourceManager();
        
        if (region == null) {
          return resourceManager.getHeapMonitor().getState().isEviction();
        }
        
        final boolean monitorStateIsEviction;
        if (!((AbstractRegion) region).getOffHeap()) {
          monitorStateIsEviction = resourceManager.getHeapMonitor().getState().isEviction();
        } else {
          monitorStateIsEviction = resourceManager.getOffHeapMonitor().getState().isEviction();
        }
        
        if (region instanceof BucketRegion) {
          return monitorStateIsEviction && ((BucketRegion) region).getSizeForEviction() > 0;
        }
        
        return monitorStateIsEviction && ((LocalRegion) region).getRegionMap().sizeInVM() > 0;
      }
    };
  }

  // ////////////////////// Inner Classes ////////////////////////

  private ObjectSizer sizer;

  /**
   * Return the size of an object as stored in GemFire... Typically this is the
   * serialized size in bytes.. This implementation is slow.... Need to add
   * Sizer interface and call it for customer objects.
   */
  protected int sizeof(Object o) throws IllegalArgumentException {
    return MemLRUCapacityController.basicSizeof(o, this.sizer);
  }

  public int getPerEntryOverhead() {
    return perEntryOverhead;
  }

  public void setEntryOverHead(int entryOverHead) {
    this.perEntryOverhead = entryOverHead;
  }

}
