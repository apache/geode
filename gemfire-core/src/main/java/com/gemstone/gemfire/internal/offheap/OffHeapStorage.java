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
package com.gemstone.gemfire.internal.offheap;

import java.lang.reflect.Method;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Enables off-heap storage by creating a MemoryAllocator.
 * 
 * @author Darrel Schneider
 * @author Kirk Lund
 * @since 9.0
 */
public class OffHeapStorage implements OffHeapMemoryStats {
  public static final String STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY = "gemfire.offheap.stayConnectedOnOutOfOffHeapMemory";
  
  // statistics type
  private static final StatisticsType statsType;
  private static final String statsTypeName = "OffHeapMemoryStats";
  private static final String statsTypeDescription = "Statistics about off-heap memory storage.";
  
  // statistics instance
  private static final String statsName = "offHeapMemoryStats";
  
  // statistics fields
  private static final int freeMemoryId;
  private static final int maxMemoryId;
  private static final int usedMemoryId;
  private static final int objectsId;
  private static final int readsId;
  private static final int compactionsId;
  private static final int fragmentsId;
  private static final int largestFragmentId;
  private static final int compactionTimeId;
  private static final int fragmentationId;
  // NOTE!!!! When adding new stats make sure and update the initialize method on this class
  
  // creates and registers the statistics type
  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    
    final String usedMemoryDesc = "The amount of off-heap memory, in bytes, that is being used to store data.";
    final String compactionsDesc = "The total number of times off-heap memory has been compacted.";
    final String compactionTimeDesc = "The total time spent compacting off-heap memory.";
    final String fragmentationDesc = "The percentage of off-heap memory fragmentation.  Updated every time a compaction is performed.";
    final String fragmentsDesc = "The number of fragments of free off-heap memory. Updated every time a compaction is done.";
    final String freeMemoryDesc = "The amount of off-heap memory, in bytes, that is not being used.";
    final String largestFragmentDesc = "The largest fragment of memory found by the last compaction of off heap memory. Updated every time a compaction is done.";
    final String objectsDesc = "The number of objects stored in off-heap memory.";
    final String readsDesc = "The total number of reads of off-heap memory. Only reads of a full object increment this statistic. If only a part of the object is read this statistic is not incremented.";
    final String maxMemoryDesc = "The maximum amount of off-heap memory, in bytes. This is the amount of memory allocated at startup and does not change.";

    final String usedMemory = "usedMemory";
    final String compactions = "compactions";
    final String compactionTime = "compactionTime";
    final String fragmentation = "fragmentation";
    final String fragments = "fragments";
    final String freeMemory = "freeMemory";
    final String largestFragment = "largestFragment";
    final String objects = "objects";
    final String reads = "reads";
    final String maxMemory = "maxMemory";
    
    statsType = f.createType(
        statsTypeName,
        statsTypeDescription,
        new StatisticDescriptor[] {
            f.createLongGauge(usedMemory, usedMemoryDesc, "bytes"),
            f.createIntCounter(compactions, compactionsDesc, "compactions"),
            f.createLongCounter(compactionTime, compactionTimeDesc, "nanoseconds", false),
            f.createIntGauge(fragmentation, fragmentationDesc, "percentage"),
            f.createLongGauge(fragments, fragmentsDesc, "fragments"),
            f.createLongGauge(freeMemory, freeMemoryDesc, "bytes"),
            f.createIntGauge(largestFragment, largestFragmentDesc, "bytes"),
            f.createIntGauge(objects, objectsDesc, "objects"),
            f.createLongCounter(reads, readsDesc, "operations"),
            f.createLongGauge(maxMemory, maxMemoryDesc, "bytes"),
        }
    );
    
    usedMemoryId = statsType.nameToId(usedMemory);
    compactionsId = statsType.nameToId(compactions);
    compactionTimeId = statsType.nameToId(compactionTime);
    fragmentationId = statsType.nameToId(fragmentation);
    fragmentsId = statsType.nameToId(fragments);
    freeMemoryId = statsType.nameToId(freeMemory);
    largestFragmentId = statsType.nameToId(largestFragment);
    objectsId = statsType.nameToId(objects);
    readsId = statsType.nameToId(reads);
    maxMemoryId = statsType.nameToId(maxMemory);
  }

  public static long parseOffHeapMemorySize(String value) {
    if (value == null || value.equals("")) {
      return 0;
    }
    final long parsed = parseLongWithUnits(value, 0L, 1024*1024);
    if (parsed < 0) {
      return 0;
    }
    return parsed;
  }
  
  public static long calcMaxSlabSize(long offHeapMemorySize) {
    final String offHeapSlabConfig = System.getProperty("gemfire.OFF_HEAP_SLAB_SIZE");
    long result = 0;
    if (offHeapSlabConfig != null && !offHeapSlabConfig.equals("")) {
      result = parseLongWithUnits(offHeapSlabConfig, MAX_SLAB_SIZE, 1024*1024);
      if (result > offHeapMemorySize) {
        result = offHeapMemorySize;
      }
    } else { // calculate slabSize
      if (offHeapMemorySize < MAX_SLAB_SIZE) {
        // just one slab
        result = offHeapMemorySize;
      } else {
        result = MAX_SLAB_SIZE;
      }
    }
    assert result > 0 && result <= MAX_SLAB_SIZE && result <= offHeapMemorySize;
    return result;
  }
  
  /**
   * Validates that the running VM is compatible with off heap storage.  Throws a
   * {@link CacheException} if incompatible.
   */
  @SuppressWarnings("serial")
  private static void validateVmCompatibility() {
    try {    
      // Do we have the Unsafe class?  Throw ClassNotFoundException if not.
      Class<?> klass = ClassPathLoader.getLatest().forName("sun.misc.Unsafe");
      
      // Okay, we have the class.  Do we have the copyMemory method (not all JVMs support it)?  Throw NoSuchMethodException if not.
      @SuppressWarnings("unused")
      Method copyMemory = klass.getMethod("copyMemory", Object.class,long.class,Object.class,long.class,long.class);
    } catch (ClassNotFoundException e) {
      throw new CacheException(LocalizedStrings.MEMSCALE_JVM_INCOMPATIBLE_WITH_OFF_HEAP.toLocalizedString("product"),e) {};
    } catch (NoSuchMethodException e) {
      throw new CacheException(LocalizedStrings.MEMSCALE_JVM_INCOMPATIBLE_WITH_OFF_HEAP.toLocalizedString("product"),e) {};
    }
  }
  
  /**
   * Constructs a MemoryAllocator for off-heap storage.
   * @return MemoryAllocator for off-heap storage
   */
  public static MemoryAllocator createOffHeapStorage(LogWriter lw, StatisticsFactory sf, long offHeapMemorySize, DistributedSystem system) {
    MemoryAllocator result;
    if (offHeapMemorySize == 0 || Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE)) {
      // Checking the FORCE_LOCATOR_DM_TYPE is a quick hack to keep our locator from allocating off heap memory.
      result = null;
    } else {
      // Ensure that using off-heap will work with this JVM.
      validateVmCompatibility();
      
      final OffHeapMemoryStats stats = new OffHeapStorage(sf);
      
      if (offHeapMemorySize < MIN_SLAB_SIZE) {
        throw new IllegalArgumentException("The amount of off heap memory must be at least " + MIN_SLAB_SIZE + " but it was set to " + offHeapMemorySize);
      }
      
      // determine off-heap and slab sizes
      final long maxSlabSize = calcMaxSlabSize(offHeapMemorySize);
      
      final int slabCount = calcSlabCount(maxSlabSize, offHeapMemorySize);

      if (system == null) {
        throw new IllegalArgumentException("InternalDistributedSystem is null");
      }
      // ooohml provides the hook for disconnecting and closing cache on OutOfOffHeapMemoryException
      OutOfOffHeapMemoryListener ooohml = new DisconnectingOutOfOffHeapMemoryListener((InternalDistributedSystem) system);
      result = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, slabCount, offHeapMemorySize, maxSlabSize);
    }
    return result;
  }
  
  private static final long MAX_SLAB_SIZE = Integer.MAX_VALUE;
  static final long MIN_SLAB_SIZE = 1024;

  // non-private for unit test access
  static int calcSlabCount(long maxSlabSize, long offHeapMemorySize) {
    long result = offHeapMemorySize / maxSlabSize;
    if ((offHeapMemorySize % maxSlabSize) >= MIN_SLAB_SIZE) {
      result++;
    }
    if (result > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("The number of slabs of off heap memory exceeded the limit of " + Integer.MAX_VALUE + ". Decrease the amount of off heap memory or increase the maximum slab size using gemfire.OFF_HEAP_SLAB_SIZE.");
    }
    return (int)result;
  }

  private static long parseLongWithUnits(String v, long defaultValue, int defaultMultiplier) {
    if (v == null || v.equals("")) {
      return defaultValue;
    }
    int unitMultiplier = defaultMultiplier;
    if (v.toLowerCase().endsWith("g")) {
      unitMultiplier = 1024*1024*1024;
      v = v.substring(0, v.length()-1);
    } else if (v.toLowerCase().endsWith("m")) {
      unitMultiplier = 1024*1024;
      v = v.substring(0, v.length()-1);
    }
    try {
      long result = Long.parseLong(v);
      result *= unitMultiplier;
      return result;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Memory size must be specified as <n>[g|m], where <n> is the size and [g|m] specifies the units in gigabytes or megabytes.");
    }
  }
  
  private final Statistics stats;
  
  private OffHeapStorage(StatisticsFactory f) {
    this.stats = f.createAtomicStatistics(statsType, statsName);
  }
  
  public void incFreeMemory(long value) {
    this.stats.incLong(freeMemoryId, value);
  }
  public void incMaxMemory(long value) {
    this.stats.incLong(maxMemoryId, value);
  }
  public void incUsedMemory(long value) {
    this.stats.incLong(usedMemoryId, value);
  }
  public void incObjects(int value) {
    this.stats.incInt(objectsId, value);
  }
  
  public long getFreeMemory() {
    return this.stats.getLong(freeMemoryId);
  }
  public long getMaxMemory() {
    return this.stats.getLong(maxMemoryId);
  }
  public long getUsedMemory() {
    return this.stats.getLong(usedMemoryId);
  }
  public int getObjects() {
    return this.stats.getInt(objectsId);
  }

  @Override
  public void incReads() {
    this.stats.incLong(readsId, 1);
  }

  @Override
  public long getReads() {
    return this.stats.getLong(readsId);
  }

  private void incCompactions() {
    this.stats.incInt(compactionsId, 1);
  }

  @Override
  public int getCompactions() {
    return this.stats.getInt(compactionsId);
  }

  @Override
  public void setFragments(long value) {
    this.stats.setLong(fragmentsId, value);
  }

  @Override
  public long getFragments() {
    return this.stats.getLong(fragmentsId);
  }

  @Override
  public void setLargestFragment(int value) {
    this.stats.setInt(largestFragmentId, value);
  }

  @Override
  public int getLargestFragment() {
    return this.stats.getInt(largestFragmentId);
  }
  
  @Override
  public long startCompaction() {
    return DistributionStats.getStatTime();
  }
  
  @Override
  public void endCompaction(long start) {
    incCompactions();
    if (DistributionStats.enableClockStats) {
      stats.incLong(compactionTimeId, DistributionStats.getStatTime()-start);
    }
  }  
  
  @Override
  public long getCompactionTime() {
    return stats.getLong(compactionTimeId);
  }

  @Override
  public void setFragmentation(int value) {
    this.stats.setInt(fragmentationId, value);
  }

  @Override
  public int getFragmentation() {
    return this.stats.getInt(fragmentationId);
  }
  
  public Statistics getStats() {
    return this.stats;
  }

  @Override
  public void close() {
    this.stats.close();
  }

  @Override
  public void initialize(OffHeapMemoryStats oldStats) {
    setFreeMemory(oldStats.getFreeMemory());
    setMaxMemory(oldStats.getMaxMemory());
    setUsedMemory(oldStats.getUsedMemory());
    setObjects(oldStats.getObjects());
    setReads(oldStats.getReads());
    setCompactions(oldStats.getCompactions());
    setFragments(oldStats.getFragments());
    setLargestFragment(oldStats.getLargestFragment());
    setCompactionTime(oldStats.getCompactionTime());
    setFragmentation(oldStats.getFragmentation());
    
    oldStats.close();
  }

  private void setCompactionTime(long value) {
    stats.setLong(compactionTimeId, value);
  }

  private void setCompactions(int value) {
    this.stats.setInt(compactionsId, value);
  }

  private void setReads(long value) {
    this.stats.setLong(readsId, value);
  }

  private void setObjects(int value) {
    this.stats.setInt(objectsId, value);
  }

  private void setUsedMemory(long value) {
    this.stats.setLong(usedMemoryId, value);
  }

  private void setMaxMemory(long value) {
    this.stats.setLong(maxMemoryId, value);
  }

  private void setFreeMemory(long value) {
    this.stats.setLong(freeMemoryId, value);
  }
  
  static class DisconnectingOutOfOffHeapMemoryListener implements OutOfOffHeapMemoryListener {
    private final Object lock = new Object();
    private InternalDistributedSystem ids;
    
    DisconnectingOutOfOffHeapMemoryListener(InternalDistributedSystem ids) {
      this.ids = ids;
    }
    
    public void close() {
      synchronized (lock) {
        this.ids = null; // set null to prevent memory leak after closure!
      }
    }
    
    @Override
    public void outOfOffHeapMemory(final OutOfOffHeapMemoryException cause) {
      synchronized (lock) {
        if (this.ids == null) {
          return;
        }
        if (Boolean.getBoolean(STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY)) {
          return;
        }
        
        final InternalDistributedSystem dsToDisconnect = this.ids;
        this.ids = null; // set null to prevent memory leak after closure!
        
        if (dsToDisconnect.getDistributionManager().getRootCause() == null) {
          dsToDisconnect.getDistributionManager().setRootCause(cause);
        }
          
        Runnable runnable = new Runnable() {
          @Override
          public void run() {
            dsToDisconnect.getLogWriter().info("OffHeapStorage about to invoke disconnect on " + dsToDisconnect);
            dsToDisconnect.disconnect(cause.getMessage(), cause, false);
          }
        };
        
        // invoking disconnect is async because caller may be a DM pool thread which will block until DM shutdown times out

        //LogWriterImpl.LoggingThreadGroup group = LogWriterImpl.createThreadGroup("MemScale Threads", ids.getLogWriterI18n());
        String name = this.getClass().getSimpleName()+"@"+this.hashCode()+" Handle OutOfOffHeapMemoryException Thread";
        //Thread thread = new Thread(group, runnable, name);
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        thread.start();
      }
    }
  }
}
