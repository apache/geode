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

import java.lang.reflect.Method;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * Enables off-heap storage by creating a MemoryAllocator.
 *
 * @since Geode 1.0
 */
public class OffHeapStorage implements OffHeapMemoryStats {
  public static final String STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "offheap.stayConnectedOnOutOfOffHeapMemory";

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
  private static final int defragmentationId;
  private static final int fragmentsId;
  private static final int largestFragmentId;
  private static final int defragmentationTimeId;
  private static final int fragmentationId;
  private static final int defragmentationsInProgressId;
  // NOTE!!!! When adding new stats make sure and update the initialize method on this class

  // creates and registers the statistics type
  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    final String usedMemoryDesc =
        "The amount of off-heap memory, in bytes, that is being used to store data.";
    final String defragmentationDesc =
        "The total number of times off-heap memory has been defragmented.";
    final String defragmentationsInProgressDesc =
        "Current number of defragment operations currently in progress.";
    final String defragmentationTimeDesc = "The total time spent defragmenting off-heap memory.";
    final String fragmentationDesc =
        "The percentage of off-heap free memory that is fragmented.  Updated every time a defragmentation is performed.";
    final String fragmentsDesc =
        "The number of fragments of free off-heap memory. Updated every time a defragmentation is done.";
    final String freeMemoryDesc =
        "The amount of off-heap memory, in bytes, that is not being used.";
    final String largestFragmentDesc =
        "The largest fragment of memory found by the last defragmentation of off heap memory. Updated every time a defragmentation is done.";
    final String objectsDesc = "The number of objects stored in off-heap memory.";
    final String readsDesc =
        "The total number of reads of off-heap memory. Only reads of a full object increment this statistic. If only a part of the object is read this statistic is not incremented.";
    final String maxMemoryDesc =
        "The maximum amount of off-heap memory, in bytes. This is the amount of memory allocated at startup and does not change.";

    final String usedMemory = "usedMemory";
    final String defragmentations = "defragmentations";
    final String defragmentationsInProgress = "defragmentationsInProgress";
    final String defragmentationTime = "defragmentationTime";
    final String fragmentation = "fragmentation";
    final String fragments = "fragments";
    final String freeMemory = "freeMemory";
    final String largestFragment = "largestFragment";
    final String objects = "objects";
    final String reads = "reads";
    final String maxMemory = "maxMemory";

    statsType = f.createType(statsTypeName, statsTypeDescription,
        new StatisticDescriptor[] {f.createLongGauge(usedMemory, usedMemoryDesc, "bytes"),
            f.createIntCounter(defragmentations, defragmentationDesc, "operations"),
            f.createIntGauge(defragmentationsInProgress, defragmentationsInProgressDesc,
                "operations"),
            f.createLongCounter(defragmentationTime, defragmentationTimeDesc, "nanoseconds", false),
            f.createIntGauge(fragmentation, fragmentationDesc, "percentage"),
            f.createLongGauge(fragments, fragmentsDesc, "fragments"),
            f.createLongGauge(freeMemory, freeMemoryDesc, "bytes"),
            f.createIntGauge(largestFragment, largestFragmentDesc, "bytes"),
            f.createIntGauge(objects, objectsDesc, "objects"),
            f.createLongCounter(reads, readsDesc, "operations"),
            f.createLongGauge(maxMemory, maxMemoryDesc, "bytes"),});

    usedMemoryId = statsType.nameToId(usedMemory);
    defragmentationId = statsType.nameToId(defragmentations);
    defragmentationsInProgressId = statsType.nameToId(defragmentationsInProgress);
    defragmentationTimeId = statsType.nameToId(defragmentationTime);
    fragmentationId = statsType.nameToId(fragmentation);
    fragmentsId = statsType.nameToId(fragments);
    freeMemoryId = statsType.nameToId(freeMemory);
    largestFragmentId = statsType.nameToId(largestFragment);
    objectsId = statsType.nameToId(objects);
    readsId = statsType.nameToId(reads);
    maxMemoryId = statsType.nameToId(maxMemory);
  }

  public static long parseOffHeapMemorySize(String value) {
    final long parsed = parseLongWithUnits(value, 0L, 1024 * 1024);
    if (parsed < 0) {
      return 0;
    }
    return parsed;
  }

  public static long calcMaxSlabSize(long offHeapMemorySize) {
    final String offHeapSlabConfig =
        System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "OFF_HEAP_SLAB_SIZE");
    long result = 0;
    if (offHeapSlabConfig != null && !offHeapSlabConfig.equals("")) {
      result = parseLongWithUnits(offHeapSlabConfig, MAX_SLAB_SIZE, 1024 * 1024);
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
   * Validates that the running VM is compatible with off heap storage. Throws a
   * {@link CacheException} if incompatible.
   */
  @SuppressWarnings("serial")
  private static void validateVmCompatibility() {
    try {
      // Do we have the Unsafe class? Throw ClassNotFoundException if not.
      Class<?> klass = ClassPathLoader.getLatest().forName("sun.misc.Unsafe");

      // Okay, we have the class. Do we have the copyMemory method (not all JVMs support it)? Throw
      // NoSuchMethodException if not.
      @SuppressWarnings("unused")
      Method copyMemory = klass.getMethod("copyMemory", Object.class, long.class, Object.class,
          long.class, long.class);
    } catch (ClassNotFoundException e) {
      throw new CacheException(
          String.format(
              "Your Java virtual machine is incompatible with off-heap memory.  Please refer to %s documentation for suggested JVMs.",
              "product"),
          e) {};
    } catch (NoSuchMethodException e) {
      throw new CacheException(
          String.format(
              "Your Java virtual machine is incompatible with off-heap memory.  Please refer to %s documentation for suggested JVMs.",
              "product"),
          e) {};
    }
  }

  /**
   * Constructs a MemoryAllocator for off-heap storage.
   *
   * @return MemoryAllocator for off-heap storage
   */
  public static MemoryAllocator createOffHeapStorage(StatisticsFactory sf, long offHeapMemorySize,
      DistributedSystem system) {
    if (offHeapMemorySize == 0 || Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE)) {
      // Checking the FORCE_LOCATOR_DM_TYPE is a quick hack to keep our locator from allocating off
      // heap memory.
      return null;
    }

    if (offHeapMemorySize < MIN_SLAB_SIZE) {
      throw new IllegalArgumentException("The amount of off heap memory must be at least "
          + MIN_SLAB_SIZE + " but it was set to " + offHeapMemorySize);
    }

    // Ensure that using off-heap will work with this JVM.
    validateVmCompatibility();

    if (system == null) {
      throw new IllegalArgumentException("InternalDistributedSystem is null");
    }
    // ooohml provides the hook for disconnecting and closing cache on OutOfOffHeapMemoryException
    OutOfOffHeapMemoryListener ooohml =
        new DisconnectingOutOfOffHeapMemoryListener((InternalDistributedSystem) system);
    return basicCreateOffHeapStorage(sf, offHeapMemorySize, ooohml);
  }

  static MemoryAllocator basicCreateOffHeapStorage(StatisticsFactory sf, long offHeapMemorySize,
      OutOfOffHeapMemoryListener ooohml) {
    final OffHeapMemoryStats stats = new OffHeapStorage(sf);

    // determine off-heap and slab sizes
    final long maxSlabSize = calcMaxSlabSize(offHeapMemorySize);

    final int slabCount = calcSlabCount(maxSlabSize, offHeapMemorySize);

    return MemoryAllocatorImpl.create(ooohml, stats, slabCount, offHeapMemorySize, maxSlabSize);
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
      throw new IllegalArgumentException(
          "The number of slabs of off heap memory exceeded the limit of " + Integer.MAX_VALUE
              + ". Decrease the amount of off heap memory or increase the maximum slab size using gemfire.OFF_HEAP_SLAB_SIZE.");
    }
    return (int) result;
  }

  private static long parseLongWithUnits(String v, long defaultValue, int defaultMultiplier) {
    if (v == null || v.equals("")) {
      return defaultValue;
    }
    int unitMultiplier = defaultMultiplier;
    if (v.toLowerCase().endsWith("g")) {
      unitMultiplier = 1024 * 1024 * 1024;
      v = v.substring(0, v.length() - 1);
    } else if (v.toLowerCase().endsWith("m")) {
      unitMultiplier = 1024 * 1024;
      v = v.substring(0, v.length() - 1);
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

  private void incDefragmentations() {
    this.stats.incInt(defragmentationId, 1);
  }

  @Override
  public int getDefragmentations() {
    return this.stats.getInt(defragmentationId);
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
  public int getDefragmentationsInProgress() {
    return this.stats.getInt(defragmentationsInProgressId);
  }

  @Override
  public long startDefragmentation() {
    this.stats.incInt(defragmentationsInProgressId, 1);
    return DistributionStats.getStatTime();
  }

  @Override
  public void endDefragmentation(long start) {
    incDefragmentations();
    this.stats.incInt(defragmentationsInProgressId, -1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(defragmentationTimeId, DistributionStats.getStatTime() - start);
    }
  }

  @Override
  public long getDefragmentationTime() {
    return stats.getLong(defragmentationTimeId);
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
    setDefragmentations(oldStats.getDefragmentations());
    setDefragmentationsInProgress(oldStats.getDefragmentationsInProgress());
    setFragments(oldStats.getFragments());
    setLargestFragment(oldStats.getLargestFragment());
    setDefragmentationTime(oldStats.getDefragmentationTime());
    setFragmentation(oldStats.getFragmentation());

    oldStats.close();
  }

  private void setDefragmentationTime(long value) {
    stats.setLong(defragmentationTimeId, value);
  }

  private void setDefragmentations(int value) {
    this.stats.setInt(defragmentationId, value);
  }

  private void setDefragmentationsInProgress(int value) {
    this.stats.setInt(defragmentationsInProgressId, value);
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
}
