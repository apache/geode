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
package org.apache.geode.cache.query.internal.index;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsClockFactory;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * IndexStats tracks statistics about query index use.
 */
public class IndexStats {

  @Immutable
  private static final StatisticsType type;

  private static final int numKeysId;
  private static final int numValuesId;
  private static final int numUpdatesId;
  private static final int numUsesId;
  private static final int updateTimeId;
  private static final int useTimeId;
  private static final int updatesInProgressId;
  private static final int usesInProgressId;
  private static final int readLockCountId;
  private static final int numMapIndexKeysId;
  private static final int numBucketIndexesId;

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

  private final StatisticsClock clock;

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    final String numKeysDesc = "Number of keys in this index";
    final String numValuesDesc = "Number of values in this index";
    final String numUpdatesDesc = "Number of updates that have completed on this index";
    final String numUsesDesc = "Number of times this index has been used while executing a query";
    final String updateTimeDesc = "Total time spent updating this index";

    type = f.createType("IndexStats", "Statistics about a query index",
        new StatisticDescriptor[] {f.createLongGauge("numKeys", numKeysDesc, "keys"),
            f.createLongGauge("numValues", numValuesDesc, "values"),
            f.createLongCounter("numUpdates", numUpdatesDesc, "operations"),
            f.createLongCounter("numUses", numUsesDesc, "operations"),
            f.createLongCounter("updateTime", updateTimeDesc, "nanoseconds"),
            f.createLongCounter("useTime", "Total time spent using this index", "nanoseconds"),
            f.createIntGauge("updatesInProgress", "Current number of updates in progress.",
                "updates"),
            f.createIntGauge("usesInProgress", "Current number of uses in progress.", "uses"),
            f.createIntGauge("readLockCount", "Current number of read locks taken.", "uses"),
            f.createLongGauge("numMapIndexKeys", "Number of keys in this Map index", "keys"),
            f.createIntGauge("numBucketIndexes",
                "Number of bucket indexes in the partitioned region", "indexes"),});

    // Initialize id fields
    numKeysId = type.nameToId("numKeys");
    numValuesId = type.nameToId("numValues");
    numUpdatesId = type.nameToId("numUpdates");
    numUsesId = type.nameToId("numUses");
    updateTimeId = type.nameToId("updateTime");
    updatesInProgressId = type.nameToId("updatesInProgress");
    usesInProgressId = type.nameToId("usesInProgress");
    useTimeId = type.nameToId("useTime");
    readLockCountId = type.nameToId("readLockCount");
    numMapIndexKeysId = type.nameToId("numMapIndexKeys");
    numBucketIndexesId = type.nameToId("numBucketIndexes");
  }

  /**
   * Creates a new <code>IndexStats</code> and registers itself with the given statistics
   * factory.
   */
  public IndexStats(StatisticsFactory factory, String indexName) {
    this(factory, indexName, StatisticsClockFactory.clock());
  }

  private IndexStats(StatisticsFactory factory, String indexName, StatisticsClock clock) {
    stats = factory.createAtomicStatistics(type, indexName);
    this.clock = clock;
  }

  public long getNumberOfKeys() {
    return stats.getLong(numKeysId);
  }

  public long getNumberOfValues() {
    return stats.getLong(numValuesId);
  }

  public long getNumUpdates() {
    return stats.getLong(numUpdatesId);
  }

  public long getTotalUses() {
    return stats.getLong(numUsesId);
  }

  public long getTotalUpdateTime() {
    return clock.isEnabled() ? stats.getLong(updateTimeId) : 0;
  }

  public long getUseTime() {
    return clock.isEnabled() ? stats.getLong(useTimeId) : 0;
  }

  public int getReadLockCount() {
    return stats.getInt(readLockCountId);
  }

  public long getNumberOfMapIndexKeys() {
    return stats.getLong(numMapIndexKeysId);
  }

  public int getNumberOfBucketIndexes() {
    return stats.getInt(numBucketIndexesId);
  }

  public void incNumUpdates() {
    stats.incLong(numUpdatesId, 1);
  }

  public void incNumUpdates(int delta) {
    stats.incLong(numUpdatesId, delta);
  }

  public void incNumValues(int delta) {
    stats.incLong(numValuesId, delta);
  }

  public void updateNumKeys(long numKeys) {
    stats.setLong(numKeysId, numKeys);
  }

  public void incNumKeys(long numKeys) {
    stats.incLong(numKeysId, numKeys);
  }

  public void incUpdateTime(long delta) {
    if (clock.isEnabled()) {
      stats.incLong(updateTimeId, delta);
    }
  }

  public void incNumUses() {
    stats.incLong(numUsesId, 1);
  }

  public void incUpdatesInProgress(int delta) {
    stats.incInt(updatesInProgressId, delta);
  }

  public void incUsesInProgress(int delta) {
    stats.incInt(usesInProgressId, delta);
  }

  public void incUseTime(long delta) {
    if (clock.isEnabled()) {
      stats.incLong(useTimeId, delta);
    }
  }

  public void incReadLockCount(int delta) {
    stats.incInt(readLockCountId, delta);
  }

  public void incNumMapIndexKeys(long delta) {
    stats.incLong(numMapIndexKeysId, delta);
  }

  public void incNumBucketIndexes(int delta) {
    stats.incInt(numBucketIndexesId, delta);
  }

  /**
   * Closes these stats so that they can not longer be used. The stats are closed when the cache is
   * closed.
   *
   * @since GemFire 3.5
   */
  void close() {
    stats.close();
  }

  /**
   * Returns whether or not these stats have been closed
   *
   * @since GemFire 3.5
   */
  public boolean isClosed() {
    return stats.isClosed();
  }

  /**
   * Returns the Statistics instance that stores the cache perf stats.
   *
   * @since GemFire 3.5
   */
  public Statistics getStats() {
    return stats;
  }
}
