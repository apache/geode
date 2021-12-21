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
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class CountLRUStatistics implements EvictionStats {
  @Immutable
  private static final StatisticsType statType;
  private static final int limitId;
  private static final int counterId;
  private static final int evictionsId;
  private static final int destroysId;
  private static final int evaluationsId;
  private static final int greedyReturnsId;

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    final String entriesAllowedDesc = "Number of entries allowed in this region.";
    final String regionEntryCountDesc = "Number of entries in this region.";
    final String lruEvictionsDesc = "Number of total entry evictions triggered by LRU.";
    final String lruDestroysDesc =
        "Number of entries destroyed in the region through both destroy cache operations and eviction.";
    final String lruEvaluationsDesc = "Number of entries evaluated during LRU operations.";
    final String lruGreedyReturnsDesc = "Number of non-LRU entries evicted during LRU operations";

    statType = f.createType("LRUStatistics", "Statistics relates to entry cout based eviction",
        new StatisticDescriptor[] {
            f.createLongGauge("entriesAllowed", entriesAllowedDesc, "entries"),
            f.createLongGauge("entryCount", regionEntryCountDesc, "entries"),
            f.createLongCounter("lruEvictions", lruEvictionsDesc, "entries"),
            f.createLongCounter("lruDestroys", lruDestroysDesc, "entries"),
            f.createLongCounter("lruEvaluations", lruEvaluationsDesc, "entries"),
            f.createLongCounter("lruGreedyReturns", lruGreedyReturnsDesc, "entries")});

    limitId = statType.nameToId("entriesAllowed");
    counterId = statType.nameToId("entryCount");
    evictionsId = statType.nameToId("lruEvictions");
    destroysId = statType.nameToId("lruDestroys");
    evaluationsId = statType.nameToId("lruEvaluations");
    greedyReturnsId = statType.nameToId("lruGreedyReturns");
  }

  private final Statistics stats;

  public CountLRUStatistics(StatisticsFactory factory, String name) {
    stats = factory.createAtomicStatistics(statType, "LRUStatistics-" + name);
  }

  @Override
  public Statistics getStatistics() {
    return stats;
  }

  @Override
  public void close() {
    stats.close();
  }

  @Override
  public void incEvictions() {
    stats.incLong(evictionsId, 1);
  }

  @Override
  public void updateCounter(long delta) {
    stats.incLong(counterId, delta);
  }

  @Override
  public void incDestroys() {
    stats.incLong(destroysId, 1);
  }

  @Override
  public void setLimit(long newValue) {
    stats.setLong(limitId, newValue);
  }

  @Override
  public void setCounter(long newValue) {
    stats.setLong(counterId, newValue);
  }

  @Override
  public void incEvaluations(long delta) {
    stats.incLong(evaluationsId, delta);
  }

  @Override
  public void incGreedyReturns(long delta) {
    stats.incLong(greedyReturnsId, delta);
  }

}
