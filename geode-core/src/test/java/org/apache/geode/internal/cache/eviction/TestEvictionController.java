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
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

class TestEvictionController implements EvictionController {

  private final StatisticsType statType;

  {
    // create the stats type for MemLRU.
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    final String bytesAllowedDesc = "Number of total bytes allowed in this region.";
    final String byteCountDesc = "Number of bytes in region.";
    final String lruEvictionsDesc = "Number of total entry evictions triggered by LRU.";
    final String lruEvaluationsDesc = "Number of entries evaluated during LRU operations.";
    final String lruGreedyReturnsDesc = "Number of non-LRU entries evicted during LRU operations";
    final String lruDestroysDesc = "Number of entry destroys triggered by LRU.";
    final String lruDestroysLimitDesc =
        "Maximum number of entry destroys triggered by LRU before scan occurs.";

    statType = f.createType("TestLRUStatistics",
        "Statistics about byte based Least Recently Used region entry disposal",
        new StatisticDescriptor[] {f.createLongGauge("bytesAllowed", bytesAllowedDesc, "bytes"),
            f.createLongGauge("byteCount", byteCountDesc, "bytes"),
            f.createLongCounter("lruEvictions", lruEvictionsDesc, "entries"),
            f.createLongCounter("lruEvaluations", lruEvaluationsDesc, "entries"),
            f.createLongCounter("lruGreedyReturns", lruGreedyReturnsDesc, "entries"),
            f.createLongCounter("lruDestroys", lruDestroysDesc, "entries"),
            f.createLongCounter("lruDestroysLimit", lruDestroysLimitDesc, "entries"),});
  }

  @Override
  public int entrySize(Object key, Object value) throws IllegalArgumentException {
    return 1;
  }

  @Override
  public long limit() {
    return 20;
  }

  public boolean usesMem() {
    return false;
  }

  @Override
  public EvictionAlgorithm getEvictionAlgorithm() {
    return EvictionAlgorithm.LRU_ENTRY;
  }

  @Override
  public EvictionStatistics getStatistics() {
    return null;
  }

  @Override
  public EvictionAction getEvictionAction() {
    return EvictionAction.DEFAULT_EVICTION_ACTION;
  }

  @Override
  public StatisticsType getStatisticsType() {
    return statType;
  }

  @Override
  public String getStatisticsName() {
    return "TestLRUStatistics";
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
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public EvictionStatistics initStats(Object region, StatisticsFactory statsFactory) {
    String regionName;
    if (region instanceof Region) {
      regionName = ((Region) region).getName();
    } else if (region instanceof PlaceHolderDiskRegion) {
      regionName = ((PlaceHolderDiskRegion) region).getName();
      // @todo make it shorter (I think it is the fullPath
    } else {
      throw new IllegalStateException("expected Region or PlaceHolderDiskRegion");
    }
    final InternalEvictionStatistics stats =
        new EvictionStatisticsImpl(statsFactory, "TestLRUStatistics" + regionName, this);
    stats.setLimit(limit());
    return stats;
  }

  @Override
  public boolean lruLimitExceeded(EvictionStatistics stats, DiskRegionView diskRegionView) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBucketRegion(Region region) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLimit() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setLimit(int maximum) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
