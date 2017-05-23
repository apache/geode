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

package org.apache.geode.internal.cache.lru;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.persistence.DiskRegionView;

/**
 * Marker interface to eviction controller that determines if LRU list maintainance is required.
 */
public interface EnableLRU {

  /**
   * return the size of an entry or its worth when constraining the size of an LRU EntriesMap.
   */
  int entrySize(Object key, Object value) throws IllegalArgumentException;

  /**
   * return the limit for the accumulated entrySize which triggers disposal.
   */
  long limit();

  /** setup stats for this LRU type, if reset is true, initialize counter on stats to zero. */
  LRUStatistics initStats(Object region, StatisticsFactory sf);

  /** return the eviction controller instance this came from */
  EvictionAlgorithm getEvictionAlgorithm();

  /** return the stats object for this eviction controller */
  LRUStatistics getStats();

  /**
   * Returns the {@linkplain EvictionAction action} to take when the LRU entry is evicted.
   */
  EvictionAction getEvictionAction();

  /**
   * Returns the statistics for this LRU algorithm
   */
  StatisticsType getStatisticsType();

  /**
   * Returns the name of the statistics for this LRU algorithm
   */
  String getStatisticsName();

  /**
   * Returns the id of the "limit" statistic for this LRU algorithm's statistics
   */
  int getLimitStatId();

  /**
   * Returns the id of the "counter" statistic for this LRU algorithm's statistics.
   */
  int getCountStatId();

  /**
   * Returns the id of the "evictions" statistic for this LRU algorithm's statistics.
   */
  int getEvictionsStatId();

  /**
   * Returns the id of the "destroys" statistic for this LRU algorithm's statistics.
   */
  int getDestroysStatId();

  /**
   * Returns the id of the "destroysLimit" statistic for this LRU algorithm's statistics
   */
  int getDestroysLimitStatId();

  /**
   * Returns the id of the "evaluations" statistic for this LRU algorithm's statistics.
   */
  int getEvaluationsStatId();

  /**
   * Returns the id of the "greedyReturns" statistic for this LRU algorith'ms statistics
   * 
   * @return the id
   */
  int getGreedyReturnsStatId();

  /**
   * Returns whether or not there is enough room to accommodate data of the given size based on the
   * given <code>LRUStatistics</code>.
   */
  boolean mustEvict(LRUStatistics stats, Region region, int delta);

  /**
   * Envoked after an entry has been evicted
   */
  void afterEviction();

  boolean lruLimitExceeded(LRUStatistics lruStatistics, DiskRegionView drv);

}

