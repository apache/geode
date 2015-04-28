/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.lru;

import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.Region;

/**
 *  Marker interface to eviction controller that determines if LRU list
 *  maintainance is required.
 */
public interface EnableLRU {

  /**
   * return the size of an entry or its worth when constraining the size of an LRU EntriesMap.
   */
  public int entrySize( Object key, Object value ) throws IllegalArgumentException;

  /**
   * return the limit for the accumulated entrySize which triggers disposal.
   */
  public long limit( );

  /** setup stats for this LRU type, if reset is true, initialize counter on stats to zero. */
  public LRUStatistics initStats(Object region, StatisticsFactory sf);

  /** return the eviction controller instance this came from */
  public EvictionAlgorithm getEvictionAlgorithm();

  /** return the stats object for this eviction controller */
  public LRUStatistics getStats();

  /**
   *  Returns the {@linkplain EvictionAction action} to
   * take when the LRU entry is evicted.
   */
  public EvictionAction getEvictionAction();

  /**
   * Returns the statistics for this LRU algorithm
   */
  public StatisticsType getStatisticsType();

  /**
   * Returns the name of the statistics for this LRU algorithm
   */
  public String getStatisticsName();

  /**
   * Returns the id of the "limit" statistic for this LRU algorithm's
   * statistics
   */
  public int getLimitStatId();

  /**
   * Returns the id of the "counter" statistic for this LRU
   * algorithm's statistics.
   */
  public int getCountStatId();

  /**
   * Returns the id of the "evictions" statistic for this LRU
   * algorithm's statistics.
   */
  public int getEvictionsStatId();

  /**
   * Returns the id of the "destroys" statistic for this LRU
   * algorithm's statistics.
   */
  public int getDestroysStatId();

  /**
   * Returns the id of the "destroysLimit" statistic for this LRU algorithm's
   * statistics
   */
  public int getDestroysLimitStatId();

  /**
   * Returns the id of the "evaluations" statistic for this LRU
   * algorithm's statistics.
   */
  public int getEvaluationsStatId();

  /**
   * Returns the id of the "greedyReturns" statistic for this LRU
   * algorith'ms statistics
   * 
   * @return the id
   */
  public int getGreedyReturnsStatId();
  
  /**
   * Returns whether or not there is enough room to accommodate data
   * of the given size based on the given <code>LRUStatistics</code>.
   */
  public boolean mustEvict(LRUStatistics stats, Region region, int delta);
  /**
   * Envoked after an entry has been evicted
   */
  public void afterEviction();

}

