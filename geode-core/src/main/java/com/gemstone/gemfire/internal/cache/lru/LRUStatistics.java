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

package com.gemstone.gemfire.internal.cache.lru;

import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.Assert;

/**
 * Statistics for both the LocalLRUClockHand.  Note that all its instance fields are
 * <code>final</code>.  Thus, we do not need to worry about refreshing
 * an instance when it resides in shared memory.
 */
public class LRUStatistics  {

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;
  protected  int limitId;
  /**
   * the number of destroys that must occur before a list scan is initiated to
   * to remove unlinked entries.
   */
  protected  int destroysLimitId;
  protected int counterId;
  /** entries that have been evicted from the LRU list */
  protected  int evictionsId;
  /** entries that have been destroyed, but not yet evicted from the LRU list */
  protected int destroysId;
  protected  int evaluationsId;
  protected  int greedyReturnsId;

  // Note: the following atomics have been added so that the LRU code
  // does not depend on the value of a statistic for its operations.
  // In particular they optimize the "get" methods for these items.
  // Striped stats optimize inc but cause set and get to be more expensive.
  private final AtomicLong counter = new AtomicLong();
  private final AtomicLong limit = new AtomicLong();
  private final AtomicLong destroysLimit = new AtomicLong();
  private final AtomicLong destroys = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();
  
  /////////////////////////  Constructors  /////////////////////////

  /**
   *  Constructor for the LRUStatistics object
   *
   * @param  name  Description of the Parameter
   */
  public LRUStatistics( StatisticsFactory factory, String name, 
                        EnableLRU helper) {
    String statName = helper.getStatisticsName() + "-" + name;
    stats = factory.createAtomicStatistics(helper.getStatisticsType(),
                                           statName);
    if(!helper.getEvictionAlgorithm().isLRUHeap())
    {
      limitId = helper.getLimitStatId();
    }
    destroysLimitId = helper.getDestroysLimitStatId();
    counterId = helper.getCountStatId();
    evictionsId = helper.getEvictionsStatId();
    destroysId = helper.getDestroysStatId();
    this.evaluationsId = helper.getEvaluationsStatId();
    this.greedyReturnsId = helper.getGreedyReturnsStatId();
  }

  public LRUStatistics(StatisticsFactory factory, String name,
      StatisticsType statisticsType) {
    stats = factory.createAtomicStatistics(statisticsType, name);
    limitId = 0;
    destroysLimitId = 0;
    counterId = 0;
    evictionsId = 0;
    destroysId = 0;
    this.evaluationsId = 0;
    this.greedyReturnsId = 0;
  }

  public void close() {
    stats.close();
  }

  /** common counter for different lru types */
  public long getCounter( ) {
    return this.counter.get();
  }

  /** limit */
  public void setLimit( long newValue ) {
    Assert.assertTrue(newValue > 0L,
                      "limit must be positive, an attempt was made to set it to: "
                      + newValue);
    long oldValue = this.limit.get();
    if (oldValue != newValue) {
      this.limit.set(newValue);
      stats.setLong(limitId, newValue);
    }
  }

  /** destroy limit */
  public void setDestroysLimit( long newValue ) {
    Assert.assertTrue(newValue > 0L,
                      "destroys limit must be positive, an attempt was made to set it to: "
                      + newValue);
    long oldValue = this.destroysLimit.get();
    if (oldValue != newValue) {
      this.destroysLimit.set(newValue);
      stats.setLong( destroysLimitId, newValue );
    }
  }

  public long getLimit( ) {
    return this.limit.get();
  }

  public long getDestroysLimit( ) {
    return this.destroysLimit.get();
  }

  public void updateCounter( long delta ) {
    if (delta != 0) {
      this.counter.getAndAdd(delta);
      stats.incLong(counterId, delta);
    }
  }

  public void resetCounter( ) {
    if (this.counter.get() != 0) {
      this.counter.set(0);
      stats.setLong(counterId, 0);
    }
  }

//  public void setCounter(long newValue) {
//    long oldValue = this.counter.get();
//    if (oldValue != newValue) {
//      this.counter.set(oldValue+newValue);
//      stats.setLong(counterId, newValue);
//    }
//  }
//  
  public void decrementCounter(long delta) {
    if (delta != 0) {
      this.counter.addAndGet(-delta);
      stats.setLong(counterId, counter.get());
    }
  }

  public void incEvictions( ) {
    this.evictions.getAndAdd(1);
    stats.incLong( evictionsId, 1 );
  }

  public void incEvictions(long delta ) {
    this.evictions.getAndAdd(delta);
    stats.incLong( evictionsId, delta );
  }
  
  public long getEvictions( ) {
    return this.evictions.get();
  }

  public void resetDestroys( ) {
    if (this.destroys.get() != 0) {
      this.destroys.set(0);
      stats.setLong(destroysId, 0);
    }
  }

  public void incDestroys( ) {
    this.destroys.getAndAdd(1);
    stats.incLong( destroysId, 1 );
  }

  public long getDestroys( ) {
    return this.destroys.get();
  }

  public void incEvaluations(long numEvals ) {
    stats.incLong( evaluationsId, numEvals );
  }

  public void incGreedyReturns(long numEvals) {
    stats.incLong(greedyReturnsId, numEvals);
  }
  

  public Statistics getStats() {
    return this.stats;
  }

}

