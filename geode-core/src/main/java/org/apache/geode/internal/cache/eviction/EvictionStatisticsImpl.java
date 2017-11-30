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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.Assert;

class EvictionStatisticsImpl implements InternalEvictionStatistics {

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;
  private final int limitId;

  /**
   * the number of destroys that must occur before a list scan is initiated to to remove unlinked
   * entries.
   */
  private final int destroysLimitId;
  private final int counterId;

  /** entries that have been evicted from the LRU list */
  private final int evictionsId;

  /** entries that have been destroyed, but not yet evicted from the LRU list */
  private final int destroysId;
  private final int evaluationsId;
  private final int greedyReturnsId;

  // Note: the following atomics have been added so that the eviction code
  // does not depend on the value of a statistic for its operations.
  // In particular they optimize the "get" methods for these items.
  // Striped stats optimize inc but cause set and get to be more expensive.
  private final AtomicLong counter = new AtomicLong();
  private final AtomicLong limit = new AtomicLong();
  private final AtomicLong destroysLimit = new AtomicLong();
  private final AtomicLong destroys = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();

  public EvictionStatisticsImpl(StatisticsFactory factory, String name, EvictionController helper) {
    String statName = helper.getStatisticsName() + "-" + name;
    stats = factory.createAtomicStatistics(helper.getStatisticsType(), statName);
    if (helper.getEvictionAlgorithm().isLRUHeap()) {
      limitId = 0;
    } else {
      limitId = helper.getLimitStatId();
    }
    destroysLimitId = helper.getDestroysLimitStatId();
    counterId = helper.getCountStatId();
    evictionsId = helper.getEvictionsStatId();
    destroysId = helper.getDestroysStatId();
    this.evaluationsId = helper.getEvaluationsStatId();
    this.greedyReturnsId = helper.getGreedyReturnsStatId();
  }

  @Override
  public void incEvictions() {
    this.evictions.getAndAdd(1);
    stats.incLong(evictionsId, 1);
  }

  /** common counter for different eviction types */
  @Override
  public long getCounter() {
    return this.counter.get();
  }

  @Override
  public long getLimit() {
    return this.limit.get();
  }

  @Override
  public void updateCounter(long delta) {
    if (delta != 0) {
      this.counter.getAndAdd(delta);
      stats.incLong(counterId, delta);
    }
  }

  @Override
  public long getEvictions() {
    return this.evictions.get();
  }

  @Override
  public Statistics getStats() {
    return this.stats;
  }

  @Override
  public void incDestroys() {
    this.destroys.getAndAdd(1);
    stats.incLong(destroysId, 1);
  }

  @Override
  public void close() {
    stats.close();
  }

  @Override
  public void setLimit(long newValue) {
    Assert.assertTrue(newValue > 0L,
        "limit must be positive, an attempt was made to set it to: " + newValue);
    long oldValue = this.limit.get();
    if (oldValue != newValue) {
      this.limit.set(newValue);
      stats.setLong(limitId, newValue);
    }
  }

  /** destroy limit */
  @Override
  public void setDestroysLimit(long newValue) {
    Assert.assertTrue(newValue > 0L,
        "destroys limit must be positive, an attempt was made to set it to: " + newValue);
    long oldValue = this.destroysLimit.get();
    if (oldValue != newValue) {
      this.destroysLimit.set(newValue);
      stats.setLong(destroysLimitId, newValue);
    }
  }

  @Override
  public long getDestroysLimit() {
    return this.destroysLimit.get();
  }

  @Override
  public void resetCounter() {
    if (this.counter.get() != 0) {
      this.counter.set(0);
      stats.setLong(counterId, 0);
    }
  }

  @Override
  public void decrementCounter(long delta) {
    if (delta != 0) {
      this.counter.addAndGet(-delta);
      stats.setLong(counterId, counter.get());
    }
  }

  @Override
  public long getDestroys() {
    return this.destroys.get();
  }

  @Override
  public void incEvaluations(long evaluations) {
    stats.incLong(evaluationsId, evaluations);
  }

  @Override
  public void incGreedyReturns(long greedyReturns) {
    stats.incLong(greedyReturnsId, greedyReturns);
  }
}
