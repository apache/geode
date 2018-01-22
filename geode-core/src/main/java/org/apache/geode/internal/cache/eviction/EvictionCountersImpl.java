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

class EvictionCountersImpl implements EvictionCounters {

  /** The Statistics object that we delegate most behavior to */
  private final EvictionStats stats;

  // Note: the following atomics have been added so that the eviction code
  // does not depend on the value of a statistic for its operations.
  // In particular they optimize the "get" methods for these items.
  // Striped stats optimize inc but cause set and get to be more expensive.
  private final AtomicLong counter = new AtomicLong();
  private final AtomicLong limit = new AtomicLong();
  private final AtomicLong destroys = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();

  public EvictionCountersImpl(EvictionStats stats) {
    this.stats = stats;
  }

  @Override
  public void incEvictions() {
    this.evictions.getAndAdd(1);
    this.stats.incEvictions();
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
      this.stats.updateCounter(delta);
    }
  }

  @Override
  public long getEvictions() {
    return this.evictions.get();
  }

  @Override
  public Statistics getStatistics() {
    return this.stats.getStatistics();
  }

  @Override
  public void incDestroys() {
    this.destroys.getAndAdd(1);
    this.stats.incDestroys();
  }

  @Override
  public void close() {
    this.stats.close();
  }

  @Override
  public void setLimit(long newValue) {
    long oldValue = this.limit.get();
    if (oldValue != newValue) {
      this.limit.set(newValue);
      this.stats.setLimit(newValue);
    }
  }

  @Override
  public void resetCounter() {
    if (this.counter.get() != 0) {
      this.counter.set(0);
      this.stats.setCounter(0L);
    }
  }

  @Override
  public void decrementCounter(long delta) {
    if (delta != 0) {
      long newValue = this.counter.addAndGet(-delta);
      this.stats.setCounter(newValue);
    }
  }

  @Override
  public long getDestroys() {
    return this.destroys.get();
  }

  @Override
  public void incEvaluations(long evaluations) {
    this.stats.incEvaluations(evaluations);
  }

  @Override
  public void incGreedyReturns(long greedyReturns) {
    this.stats.incGreedyReturns(greedyReturns);
  }
}
