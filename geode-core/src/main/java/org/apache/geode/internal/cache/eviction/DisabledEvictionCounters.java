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

import org.apache.geode.Statistics;

class DisabledEvictionCounters implements EvictionCounters {

  @Override
  public void close() {
    // nothing
  }

  @Override
  public void setLimit(long newValue) {
    // nothing
  }

  @Override
  public void resetCounter() {
    // nothing
  }

  @Override
  public void decrementCounter(long delta) {
    // nothing
  }

  @Override
  public long getDestroys() {
    return 0;
  }

  @Override
  public void incEvaluations(long evaluations) {
    // nothing
  }

  @Override
  public void incGreedyReturns(long greedyReturns) {
    // nothing
  }

  @Override
  public void incEvictions() {
    // nothing
  }

  @Override
  public long getCounter() {
    return 0;
  }

  @Override
  public long getLimit() {
    return 0;
  }

  @Override
  public void updateCounter(long delta) {
    // nothing
  }

  @Override
  public long getEvictions() {
    return 0;
  }

  @Override
  public Statistics getStatistics() {
    return null;
  }

  @Override
  public void incDestroys() {
    // nothing
  }
}
