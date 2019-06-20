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

package org.apache.geode.internal.statistics.meters;

import org.apache.geode.Statistics;

/**
 * Increments and reads an {@code int} stat.
 */
public class IntStatisticBinding implements StatisticBinding {
  private final Statistics statistics;
  private final int statId;

  public IntStatisticBinding(Statistics statistics, int statId) {
    this.statistics = statistics;
    this.statId = statId;
  }

  @Override
  public void add(double amount) {
    statistics.incInt(statId, (int) amount);
  }

  @Override
  public double doubleValue() {
    return (double) statistics.getInt(statId);
  }

  @Override
  public long longValue() {
    return 0;
  }
}
