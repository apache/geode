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
package com.gemstone.gemfire.internal.statistics;

/**
 * @since 7.0
 */
public final class CounterMonitor extends StatisticsMonitor {
  
  public static enum Type {
    GREATER_THAN, LESS_THAN
  }

  private volatile Number threshold;
  
  public CounterMonitor(Number threshold) {
    super();
    this.threshold = threshold;
  }

  @Override
  public CounterMonitor addStatistic(StatisticId statId) {
    super.addStatistic(statId);
    return this;
  }
  
  @Override
  public CounterMonitor removeStatistic(StatisticId statId) {
    super.removeStatistic(statId);
    return this;
  }

  public CounterMonitor greaterThan(Number threshold) {
    return this;
  }
  
  @Override
  protected StringBuilder appendToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("threshold=").append(this.threshold);
    return sb;
  }
}
