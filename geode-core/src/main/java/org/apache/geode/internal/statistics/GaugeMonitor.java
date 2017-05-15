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
package org.apache.geode.internal.statistics;

/**
 * @since GemFire 7.0
 */
public class GaugeMonitor extends StatisticsMonitor {

  private final Number lowThreshold;
  private final Number highThreshold;

  public GaugeMonitor(Number lowThreshold, Number highThreshold) {
    super();
    this.lowThreshold = lowThreshold;
    this.highThreshold = highThreshold;
  }

  @Override
  public GaugeMonitor addStatistic(StatisticId statId) {
    super.addStatistic(statId);
    return this;
  }

  @Override
  public GaugeMonitor removeStatistic(StatisticId statId) {
    super.removeStatistic(statId);
    return this;
  }

  @Override
  protected StringBuilder appendToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("lowThreshold=").append(this.lowThreshold);
    sb.append(", highThreshold=").append(this.highThreshold);
    return sb;
  }
}
