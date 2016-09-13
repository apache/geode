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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;

/**
 * @since GemFire 7.0
 */
public class MapBasedStatisticsNotification implements StatisticsNotification {

  private final long millisTimeStamp;
  private final Type type;
  private final Map<StatisticId, Number> stats;
  
  protected MapBasedStatisticsNotification(long millisTimeStamp,
                                           Type type,
                                           Map<StatisticId, Number> stats) {
    this.millisTimeStamp = millisTimeStamp;
    this.type = type;
    this.stats = Collections.unmodifiableMap(stats);
  }

  public long getTimeStamp() {
    return this.millisTimeStamp;
  }

  public Type getType() {
    return this.type;
  }

  public Iterator<StatisticId> iterator() {
    return this.stats.keySet().iterator();
  }

  public Iterator<StatisticId> iterator(StatisticDescriptor statDesc) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Iterator<StatisticId> iterator(Statistics statistics) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Iterator<StatisticId> iterator(StatisticsType statisticsType) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Number getValue(StatisticId statId) throws StatisticNotFoundException {
    Number value = this.stats.get(statId);
    if (value == null) {
      throw new StatisticNotFoundException(statId.getStatisticDescriptor().getName()
          + " not found in notification");
    }
    return value;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("millisTimeStamp=").append(this.millisTimeStamp);
    sb.append(", type=").append(this.type);
    sb.append(", stats=").append(this.stats);
    sb.append("}");
    return sb.toString();
  }
}
