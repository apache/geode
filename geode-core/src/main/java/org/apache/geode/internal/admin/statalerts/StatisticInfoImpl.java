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
package org.apache.geode.internal.admin.statalerts;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.admin.Statistic;

/**
 *
 * Implemetation of {@link StatisticInfo}, provides all the information {@link Statistic}
 *
 *
 */
public class StatisticInfoImpl implements StatisticInfo {
  private static final long serialVersionUID = -1525964578728218894L;

  protected transient Statistics statistics;

  protected transient StatisticDescriptor descriptor;

  public StatisticInfoImpl(Statistics statistics, StatisticDescriptor descriptor) {
    super();
    this.statistics = statistics;
    this.descriptor = descriptor;
  }

  @Override
  public String getStatisticName() {
    return descriptor.getName();
  }

  @Override
  public String getStatisticsTextId() {
    return statistics.getTextId();
  }

  @Override
  public void setStatisticName(String statisticName) {
    throw new UnsupportedOperationException(
        "StatisticInfoImpl class does not support setStatisticName method.");
  }

  @Override
  public void setStatisticsTextId(String statisticsTextId) {
    throw new UnsupportedOperationException(
        "StatisticInfoImpl class does not support setStatisticsTextId method.");
  }

  @Override
  public Number getValue() {
    return statistics.get(descriptor);
  }

  @Override
  public StatisticDescriptor getStatisticDescriptor() {
    return descriptor;
  }

  @Override
  public Statistics getStatistics() {
    return statistics;
  }

  @Override
  public String getStatisticsTypeName() {
    return statistics.getType().getName();
  }

  @Override
  public void setStatisticsTypeName(String statisticsType) {
    throw new UnsupportedOperationException(
        "StatisticInfoImpl class does not support setStatisticsTypeName method.");
  }

  @Override
  public boolean equals(Object object) {

    if (!(object instanceof StatisticInfoImpl)) {
      return false;
    }

    String statisticsTextId = getStatisticsTextId();

    StatisticInfoImpl other = (StatisticInfoImpl) object;

    return StringUtils.equals(getStatisticName(), other.getStatisticName())
        && statisticsTextId != null
        && statisticsTextId.equals(other.getStatisticsTextId());
  }

  @Override
  public void toData(DataOutput out) throws IOException {}

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {}

  @Override
  public int hashCode() {
    return (getStatisticName() + ":" + getStatisticsTextId()).hashCode();
  }

  @Override
  public String toString() {
    return statistics.getType().getName() + " [" + descriptor.getName() + "]";
  }

  public static StatisticInfoImpl create(String toString, StatisticsFactory f) {
    int startBrack = toString.indexOf("[");
    int endBrack = toString.indexOf("]");

    if (startBrack == -1 || endBrack == -1) {
      return null;
    }

    String name = toString.substring(0, startBrack).trim();
    String ids = toString.substring(startBrack + 1, endBrack).trim();

    StatisticsType type = f.findType(name);
    if (type == null) {
      return null;
    }

    Statistics[] stats = f.findStatisticsByType(type);
    if (stats.length == 0) {
      return null;
    }

    StatisticDescriptor[] descs = type.getStatistics();
    for (final StatisticDescriptor desc : descs) {
      if (desc.getName().equalsIgnoreCase(ids)) {
        return new StatisticInfoImpl(stats[0], desc);
      }
    }

    return null;
  }
}
