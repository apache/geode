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
package com.gemstone.gemfire.internal.admin.statalerts;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.admin.Statistic;

/**
 * 
 * Implemetation of {@link StatisticInfo}, provides all the information
 * {@link Statistic}
 * 
 * 
 */
public class StatisticInfoImpl implements StatisticInfo {
  private static final long serialVersionUID = -1525964578728218894L;

  protected transient Statistics statistics;

  protected transient StatisticDescriptor descriptor;

  /**
   * @param statistics
   * @param descriptor
   */
  public StatisticInfoImpl(Statistics statistics, StatisticDescriptor descriptor) {
    super();
    this.statistics = statistics;
    this.descriptor = descriptor;
  }

  public String getStatisticName() {
    return descriptor.getName();
  }

  public String getStatisticsTextId() {
    return statistics.getTextId();
  }

  public void setStatisticName(String statisticName) {
    throw new UnsupportedOperationException(
        "StatisticInfoImpl class does not support setStatisticName method.");
  }

  public void setStatisticsTextId(String statisticsTextId) {
    throw new UnsupportedOperationException(
        "StatisticInfoImpl class does not support setStatisticsTextId method.");
  }

  public Number getValue() {
    return statistics.get(descriptor);
  }

  public StatisticDescriptor getStatisticDescriptor() {
    return this.descriptor;
  }

  public Statistics getStatistics() {
    return this.statistics;
  }

  public String getStatisticsTypeName() {
    return this.statistics.getType().getName();
  }

  public void setStatisticsTypeName(String statisticsType) {
    throw new UnsupportedOperationException(
        "StatisticInfoImpl class does not support setStatisticsTypeName method.");
  }

  @Override
  public boolean equals(Object object) {

    if (object == null || !(object instanceof StatisticInfoImpl)) {
      return false;
    }

    String statisticsTextId = getStatisticsTextId();

    StatisticInfoImpl other = (StatisticInfoImpl)object;

    if (StringUtils.equals(getStatisticName(), other.getStatisticName())
        && statisticsTextId != null
        && statisticsTextId.equals(other.getStatisticsTextId())) {
      return true;
    }

    return false;
  }

  public void toData(DataOutput out) throws IOException {
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
  }

  @Override
  public int hashCode() {
    return (getStatisticName() + ":" + getStatisticsTextId()).hashCode();
  }

  @Override
  public String toString() {
    return this.statistics.getType().getName() + " ["
        + this.descriptor.getName() + "]";
  }

  public static StatisticInfoImpl create(String toString, StatisticsFactory f) {
    int startBrack = toString.indexOf("[");
    int endBrack = toString.indexOf("]");

    if (startBrack == -1 || endBrack == -1)
      return null;

    String name = toString.substring(0, startBrack).trim();
    String ids = toString.substring(startBrack + 1, endBrack).trim();

    StatisticsType type = f.findType(name);
    if (type == null)
      return null;

    Statistics[] stats = f.findStatisticsByType(type);
    if (stats.length == 0)
      return null;

    StatisticDescriptor[] descs = type.getStatistics();
    for (int i = 0; i < descs.length; i++) {
      if (descs[i].getName().equalsIgnoreCase(ids))
        return new StatisticInfoImpl(stats[0], descs[i]);
    }

    return null;
  }
}
