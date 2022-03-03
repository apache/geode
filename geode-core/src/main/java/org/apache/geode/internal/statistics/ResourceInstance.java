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

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;

/**
 * An instance of a Statistics resource. There may be zero, one or many instances depending on the
 * statistics type. The ResourceInstance holds an array of the latest stat values.
 * <p/>
 * Extracted from {@link StatArchiveWriter}.
 *
 * @since GemFire 7.0
 */
public class ResourceInstance {

  private final int id;
  private final Statistics statistics;
  private final ResourceType type;
  private long[] previousStatValues = null;
  private long[] latestStatValues = null;
  private int[] updatedStats = null;
  private boolean statValuesNotified;

  public ResourceInstance(int id, Statistics statistics, ResourceType type) {
    this.id = id;
    this.statistics = statistics;
    this.type = type;
    statValuesNotified = false;
  }

  public int getId() {
    return id;
  }

  public Statistics getStatistics() {
    return statistics;
  }

  public ResourceType getResourceType() {
    return type;
  }

  public boolean getStatValuesNotified() {
    return statValuesNotified;
  }

  public void setStatValuesNotified(boolean notified) {
    statValuesNotified = notified;
  }

  public Number getStatValue(StatisticDescriptor sd) {
    return statistics.get(sd);
  }

  public long getRawStatValue(StatisticDescriptor sd) {
    return statistics.getRawBits(sd);
  }

  public long[] getLatestStatValues() {
    return latestStatValues;
  }

  public void setLatestStatValues(long[] latestStatValues) {
    this.latestStatValues = latestStatValues;
  }

  public long[] getPreviousStatValues() {
    return previousStatValues;
  }

  public void setPreviousStatValues(long[] previousStatValues) {
    this.previousStatValues = previousStatValues;
  }

  public int[] getUpdatedStats() {
    return updatedStats;
  }

  public void setUpdatedStats(int[] updatedStats) {
    this.updatedStats = updatedStats;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("id=").append(id);
    if (updatedStats == null) {
      sb.append(", updatedStats=null");
    } else {
      sb.append(", updatedStats.length=").append(updatedStats.length);
    }
    if (previousStatValues == null) {
      sb.append(", previousStatValues=null");
    } else {
      sb.append(", previousStatValues.length=").append(previousStatValues.length);
    }
    if (latestStatValues == null) {
      sb.append(", latestStatValues=null");
    } else {
      sb.append(", latestStatValues.length=").append(latestStatValues.length);
    }
    sb.append(", statistics=").append(statistics);
    sb.append(", resourceType=").append(type);
    sb.append("}");
    return sb.toString();
  }

  // private int unsignedByteToInt(byte value) {
  // return value & 0x000000FF;
  // }
}
