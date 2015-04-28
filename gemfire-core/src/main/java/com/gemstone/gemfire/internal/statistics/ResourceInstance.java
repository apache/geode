/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;

/**
 * An instance of a Statistics resource. There may be zero, one or many
 * instances depending on the statistics type. The ResourceInstance holds
 * an array of the latest stat values.
 * <p/>
 * Extracted from {@link com.gemstone.gemfire.internal.StatArchiveWriter}.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public class ResourceInstance {

  private final int id;
  private final Statistics statistics;
  private final ResourceType type;
  private long[] previousStatValues = null;
  private long[] latestStatValues = null;
  private int[] updatedStats = null;

  public ResourceInstance(int id, Statistics statistics, ResourceType type) {
    this.id = id;
    this.statistics = statistics;
    this.type = type;
  }

  public int getId() {
    return this.id;
  }
  
  public Statistics getStatistics() {
    return this.statistics;
  }
  
  public ResourceType getResourceType() {
    return this.type;
  }

  public Number getStatValue(StatisticDescriptor sd) {
    return this.statistics.get(sd);
  }
  
  public long getRawStatValue(StatisticDescriptor sd) {
    return this.statistics.getRawBits(sd);
  }
  
  public long[] getLatestStatValues() {
    return this.latestStatValues;
  }
  
  public void setLatestStatValues(long[] latestStatValues) {
    this.latestStatValues = latestStatValues;
  }
  
  public long[] getPreviousStatValues() {
    return this.previousStatValues;
  }
  
  public void setPreviousStatValues(long[] previousStatValues) {
    this.previousStatValues = previousStatValues;
  }
  
  public int[] getUpdatedStats() {
    return this.updatedStats;
  }
  
  public void setUpdatedStats(int[] updatedStats) {
    this.updatedStats = updatedStats;
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("id=").append(this.id);
    if (this.updatedStats == null) {
      sb.append(", updatedStats=null");
    } else {
      sb.append(", updatedStats.length=").append(this.updatedStats.length);
    }
    if (this.previousStatValues == null) {
      sb.append(", previousStatValues=null");
    } else {
      sb.append(", previousStatValues.length=").append(this.previousStatValues.length);
    }
    if (this.latestStatValues == null) {
      sb.append(", latestStatValues=null");
    } else {
      sb.append(", latestStatValues.length=").append(this.latestStatValues.length);
    }
    sb.append(", statistics=").append(this.statistics);
    sb.append(", resourceType=").append(this.type);
    sb.append("}");
    return sb.toString();
  }

//  private int unsignedByteToInt(byte value) {
//    return value & 0x000000FF;
//  }
}
