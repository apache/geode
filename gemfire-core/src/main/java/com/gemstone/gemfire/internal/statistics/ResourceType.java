/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;

/**
 * An instance of a StatisticsType which describes the individual stats for
 * each ResourceInstance. The ResourceType holds an array of 
 * StatisticDescriptors for its StatisticsType.
 * <p/>
 * Extracted from {@link com.gemstone.gemfire.internal.StatArchiveWriter}.
 *  
 * @author Kirk Lund
 * @since 7.0
 */
public class ResourceType {
  
  private final int id;
  private final StatisticDescriptor[] statisticDescriptors;
  private final StatisticsType statisticsType;

  public ResourceType(int id, StatisticsType type) {
    this.id = id;
    this.statisticDescriptors = type.getStatistics();
    this.statisticsType = type;
    // moved to StatArchiveWriter->SampleHandler#handleNewResourceType
//    if (this.stats.length >= ILLEGAL_STAT_OFFSET) {
//      throw new InternalGemFireException(LocalizedStrings.StatArchiveWriter_COULD_NOT_ARCHIVE_TYPE_0_BECAUSE_IT_HAD_MORE_THAN_1_STATISTICS.toLocalizedString(new Object[] {type.getName(), Integer.valueOf(ILLEGAL_STAT_OFFSET-1)}));
//    }
  }

  public int getId() {
    return this.id;
  }
  
  public StatisticDescriptor[] getStatisticDescriptors() {
    return this.statisticDescriptors;
  }
  
  public StatisticsType getStatisticsType() {
    return this.statisticsType;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("id=").append(this.id);
    sb.append(", statisticDescriptors.length=").append(this.statisticDescriptors.length);
    sb.append(", statisticsType=").append(this.statisticsType);
    sb.append("}");
    return sb.toString();
  }
}
