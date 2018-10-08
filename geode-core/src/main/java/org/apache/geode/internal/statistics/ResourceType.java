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
import org.apache.geode.StatisticsType;

/**
 * An instance of a StatisticsType which describes the individual stats for each ResourceInstance.
 * The ResourceType holds an array of StatisticDescriptors for its StatisticsType.
 * <p/>
 * Extracted from {@link StatArchiveWriter}.
 *
 * @since GemFire 7.0
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
    // if (this.stats.length >= ILLEGAL_STAT_OFFSET) {
    // throw new
    // InternalGemFireException(String.format("Could not archive type %s because it had more than %s
    // statistics.",new
    // Object[] {type.getName(), Integer.valueOf(ILLEGAL_STAT_OFFSET-1)}));
    // }
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
