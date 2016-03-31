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

import com.gemstone.gemfire.DataSerializable;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;

/**
 * Provides informations of the statistic
 * 
 * 
 */
public interface StatisticInfo extends DataSerializable {

  /**
   * @return name of statistics
   */
  public String getStatisticName();

  /**
   * 
   * @param statisticName
   *                Name of statistic
   */
  public void setStatisticName(String statisticName);

  /**
   * 
   * @return instance of statistic descriptor
   */
  public StatisticDescriptor getStatisticDescriptor();

  /**
   * @return text id of the associated {@link Statistics}
   */
  public String getStatisticsTextId();

  /**
   * @param statisticsTextId
   *                Text id of the associated {@link Statistics}
   */
  public void setStatisticsTextId(String statisticsTextId);

  /**
   * @return instance of associated {@link Statistics}
   */
  public Statistics getStatistics();

  /**
   * 
   * @return associated {@link StatisticsType}
   */
  public String getStatisticsTypeName();

  /**
   * 
   * @param statisticsType
   *                Associated {@link StatisticsType}
   */
  public void setStatisticsTypeName(String statisticsType);

  /**
   * 
   * @return value of statistic
   */
  public Number getValue();
}
