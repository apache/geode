/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.statalerts;

import com.gemstone.gemfire.DataSerializable;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;

/**
 * Provides informations of the statistic
 * 
 * @author hgadre
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
