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
 * Identifies a specific instance of a stat as defined by a StatisticDescriptor.
 * <p>
 * A StatisticsType contains any number of StatisticDescriptors. A 
 * StatisticsType may describe one or more Statistics instances, while a 
 * StatisticDescriptor may describe one or more StatisticId instances.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public interface StatisticId {

  /** Returns the descriptor that this stat is an instance of */
  public StatisticDescriptor getStatisticDescriptor();

  /** Returns the statistics instance which owns this stat instance */
  public Statistics getStatistics();
}
