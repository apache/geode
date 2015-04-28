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
public final class SimpleStatisticId implements StatisticId {

  private final StatisticDescriptor descriptor;
  private final Statistics statistics;
  
  protected SimpleStatisticId(StatisticDescriptor descriptor, Statistics statistics) {
    this.descriptor = descriptor;
    this.statistics = statistics;
  }
  
  @Override
  public StatisticDescriptor getStatisticDescriptor() {
    return this.descriptor;
  }

  @Override
  public Statistics getStatistics() {
    return this.statistics;
  }
  
  /**
   * Object equality must be based on instance identity.
   */
  @Override
  public final boolean equals(Object obj) {
    return super.equals(obj);
  }

  /**
   * Object equality must be based on instance identity.
   */
  @Override
  public final int hashCode() {
    return super.hashCode();
  }
}
