/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.AbstractStatisticsFactory;
import com.gemstone.gemfire.internal.OsStatisticsFactory;
import com.gemstone.gemfire.internal.StatisticsManager;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class TestStatisticsManager extends AbstractStatisticsFactory 
    implements StatisticsManager, OsStatisticsFactory {

  public TestStatisticsManager(long id, String name, long startTime) {
    super(id, name, startTime);
  }

  @Override
  public Statistics createOsStatistics(StatisticsType type, String textId,
      long numericId, int osStatFlags) {
    // TODO ?
    return null;
  }
}
