/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal;

import java.util.List;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;

/**
 * Defines the contract that a statistics factory must implement
 * for its Statistics instances. This is an internal implementation
 * specific interface.
 *
 * @author Darrel Schneider
 *
 */
public interface StatisticsManager extends StatisticsFactory {
  /**
   * Called when the Statistics instance <code>s</code> is closed.
   */
  public void destroyStatistics(Statistics s);
  /**
   * Returns a name that can be used to identify the manager
   */
  public String getName();
  /**
   * Returns a numeric id that can be used to identify the manager
   */
  public long getId();
  /**
   * Returns the start time of this manager.
   */
  public long getStartTime();
  /**
   * Returns a value that changes any time a Statistics instance is added
   * or removed from this manager.
   */
  public int getStatListModCount();
  /**
   * Returns a list of all the Statistics this manager is currently managing.
   */
  public List<Statistics> getStatsList();
  
  /**
   * Returns the current number of statistics instances.
   */
  public int getStatisticsCount();

  /**
   * Returns the statistics resource instance given its id.
   */
  public Statistics findStatistics(long id);

  /**
   * Returns true if the specified statistic resource still exists.
   */
  public boolean statisticsExists(long id);

  /**
   * Returns an array of all the current statistic resource instances.
   */
  public Statistics[] getStatistics();
}
