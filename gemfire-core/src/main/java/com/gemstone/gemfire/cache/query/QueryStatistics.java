/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query;

/**
 * Provides statistical information about a query performed on a
 * GemFire <code>Region</code>.
 *
 * @since 4.0
 */
public interface QueryStatistics {

  /**
   * Returns the total number of times the query has been executed.
   */
  public long getNumExecutions();

  /**
   * Returns the total amount of time (in nanoseconds) spent executing
   * the query.
   */
  public long getTotalExecutionTime();

}
