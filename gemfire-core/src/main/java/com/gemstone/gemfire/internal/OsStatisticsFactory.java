/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.*;

/**
 * Instances of this interface provide methods that create operating system
 * instances of {@link com.gemstone.gemfire.Statistics}. Its for internal use 
 * only.
 *
 * {@link com.gemstone.gemfire.distributed.DistributedSystem} is an OS 
 * statistics factory.
 * <P>
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 * @author Darrel Schneider
 *
 * @since 3.0
 */
public interface OsStatisticsFactory {
  /**
   * Creates and returns a OS {@link Statistics} instance of the given {@link StatisticsType type}, <code>textId</code>, <code>numericId</code>, and <code>osStatFlags</code>..
   * <p>
   * The created instance may not be {@link Statistics#isAtomic atomic}.
   */
  public Statistics createOsStatistics(StatisticsType type, String textId, long numericId, int osStatFlags);
}
