/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * Thrown if statistics are requested when statistics are disabled on the
 * region.
 *
 * @author Eric Zoerner
 *
 *
 * @see com.gemstone.gemfire.cache.AttributesFactory#setStatisticsEnabled(boolean)
 * @see com.gemstone.gemfire.cache.RegionAttributes#getStatisticsEnabled
 * @see com.gemstone.gemfire.cache.Region#getStatistics
 * @see com.gemstone.gemfire.cache.Region.Entry#getStatistics
 * @since 3.0
 */
public class InternalStatisticsDisabledException extends GemFireCheckedException {
private static final long serialVersionUID = 4146181546364258311L;
  
  /**
   * Creates a new instance of <code>StatisticsDisabledException</code> without detail message.
   */
  public InternalStatisticsDisabledException() {
  }
  
  
  /**
   * Constructs an instance of <code>StatisticsDisabledException</code> with the specified detail message.
   * @param msg the detail message
   */
  public InternalStatisticsDisabledException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>StatisticsDisabledException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public InternalStatisticsDisabledException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>StatisticsDisabledException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public InternalStatisticsDisabledException(Throwable cause) {
    super(cause);
  }
}
