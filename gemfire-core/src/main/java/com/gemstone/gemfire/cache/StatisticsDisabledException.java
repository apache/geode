/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * Thrown if statistics are requested when statistics are disabled on the
 * region.
 *
 * @author Eric Zoerner
 *
 *
 * @see AttributesFactory#setStatisticsEnabled
 * @see RegionAttributes#getStatisticsEnabled
 * @see Region#getStatistics
 * @see Region.Entry#getStatistics
 * @since 3.0
 */
public class StatisticsDisabledException extends CacheRuntimeException {
private static final long serialVersionUID = -2987721454129719551L;
  
  /**
   * Creates a new instance of <code>StatisticsDisabledException</code> without detail message.
   */
  public StatisticsDisabledException() {
  }
  
  
  /**
   * Constructs an instance of <code>StatisticsDisabledException</code> with the specified detail message.
   * @param msg the detail message
   */
  public StatisticsDisabledException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>StatisticsDisabledException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public StatisticsDisabledException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>StatisticsDisabledException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public StatisticsDisabledException(Throwable cause) {
    super(cause);
  }
}
