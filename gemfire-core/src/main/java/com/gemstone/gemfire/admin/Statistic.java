/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.admin;

/**
 * Interface to represent a single statistic of a <code>StatisticResource</code>
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface Statistic extends java.io.Serializable {
    
  /**
   * Gets the identifying name of this statistic.
   *
   * @return the identifying name of this statistic 
   */
  public String getName();
    
  /**
   * Gets the value of this statistic as a <code>java.lang.Number</code>.
   *
   * @return the value of this statistic
   */
  public Number getValue();
  
  /**
   * Gets the unit of measurement (if any) this statistic represents.
   *
   * @return the unit of measurement (if any) this statistic represents
   */
  public String getUnits();
  
  /**
   * Returns true if this statistic represents a numeric value which always 
   * increases.
   *
   * @return true if this statistic represents a value which always increases
   */
  public boolean isCounter();
  
  /**
   * Gets the full description of this statistic.
   *
   * @return the full description of this statistic
   */
  public String getDescription();
}

