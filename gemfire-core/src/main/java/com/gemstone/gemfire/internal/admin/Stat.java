/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

/**
 * Interface to represent a single statistic of a <code>StatResource</code>
 *
 * @author Darrel Schneider
 * @author Kirk Lund
 */
public interface Stat extends GfObject {
    
  /**
   * @return the value of this stat as a <code>java.lang.Number</code> 
   */
  public Number getValue();
  
  /**
   * @return a display string for the unit of measurement (if any) this stat represents
   */
  public String getUnits();
  
  /**
   * @return true if this stat represents a numeric value which always increases
   */
  public boolean isCounter();
  
}

