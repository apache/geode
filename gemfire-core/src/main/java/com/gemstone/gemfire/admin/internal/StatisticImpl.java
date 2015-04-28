/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.internal.admin.Stat;

/**
 * Implementation of a single statistic in a <code>StatisticResource</code>
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 */
public class StatisticImpl
implements com.gemstone.gemfire.admin.Statistic {

  private static final long serialVersionUID = 3899296873901634399L;
  
  private Stat internalStat;

  protected StatisticImpl() {
  }

  protected StatisticImpl(Stat internalStat) {
    this.internalStat = internalStat;
  }
    
  /**
   * @return the identifying name of this stat 
   */
  public String getName() {
    return this.internalStat.getName();
  }
  
  /**
   * @return the value of this stat as a <code>java.lang.Number</code> 
   */
  public Number getValue() {
    return this.internalStat.getValue();
  }
  
  /**
   * @return a display string for the unit of measurement (if any) this stat represents
   */
  public String getUnits() {
    return this.internalStat.getUnits();
  }
  
  /**
   * @return true if this stat represents a numeric value which always increases
   */
  public boolean isCounter() {
    return this.internalStat.isCounter();
  }
  
  /**
   * @return the full description of this stat
   */
  public String getDescription() {
    return this.internalStat.getDescription();
  }
  
  /**
   * Sets the internal stat which allows us to reuse the wrapper object and
   * handle refreshes along with isWriteable set to false on the attribute.
   */
  protected void setStat(Stat internalStat) {
    this.internalStat = internalStat;
  }
    
	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 */
  @Override
	public String toString() {
		return getName();
	}
  
}

