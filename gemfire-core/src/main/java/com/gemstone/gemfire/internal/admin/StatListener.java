/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
package com.gemstone.gemfire.internal.admin;

/**
 * Interface for those who want to be alerted of a change in value of
 * a statistic
 */
public interface StatListener {
  /**
   * Invoked when the value of a statistic has changed
   *
   * @param value
   *        The new value of the statistic
   * @param time
   *        The time at which the statistic's value change was
   *        detected 
   */
  public void statValueChanged( double value, long time );

  /**
   * Invoked when the value of a statistic has not changed
   *
   * @param time
   *        The time of the latest statistic sample
   */
  public void statValueUnchanged( long time );
}
