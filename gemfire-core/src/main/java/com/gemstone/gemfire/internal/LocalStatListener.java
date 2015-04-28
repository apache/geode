/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal;

/**
 * Implement this interface to receive call back when a stat value has changed.
 * The listener has to be registered with statSampler. This can be done in the
 * following manner:
 * <code>
 * InternalDistributedSystem internalSystem = (InternalDistributedSystem)cache
 *                                                      .getDistributedSystem();
 * final GemFireStatSampler sampler = internalSystem.getStatSampler();          
 * sampler.addLocalStatListener(l, stats, statName);
 * </code>
 * 
 * @author sbawaska
 *
 */
public interface LocalStatListener {
  /**
   * Invoked when the value of a statistic has changed
   *
   * @param value
   *        The new value of the statistic 
   */
  public void statValueChanged( double value );
}
