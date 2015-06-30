/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.concurrent;

/**
 * Any additional result state needed to be passed to {@link MapCallback} which
 * returns values by reference.
 * 
 * @author swale
 * @since Helios
 */
public interface MapResult {

  /**
   * Set whether the result of {@link MapCallback#newValue} created a new value
   * or not. If not, then the result value of newValue is not inserted into the
   * map though it is still returned by the create methods. Default for
   * MapResult is assumed to be true if this method was not invoked by
   * {@link MapCallback} explicitly.
   */
  public void setNewValueCreated(boolean created);

  /**
   * Result set by {@link #setNewValueCreated(boolean)}. Default is required to
   * be true.
   */
  public boolean isNewValueCreated();
}
