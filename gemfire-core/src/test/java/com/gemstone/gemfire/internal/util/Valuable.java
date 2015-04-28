/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

/**
 * Classes that implement this interface have a <code>Object</code>
 * value associated with them.  This interface is not considered to be
 * a "user class".
 *
 * @author David Whitlock
 *
 * @since 2.0.3
 */
public interface Valuable {

  /**
   * Returns the value associated with this object
   */
  public Object getValue();

  /**
   * Sets the value associated with this object
   */
  public void setValue(Object value);

}
