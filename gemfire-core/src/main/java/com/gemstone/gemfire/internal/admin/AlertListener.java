/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin;

/**
 * A listener whose callback methods are invoekd when an {@link Alert}
 * is received.
 */
public interface AlertListener extends java.util.EventListener {

  /**
   * Invoked when an <code>Alert</code> is received.
   */
  public void alert( Alert alert );
}
