/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

/**
 * A snapshot of a <code>Region</code> entry.
 */
public interface EntrySnapshot extends CacheSnapshot {

  /**
   * Returns the value of the <code>Region</code> entry
   */
  public Object getValue();
}
