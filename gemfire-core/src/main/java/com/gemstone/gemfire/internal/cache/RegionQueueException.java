/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CacheException;
/**
 * An exception thrown by a <code>RegionQueue</code>.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 */
// Since this exception is in an internal package, we make it
// a checked exception.
public class RegionQueueException extends CacheException {
private static final long serialVersionUID = 4159307586325821105L;

  /**
   * Required for serialization
   */
  public RegionQueueException() {
  }

  /**
   * Constructor.
   * Creates an instance of <code>RegionQueueException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public RegionQueueException(String msg) {
    super(msg);
  }

}
