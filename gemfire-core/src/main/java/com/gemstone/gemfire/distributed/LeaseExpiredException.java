/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.GemFireException;

/**
 * A <code>LeaseExpiredException</code> is thrown when GemFire
 * detects that a distributed lock obtained by the current thread
 * with a limited lease (see @link DistributedLockService} has 
 * expired before it was explicitly released.
 */

public class LeaseExpiredException extends GemFireException  {
private static final long serialVersionUID = 6216142987243536540L;

  /**
   * Creates a new <code>LeaseExpiredException</code>
   */
  public LeaseExpiredException(String s) {
    super(s);
  }

}
