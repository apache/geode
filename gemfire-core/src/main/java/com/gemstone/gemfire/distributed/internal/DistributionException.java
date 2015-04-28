/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.GemFireException;

/**
 * This exception is thrown when a problem occurs when accessing the
 * underlying distrubtion system (JGroups).  It most often wraps
 * another (checked) exception.
 */
public class DistributionException extends GemFireException {
private static final long serialVersionUID = 9039055444056269504L;

  /**
   * Creates a new <code>DistributionException</code> with the given
   * cause. 
   */
  public DistributionException(String message, Throwable cause) {
    super(message, cause);
  }

}
