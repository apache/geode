/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.GemFireException;

/**
 * An exception thrown if a bucket instance is not primary yet was requested
 * to perform a modification operation. 
 *
 * @author Mitch Thomas
 * @since 5.1
 *
 */
public class PrimaryBucketException extends GemFireException
{
  private static final long serialVersionUID = 1L;

  public PrimaryBucketException() {
    super();
  }

  public PrimaryBucketException(String message) {
    super(message);
  }

  public PrimaryBucketException(Throwable cause) {
    super(cause);
  }

  public PrimaryBucketException(String message, Throwable cause) {
    super(message, cause);
  }

}
