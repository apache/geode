/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * This exception is meant to represent the class of exceptions that occur
 * when our assumption about where the data lives is incorrect.
 * @see ForceReattemptException
 * @see PrimaryBucketException
 * @author sbawaska
 */
public abstract class DataLocationException extends GemFireCheckedException {
  public DataLocationException() {
  }

  public DataLocationException(String msg) {
    super(msg);
  }

  public DataLocationException(Throwable cause) {
    super(cause);
  }

  public DataLocationException(String message, Throwable cause) {
    super(message, cause);
  }
}
