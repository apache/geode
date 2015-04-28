/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

/**
 * A version of ForceReattemptException that should be used when the
 * target bucket can't be found.
 * 
 * @author bruce schuchardt
 *
 */
public class BucketNotFoundException extends ForceReattemptException {

  /**
   * @param message
   */
  public BucketNotFoundException(String message) {
    super(message);
  }

}
