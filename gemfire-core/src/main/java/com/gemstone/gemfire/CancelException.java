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
package com.gemstone.gemfire;

import com.gemstone.gemfire.cache.CacheRuntimeException;

/**
 * Abstract root class of all GemFire exceptions representing system
 * cancellation
 * 
 * @author jpenney
 * @since 6.0
 */
public abstract class CancelException extends CacheRuntimeException {

  /**
   * for serialization
   */
  public CancelException() {
  }

  /**
   * Create instance with given message
   * @param message the message
   */
  public CancelException(String message) {
    super(message);
  }

  /**
   * Create instance with given message and cause
   * @param message the message
   * @param cause the cause
   */
  public CancelException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create instance with empty message and given cause
   * @param cause the cause
   */
  public CancelException(Throwable cause) {
    super(cause);
  }

}
