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

/**
 * This exception wraps any checked exception encountered during invocation of
 * {@link Delta#fromDelta(java.io.DataInput)} or
 * {@link Delta#toDelta(java.io.DataOutput)} in GemFire.
 * 
 * @since 6.5
 */
public class DeltaSerializationException extends RuntimeException {

  /**
   * Default constructor
   */
  public DeltaSerializationException() {
  }

  /**
   * @param message
   */
  public DeltaSerializationException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public DeltaSerializationException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public DeltaSerializationException(String message, Throwable cause) {
    super(message, cause);
  }

}
