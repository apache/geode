/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.GemFireException;

/**
 * A <code>ManagementException</code> is a general exception that may be thrown
 * when any administration or monitoring operation on a GemFire component
 * fails.
 * 
 * Various management and monitoring exceptions are wrapped in
 * <code>ManagementException<code>s.
 * 
 * @author rishim
 * @since 7.0
 * 
 */
public class ManagementException extends GemFireException {

  private static final long serialVersionUID = 879398950879472121L;

  /**
   * Constructs a new exception with a <code>null</code> detail message. The
   * cause is not initialized, and may subsequently be initialized by a call to
   * {@link Throwable#initCause}.
   */
  public ManagementException() {
    super();
  }

  /**
   * Constructs a new exception with the specified detail message. The cause is
   * not initialized and may subsequently be initialized by a call to
   * {@link Throwable#initCause}.
   * 
   * @param message
   *          The detail message.
   */
  public ManagementException(String message) {
    super(message);
  }

  /**
   * Constructs a new ManagementException with the specified detail message and
   * cause.
   * <p>
   * Note that the detail message associated with <code>cause</code> is
   * <i>not</i> automatically incorporated in this runtime exception's detail
   * message.
   * 
   * @param message
   *          The detail message.
   * @param cause
   *          The cause of this exception or <code>null</code> if the cause is
   *          unknown.
   */
  public ManagementException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new ManagementException by wrapping the specified cause. The
   * detail for this exception will be null if the cause is null or
   * cause.toString() if a cause is provided.
   * 
   * @param cause
   *          The cause of this exception or <code>null</code> if the cause is
   *          unknown.
   */
  public ManagementException(Throwable cause) {
    super(cause);
  }
}
