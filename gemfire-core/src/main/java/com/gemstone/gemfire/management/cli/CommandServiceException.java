/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.cli;

/**
 * Indicates that an exception occurred while accessing/creating a Command
 * Service for processing GemFire Command Line Interface (CLI) commands.
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class CommandServiceException extends Exception {

  private static final long serialVersionUID = 7316102209844678329L;

  /**
   * Constructs a new <code>CommandServiceException</code> with the specified
   * detail message and cause.
   * 
   * @param message
   *          The detail message.
   * @param cause
   *          The cause of this exception or <code>null</code> if the cause is
   *          unknown.
   */
  public CommandServiceException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new <code>CommandServiceException</code> with the specified detail 
   * message.
   * 
   * @param message
   *          The detail message.
   */
  public CommandServiceException(String message) {
    super(message);
  }

  /**
   * Constructs a new <code>CommandServiceException</code> by wrapping the
   * specified cause. The detail for this exception will be null if the cause is
   * null or cause.toString() if a cause is provided.
   * 
   * @param cause
   *          The cause of this exception or <code>null</code> if the cause is
   *          unknown.
   */
  public CommandServiceException(Throwable cause) {
    super(cause);
  }
}
