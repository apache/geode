/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.lang;

/**
 * The AttachAPINotFoundException class is a RuntimeException indicating that the JDK tools.jar has not been properly
 * set on the user's classpath
 * <p/>
 * @author John Blum
 * @see java.lang.RuntimeException
 * @since 7.0
 */
@SuppressWarnings("unused")
public class AttachAPINotFoundException extends RuntimeException {

  /**
   * Constructs an instance of the AttachAPINotFoundException class.
   */
  public AttachAPINotFoundException() {
  }

  /**
   * Constructs an instance of the AttachAPINotFoundException class with a description of the problem.
   * <p/>
   * @param message a String describing the nature of the Exception and why it was thrown.
   */
  public AttachAPINotFoundException(final String message) {
    super(message);
  }

  /**
   * Constructs an instance of the AttachAPINotFoundException class with a reference to the underlying Exception
   * causing this Exception to be thrown.
   * <p/>
   * @param cause a Throwable indicating the reason this Exception was thrown.
   */
  public AttachAPINotFoundException(final Throwable cause) {
    super(cause);
  }

  /**
   * Constructs an instance of the AttachAPINotFoundException class with a reference to the underlying Exception
   * causing this Exception to be thrown in addition to a description of the problem.
   * <p/>
   * @param message a String describing the nature of the Exception and why it was thrown.
   * @param cause a Throwable indicating the reason this Exception was thrown.
   */
  public AttachAPINotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
