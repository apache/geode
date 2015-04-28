/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.shell;

/**
 * RuntimeException to wrap all the exception that could occur for JMX
 * operations/attributes.
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class JMXInvocationException extends RuntimeException {
  
  private static final long serialVersionUID = -4265451314790394366L;

  public JMXInvocationException(String message, Throwable cause) {
    super(message, cause);
  }

  public JMXInvocationException(String message) {
    super(message);
  }

  public JMXInvocationException(Throwable cause) {
    super(cause);
  }

}
