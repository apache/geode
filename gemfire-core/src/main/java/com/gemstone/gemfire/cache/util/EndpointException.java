/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.util;

/**
 * An <code>EndpointException</code> is a generic exception that indicates
 * a client <code>Endpoint</code> exception has occurred. All other
 * <code>Endpoint</code> exceptions are subclasses of this class. Since
 * this class is abstract, only subclasses are instantiated.
 *
 * @author Barry Oglesby
 *
 * @since 5.0.2
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */
@Deprecated
public abstract class EndpointException extends Exception {
  
  /** Constructs a new <code>EndpointException</code>. */
  public EndpointException() {
    super();
  }

  /** Constructs a new <code>EndpointException</code> with a message string. */
  public EndpointException(String s) {
    super(s);
  }

  /** Constructs a <code>EndpointException</code> with a message string and
   * a base exception
   */
  public EndpointException(String s, Throwable cause) {
    super(s, cause);
  }

  /** Constructs a <code>EndpointException</code> with a cause */
  public EndpointException(Throwable cause) {
    super(cause);
  }

  @Override
  public String toString() {
    String result = super.toString();
    Throwable cause = getCause();
    if (cause != null) {
      String causeStr = cause.toString();
      final String glue = ", caused by ";
      StringBuffer sb = new StringBuffer(result.length() + causeStr.length() + glue.length());
      sb.append(result)
        .append(glue)
        .append(causeStr);
      result = sb.toString();
    }
    return result;
  }
}
