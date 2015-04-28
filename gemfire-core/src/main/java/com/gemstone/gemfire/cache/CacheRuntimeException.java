/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.GemFireException;

/** A generic runtime exception that indicates
 * a cache error has occurred. All the other runtime cache exceptions are the
 * subclass of this class. This class is abstract so only subclasses can be
 * instantiated
 *
 * @author Eric Zoerner
 *
 * @since 3.0
 */
public abstract class CacheRuntimeException extends GemFireException {
  
  /**
   * Creates a new instance of <code>CacheRuntimeException</code> without detail message.
   */
  public CacheRuntimeException() {
  }
  
  
  /**
   * Constructs an instance of <code>CacheRuntimeException</code> with the specified detail message.
   * @param msg the detail message
   */
  public CacheRuntimeException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>CacheRuntimeException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CacheRuntimeException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>CacheRuntimeException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public CacheRuntimeException(Throwable cause) {
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
