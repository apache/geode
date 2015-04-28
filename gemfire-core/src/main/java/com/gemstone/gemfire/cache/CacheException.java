/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.GemFireException;


/**
 * A generic exception, which indicates
 * a cache error has occurred. All the other cache exceptions are 
 * subclasses of this class. This class is abstract and therefore only
 * subclasses are instantiated.
 *
 * @author Eric Zoerner
 *
 * @since 2.0
 */
public abstract class CacheException extends GemFireException {
  /** Constructs a new <code>CacheException</code>. */
  public CacheException() {
    super();
  }
  
  /** Constructs a new <code>CacheException</code> with a message string. */
  public CacheException(String s) {
    super(s);
  }
  
  /** Constructs a <code>CacheException</code> with a message string and
   * a base exception
   */
  public CacheException(String s, Throwable cause) {
    super(s, cause);
  }
  
  /** Constructs a <code>CacheException</code> with a cause */
  public CacheException(Throwable cause) {
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
