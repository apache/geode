/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/** Thrown when an operation is invoked on <code>Region</code> for an entry that
 * doesn't exist in the <code>Region</code>. This exception is <i>not</i>
 * thrown by {@link com.gemstone.gemfire.cache.Region#get(Object)} or {@link Region#getEntry}.
 *
 * @author Eric Zoerner
 *
 *
 * @see com.gemstone.gemfire.cache.Region#invalidate(Object)
 * @see com.gemstone.gemfire.cache.Region#destroy(Object)
 * @see Region.Entry
 * @since 3.0
 */
public class EntryNotFoundException extends CacheException {
private static final long serialVersionUID = -2404101631744605659L;
  /**
   * Constructs an instance of <code>EntryNotFoundException</code> with the specified detail message.
   * @param msg the detail message
   */
  public EntryNotFoundException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>EntryNotFoundException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public EntryNotFoundException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
