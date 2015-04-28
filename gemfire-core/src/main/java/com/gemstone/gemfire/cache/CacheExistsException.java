/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;


/** Thrown when attempting to create a {@link Cache} if one already exists.
 *
 * @author Darrel Schneider
 *
 * @see CacheFactory#create
 * @since 3.0
 */
public class CacheExistsException extends CacheException {
private static final long serialVersionUID = 4090002289325418100L;

  /** The <code>Cache</code> that already exists */
  private final transient Cache cache;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Constructs an instance of <code>CacheExistsException</code> with the specified detail message.
   * @param msg the detail message
   */
  public CacheExistsException(Cache cache, String msg) {
    super(msg);
    this.cache = cache;
  }
  
  /**
   * Constructs an instance of <code>CacheExistsException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CacheExistsException(Cache cache, String msg, Throwable cause) {
    super(msg, cause);
    this.cache = cache;
  }

  ///////////////////////  Instance Methods  ///////////////////////

  /**
   * Returns the <code>Cache</code> that already exists.
   *
   * @since 4.0
   */
  public Cache getCache() {
    return this.cache;
  }
}
