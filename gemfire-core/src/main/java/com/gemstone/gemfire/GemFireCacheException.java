/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import com.gemstone.gemfire.cache.CacheException;

/**
 * An <code>GemFireCacheException</code> is used to wrap a
 * {@link CacheException}. This is needed in contexts that can
 * not throw the cache exception directly because of it being
 * a typed exception.
 */
public class GemFireCacheException extends GemFireException {
private static final long serialVersionUID = -2844020916351682908L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>GemFireCacheException</code>.
   */
  public GemFireCacheException(String message, CacheException ex) {
    super(message, ex);
  }
  /**
   * Creates a new <code>GemFireCacheException</code>.
   */
  public GemFireCacheException(CacheException ex) {
    super(ex);
  }
  /**
   * Gets the wrapped {@link CacheException}
   */
  public CacheException getCacheException() {
    return (CacheException)getCause();
  }
}
