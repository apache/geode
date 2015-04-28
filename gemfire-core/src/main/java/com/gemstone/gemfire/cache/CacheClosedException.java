/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * Indicates that the caching system has 
 * been closed. Can be thrown from almost any method related to regions or the
 * <code>Cache</code> after the cache has been closed.
 *
 * @author Eric Zoerner
 *
 *
 * @see Cache
 * @since 3.0
 */
public class CacheClosedException extends CancelException {
private static final long serialVersionUID = -6479561694497811262L;
  
  /**
   * Constructs a new <code>CacheClosedException</code>.
   */
  public CacheClosedException() {
    super();
  }
  
  /**
   * Constructs a new <code>CacheClosedException</code> with a message string.
   *
   * @param msg a message string
   */
  public CacheClosedException(String msg) {
    super(msg);
    // bug #43108 - CacheClosedException should include cause of closure
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      initCause(cache.getDisconnectCause());
    }
  }
  
  /**
   * Constructs a new <code>CacheClosedException</code> with a message string
   * and a cause.
   *
   * @param msg the message string
   * @param cause a causal Throwable
   */
  public CacheClosedException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs a new <code>CacheClosedException</code> with a cause.
   *
   * @param cause a causal Throwable
   */
  public CacheClosedException(Throwable cause) {
    super(cause);
  }
}

