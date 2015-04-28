/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * Defines callbacks that are invoked when a <code>GemFireCache</code> is 
 * created or closed.
 * 
 * @author Kirk Lund
 * @see GemFireCacheImpl#addCacheLifecycleListener(CacheLifecycleListener)
 * @see GemFireCacheImpl#removeCacheLifecycleListener(CacheLifecycleListener)
 */
public interface CacheLifecycleListener {

  /**
   * Invoked when a new <code>GemFireCache</code> is created
   */
  public void cacheCreated(GemFireCacheImpl cache);
  /**
   * Invoked when a <code>GemFireCache</code> is closed
   */
  public void cacheClosed(GemFireCacheImpl cache);
}
