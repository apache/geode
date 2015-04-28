/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DynamicRegionFactory;

/** This class provides non-published methods that allow the cache
    to initialize and close the factory.

    @author Bruce Schuchardt
    @since 4.3
 */
public class DynamicRegionFactoryImpl extends DynamicRegionFactory {
  /** create an instance of the factory.  This is normally only done
      by DynamicRegionFactory's static initialization
   */
  public DynamicRegionFactoryImpl() {
  }
  
  /** close the factory.  Only do this if you're closing the cache, too */
  public void close() {
    _close();
  }
  
  /** initialize the factory for use with a new cache */
  public void internalInit( GemFireCacheImpl c ) throws CacheException {
    _internalInit(c);
  }
}
