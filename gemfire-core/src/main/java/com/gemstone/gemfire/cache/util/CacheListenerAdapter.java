/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

/**
 * <p>Utility class that implements all methods in <code>CacheListener</code>
 * with empty implementations. Applications can subclass this class and only
 * override the methods for the events of interest.<p>
 * 
 * <p>Subclasses declared in a Cache XML file, it must also implement {@link Declarable}
 * </p>
 * 
 * @author Eric Zoerner
 * 
 * @since 3.0
 */
public abstract class CacheListenerAdapter<K,V> implements CacheListener<K,V> {

  public void afterCreate(EntryEvent<K,V> event) {
  }

  public void afterDestroy(EntryEvent<K,V> event) {
  }

  public void afterInvalidate(EntryEvent<K,V> event) {
  }

  public void afterRegionDestroy(RegionEvent<K,V> event) {
  }
  
  public void afterRegionCreate(RegionEvent<K,V> event) {
  }
  
  public void afterRegionInvalidate(RegionEvent<K,V> event) {
  }

  public void afterUpdate(EntryEvent<K,V> event) {
  }

  public void afterRegionClear(RegionEvent<K,V> event) {
  }

  public void afterRegionLive(RegionEvent<K,V> event) {
  }
  
  public void close() {
  }
}
