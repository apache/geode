/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

/**
 * Utility class that implements all methods in <code>CacheWriter</code>
 * with empty implementations. Applications can subclass this class and
 * only override the methods for the events of interest.
 *
 * @author Eric Zoerner
 *
 * @since 3.0
 */
public class CacheWriterAdapter<K,V> implements CacheWriter<K,V> {

  public void beforeCreate(EntryEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeDestroy(EntryEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeRegionDestroy(RegionEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeRegionClear(RegionEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeUpdate(EntryEvent<K,V> event) throws CacheWriterException {
  }

  public void close() {
  }

}
