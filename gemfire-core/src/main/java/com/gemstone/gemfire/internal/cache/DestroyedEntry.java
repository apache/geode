/*=========================================================================
 * Copyright (c) 2012 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CacheStatistics;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;

/**
 * Represents a destroyed entry that can be returned from an <code>Iterator</code>
 * instead of null.  All methods throw {@link EntryDestroyedException} except for
 * {@link #isDestroyed()}.
 * 
 * @author bakera
 */
public class DestroyedEntry implements Region.Entry<Object, Object> {
  private final String msg;
  
  public DestroyedEntry(String msg) {
    this.msg = msg;
  }
  
  @Override
  public Object getKey() {
    throw entryDestroyed();
  }

  @Override
  public Object getValue() {
    throw entryDestroyed();
  }

  @Override
  public Region<Object, Object> getRegion() {
    throw entryDestroyed();
  }

  @Override
  public boolean isLocal() {
    throw entryDestroyed();
  }

  @Override
  public CacheStatistics getStatistics() {
    throw entryDestroyed();
  }

  @Override
  public Object getUserAttribute() {
    throw entryDestroyed();
  }

  @Override
  public Object setUserAttribute(Object userAttribute) {
    throw entryDestroyed();
  }

  @Override
  public boolean isDestroyed() {
    return true;
  }

  @Override
  public Object setValue(Object value) {
    throw entryDestroyed();
  }

  private EntryDestroyedException entryDestroyed() {
    return new EntryDestroyedException(msg);
  }
}
