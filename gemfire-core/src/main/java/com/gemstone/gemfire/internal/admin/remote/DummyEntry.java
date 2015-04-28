/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.cache.*;

/**
 * This implementation of {@link com.gemstone.gemfire.cache.Region.Entry}
 * does nothing but provide an instance of {@link com.gemstone.gemfire.cache.CacheStatistics}
 */
public class DummyEntry implements Region.Entry {
  
  private final Region region;
  private final Object key;
  private final Object value;
  private final CacheStatistics stats;
  private final Object userAttribute;

  DummyEntry(Region region, Object key, Object cachedObject,
             Object userAttribute, CacheStatistics stats) {
    this.region = region;
    this.key = key;
    this.value = cachedObject;
    this.userAttribute = userAttribute;
    this.stats = stats;
  }
  
  public boolean isLocal() {
    return false;
  }

  public Object getKey() {
    return this.key;
  }
  
  public Object getValue() {
    return this.value;
  }
  
  public Region getRegion() {
    return this.region;
  }
  
  public CacheStatistics getStatistics() {
    return this.stats;
  }
  
  public Object getUserAttribute() {
    return this.userAttribute;
  }
  
  public Object setUserAttribute(Object userAttribute) {
    throw new UnsupportedOperationException();
  }
  
  public boolean isDestroyed() {
    throw new UnsupportedOperationException();
  }

  public Object setValue(Object arg0) {
    throw new UnsupportedOperationException();
  }
}
