/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.hibernate.internal;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;

public class ReadOnlyAccess extends Access {

  public ReadOnlyAccess(GemFireEntityRegion region) {
    super(region);
  }

  @Override
  public boolean insert(Object key, Object value, Object version)
      throws CacheException {
    throw new UnsupportedOperationException(
        "insert not supported on read only access");
  }

  @Override
  public boolean update(Object key, Object value, Object currentVersion,
      Object previousVersion) throws CacheException {
    throw new UnsupportedOperationException(
        "update not supported on read only access");
  }

  @Override
  public boolean afterInsert(Object key, Object value, Object version)
      throws CacheException {
    throw new UnsupportedOperationException(
        "insert not supported on read only access");
  }

  @Override
  public boolean afterUpdate(Object key, Object value, Object currentVersion,
      Object previousVersion, SoftLock lock) throws CacheException {
    throw new UnsupportedOperationException(
        "update not supported on read only access");
  }
}
