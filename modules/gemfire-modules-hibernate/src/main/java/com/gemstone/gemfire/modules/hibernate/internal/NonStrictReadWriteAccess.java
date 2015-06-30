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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonStrictReadWriteAccess extends Access {

  private Logger log = LoggerFactory.getLogger(getClass());

  public NonStrictReadWriteAccess(GemFireEntityRegion region) {
    super(region);
  }

  @Override
  public SoftLock lockItem(Object key, Object version) throws CacheException {
    log.debug("lock item called for key {}", key);
    return null;
  }

  @Override
  public boolean afterUpdate(Object key, Object value, Object currentVersion,
      Object previousVersion, SoftLock lock) throws CacheException {
    log.debug("after update called for key: {} value: {}", key, value);
    getGemFireRegion().put(getWrappedKey(key), new EntityWrapper(value, -1L));
    return true;
  }
  
  @Override
  public boolean update(Object key, Object value, Object currentVersion,
      Object previousVersion) throws CacheException {
    log.debug("updating key: {} value: {}", key, value);
    getGemFireRegion().put(getWrappedKey(key), new EntityWrapper(value, -1L));
    return true;
  }
//  
//  @Override
//  public boolean insert(Object key, Object value, Object version)
//      throws CacheException {
//    log.debug("inserting key:{} value:{}", key, value);
//    getGemFireRegion().put(key, new EntityWrapper(value, -1L));
//    return true;
//  }
//  
//  @Override
//  public boolean afterInsert(Object key, Object value, Object version)
//      throws CacheException {
//    log.debug("after insert called for key:{} value:{}", key, value);
//    getGemFireRegion().put(key, new EntityWrapper(value, -1L));
//    return true;
//  }
//  
  @Override
  public boolean putFromLoad(Object key, Object value, long txTimestamp,
      Object version) throws CacheException {
    return putFromLoad(key, value, txTimestamp, version, true);
  }
  
  @Override
  public boolean putFromLoad(Object key, Object value, long txTimestamp,
      Object version, boolean minimalPutOverride) throws CacheException {
    log.debug("putting a new entry from load key:{} value:{}", key, value);
    getGemFireRegion().put(getWrappedKey(key), new EntityWrapper(value, -1L));
    return true;
  }
}
