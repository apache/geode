/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.hibernate.internal;

import org.hibernate.cache.CacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadWriteAccess extends Access {

  private Logger log = LoggerFactory.getLogger(getClass());

  public ReadWriteAccess(GemFireEntityRegion region) {
    super(region);
  }

  @Override
  public boolean update(Object key, Object value, Object currentVersion,
      Object previousVersion) throws CacheException {
    return super.update(key, value, currentVersion, previousVersion);
  }
}
