/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.hibernate.internal;

import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.CollectionRegion;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cache.access.CollectionRegionAccessStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.hibernate.GemFireRegionFactory;

public class GemFireCollectionRegion extends GemFireBaseRegion implements CollectionRegion {

  private Logger log = LoggerFactory.getLogger(getClass());
  
  public GemFireCollectionRegion(Region<Object, EntityWrapper> region,
      boolean isClient, CacheDataDescription metadata,
      GemFireRegionFactory regionFactory) {
    super(region, isClient, metadata, regionFactory);
  }

  @Override
  public boolean isTransactionAware() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public CacheDataDescription getCacheDataDescription() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CollectionRegionAccessStrategy buildAccessStrategy(
      AccessType accessType) throws CacheException {
    log.debug("creating collection access for region:"+this.region.getName());
    return new CollectionAccess(this);
  }

}
