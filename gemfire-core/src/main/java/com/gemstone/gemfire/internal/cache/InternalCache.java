/*
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 */

package com.gemstone.gemfire.internal.cache;

import java.util.Collection;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * The InternalCache interface is contract for implementing classes for defining internal cache operations that should
 * not be part of the "public" API of the implementing class.
 * </p>
 * @author jblum
 * @see com.gemstone.gemfire.cache.Cache
 * @since 7.0
 */
public interface InternalCache extends Cache {

  public DistributedMember getMyId();

  public Collection<DiskStoreImpl> listDiskStores();

  public Collection<DiskStoreImpl> listDiskStoresIncludingDefault();

  public Collection<DiskStoreImpl> listDiskStoresIncludingRegionOwned();

  public CqService getCqService();
  
}
