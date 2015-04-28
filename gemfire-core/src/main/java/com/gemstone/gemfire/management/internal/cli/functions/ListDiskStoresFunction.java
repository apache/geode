/*
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 */

package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.management.internal.cli.domain.DiskStoreDetails;

/**
 * The ListDiskStoresFunction class is an implementation of GemFire Function interface used to determine all the
 * disk stores that exist for the entire cache, distributed across the GemFire distributed system.
 * </p>
 * @author jblum
 * @see com.gemstone.gemfire.cache.DiskStore
 * @see com.gemstone.gemfire.cache.execute.Function
 * @see com.gemstone.gemfire.cache.execute.FunctionAdapter
 * @see com.gemstone.gemfire.cache.execute.FunctionContext
 * @see com.gemstone.gemfire.internal.InternalEntity
 * @see com.gemstone.gemfire.management.internal.cli.domain.DiskStoreDetails
 * @since 7.0
 */
public class ListDiskStoresFunction extends FunctionAdapter implements InternalEntity {

  @SuppressWarnings("unused")
  public void init(final Properties props) {
  }

  public String getId() {
    return getClass().getName();
  }

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public void execute(final FunctionContext context) {
    final Set<DiskStoreDetails> memberDiskStores = new HashSet<DiskStoreDetails>();

    try {
      final Cache cache = getCache();

      if (cache instanceof InternalCache) {
        final InternalCache gemfireCache = (InternalCache) cache;

        final DistributedMember member = gemfireCache.getMyId();

        for (final DiskStore memberDiskStore : gemfireCache.listDiskStoresIncludingRegionOwned()) {
          memberDiskStores.add(new DiskStoreDetails(memberDiskStore.getDiskStoreUUID(), memberDiskStore.getName(),
            member.getId(), member.getName()));
        }
      }

      context.getResultSender().lastResult(memberDiskStores);
    }
    catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }

}
