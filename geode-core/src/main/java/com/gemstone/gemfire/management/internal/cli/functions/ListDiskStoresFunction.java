/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * @see com.gemstone.gemfire.cache.DiskStore
 * @see com.gemstone.gemfire.cache.execute.Function
 * @see com.gemstone.gemfire.cache.execute.FunctionAdapter
 * @see com.gemstone.gemfire.cache.execute.FunctionContext
 * @see com.gemstone.gemfire.internal.InternalEntity
 * @see com.gemstone.gemfire.management.internal.cli.domain.DiskStoreDetails
 * @since GemFire 7.0
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
