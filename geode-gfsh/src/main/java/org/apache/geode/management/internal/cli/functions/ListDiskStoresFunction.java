/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.management.internal.cli.functions;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;

/**
 * The ListDiskStoresFunction class is an implementation of GemFire Function interface used to
 * determine all the disk stores that exist for the entire cache, distributed across the GemFire
 * distributed system.
 * </p>
 *
 * @see org.apache.geode.cache.DiskStore
 * @see org.apache.geode.cache.execute.Function
 * @see org.apache.geode.cache.execute.FunctionContext
 * @see org.apache.geode.internal.InternalEntity
 * @see org.apache.geode.management.internal.cli.domain.DiskStoreDetails
 * @since GemFire 7.0
 */
public class ListDiskStoresFunction implements InternalFunction<Void> {

  @SuppressWarnings("unused")
  public void init(final Properties props) {}

  @Override
  public String getId() {
    return getClass().getName();
  }

  @Override
  public void execute(final FunctionContext<Void> context) {
    final Set<DiskStoreDetails> memberDiskStores = new HashSet<>();

    try {
      final Cache cache = context.getCache();

      if (cache instanceof InternalCache) {
        final InternalCache gemfireCache = (InternalCache) cache;

        final DistributedMember member = gemfireCache.getMyId();

        for (final DiskStore memberDiskStore : gemfireCache.listDiskStoresIncludingRegionOwned()) {
          memberDiskStores.add(new DiskStoreDetails(memberDiskStore.getDiskStoreUUID(),
              memberDiskStore.getName(), member.getId(), member.getName()));
        }
      }

      context.getResultSender().lastResult(memberDiskStores);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }
}
