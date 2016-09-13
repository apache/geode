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

package org.apache.geode.internal.cache;

import java.util.Collection;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.extension.Extensible;

/**
 * The InternalCache interface is contract for implementing classes for defining internal cache operations that should
 * not be part of the "public" API of the implementing class.
 * </p>
 * @see org.apache.geode.cache.Cache
 * @since GemFire 7.0
 */
public interface InternalCache extends Cache, Extensible<Cache> {

  public DistributedMember getMyId();

  public Collection<DiskStoreImpl> listDiskStores();

  public Collection<DiskStoreImpl> listDiskStoresIncludingDefault();

  public Collection<DiskStoreImpl> listDiskStoresIncludingRegionOwned();

  public CqService getCqService();
  
  public <T extends CacheService> T getService(Class<T> clazz);
}
