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

package com.gemstone.gemfire.internal.cache;

import java.util.Collection;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSStoreDirector;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.extension.Extensible;

/**
 * The InternalCache interface is contract for implementing classes for defining internal cache operations that should
 * not be part of the "public" API of the implementing class.
 * </p>
 * @author jblum
 * @see com.gemstone.gemfire.cache.Cache
 * @since 7.0
 */
public interface InternalCache extends Cache, Extensible<Cache> {

  public DistributedMember getMyId();

  public Collection<DiskStoreImpl> listDiskStores();

  public Collection<DiskStoreImpl> listDiskStoresIncludingDefault();

  public Collection<DiskStoreImpl> listDiskStoresIncludingRegionOwned();

  public CqService getCqService();
  
  public Collection<HDFSStoreImpl> getHDFSStores() ;
  
  public <T extends CacheService> T getService(Class<T> clazz);
}
