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
package org.apache.geode.internal.cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisor;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;

/**
 * {@code RegionFactoryImpl} extends RegionFactory adding {@link RegionShortcut} support.
 * It also supports setting InternalRegionArguments.
 *
 * @since GemFire 6.5
 */
public class RegionFactoryImpl<K, V> extends RegionFactory<K, V> {
  private InternalRegionArguments internalRegionArguments;

  public RegionFactoryImpl(InternalCache cache) {
    super(cache);
  }

  public RegionFactoryImpl(InternalCache cache, RegionShortcut pra) {
    super(cache, pra);
  }

  public RegionFactoryImpl(InternalCache cache, RegionAttributes<K, V> ra) {
    super(cache, ra);
  }

  public RegionFactoryImpl(InternalCache cache, String regionAttributesId) {
    super(cache, regionAttributesId);
  }

  public RegionFactoryImpl(RegionFactory<K, V> regionFactory) {
    super(regionFactory);
  }

  private InternalRegionArguments makeInternal() {
    if (internalRegionArguments == null) {
      internalRegionArguments = new InternalRegionArguments();
    }
    return internalRegionArguments;
  }

  /**
   * Returns the region attributes that would currently be used to create the region.
   */
  public RegionAttributes<K, V> getCreateAttributes() {
    return getRegionAttributes();
  }

  @Override
  public Region<K, V> create(String name)
      throws CacheExistsException, RegionExistsException, CacheWriterException, TimeoutException {
    if (internalRegionArguments == null) {
      return super.create(name);
    }
    try {
      return getCache().createVMRegion(name, getRegionAttributes(), internalRegionArguments);
    } catch (IOException | ClassNotFoundException e) {
      throw new InternalGemFireError("unexpected exception", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Region<K, V> createSubregion(Region<?, ?> parent, String name)
      throws RegionExistsException {
    if (internalRegionArguments == null) {
      return super.createSubregion(parent, name);
    }
    try {
      return ((InternalRegion) parent).createSubregion(name, getRegionAttributes(),
          internalRegionArguments);
    } catch (IOException | ClassNotFoundException e) {
      throw new InternalGemFireError("unexpected exception", e);
    }
  }

  public RegionFactoryImpl<K, V> setIsUsedForPartitionedRegionAdmin(boolean adminFlag) {
    makeInternal().setIsUsedForPartitionedRegionAdmin(adminFlag);
    return this;
  }

  public RegionFactoryImpl<K, V> setPartitionedRegionBucketRedundancy(int redundancy) {
    makeInternal().setPartitionedRegionBucketRedundancy(redundancy);
    return this;
  }

  public RegionFactoryImpl<K, V> setPartitionedRegionAdvisor(RegionAdvisor advisor) {
    makeInternal().setPartitionedRegionAdvisor(advisor);
    return this;
  }

  public RegionFactoryImpl<K, V> setBucketAdvisor(BucketAdvisor advisor) {
    makeInternal().setBucketAdvisor(advisor);
    return this;
  }

  public RegionFactoryImpl<K, V> setPersistenceAdvisor(PersistenceAdvisor persistenceAdvisor) {
    makeInternal().setPersistenceAdvisor(persistenceAdvisor);
    return this;
  }

  public RegionFactoryImpl<K, V> setDiskRegion(DiskRegion diskRegion) {
    makeInternal().setDiskRegion(diskRegion);
    return this;
  }

  public RegionFactoryImpl<K, V> setIsUsedForMetaRegion(boolean isMetaRegion) {
    makeInternal().setIsUsedForMetaRegion(isMetaRegion);
    return this;
  }

  public RegionFactoryImpl<K, V> setMetaRegionWithTransactions(boolean metaRegionWithTransactions) {
    makeInternal().setMetaRegionWithTransactions(metaRegionWithTransactions);
    return this;
  }

  public RegionFactoryImpl<K, V> setLoaderHelperFactory(LoaderHelperFactory loaderHelperFactory) {
    makeInternal().setLoaderHelperFactory(loaderHelperFactory);
    return this;
  }

  public RegionFactoryImpl<K, V> setDestroyLockFlag(boolean getDestoryLock) {
    makeInternal().setDestroyLockFlag(getDestoryLock);
    return this;
  }

  public RegionFactoryImpl<K, V> setSnapshotInputStream(InputStream snapshotInputStream) {
    makeInternal().setSnapshotInputStream(snapshotInputStream);
    return this;
  }

  public RegionFactoryImpl<K, V> setImageTarget(InternalDistributedMember imageTarget) {
    makeInternal().setImageTarget(imageTarget);
    return this;
  }

  public RegionFactoryImpl<K, V> setRecreateFlag(boolean recreate) {
    makeInternal().setRecreateFlag(recreate);
    return this;
  }

  public RegionFactoryImpl<K, V> setInternalMetaRegion(LocalRegion r) {
    makeInternal().setInternalMetaRegion(r);
    return this;
  }

  public RegionFactoryImpl<K, V> setCachePerfStatsHolder(HasCachePerfStats cachePerfStatsHolder) {
    makeInternal().setCachePerfStatsHolder(cachePerfStatsHolder);
    return this;
  }

  public RegionFactoryImpl<K, V> setPartitionedRegion(PartitionedRegion partitionedRegion) {
    makeInternal().setPartitionedRegion(partitionedRegion);
    return this;
  }

  public RegionFactoryImpl<K, V> setTestCallable(LocalRegion.TestCallable c) {
    makeInternal().setTestCallable(c);
    return this;
  }

  public RegionFactoryImpl<K, V> setUserAttribute(Object userAttr) {
    makeInternal().setUserAttribute(userAttr);
    return this;
  }

  public RegionFactoryImpl<K, V> setIsUsedForSerialGatewaySenderQueue(boolean queueFlag) {
    makeInternal().setIsUsedForSerialGatewaySenderQueue(queueFlag);
    return this;
  }

  public RegionFactoryImpl<K, V> setIsUsedForParallelGatewaySenderQueue(boolean queueFlag) {
    makeInternal().setIsUsedForParallelGatewaySenderQueue(queueFlag);
    return this;
  }

  public RegionFactoryImpl<K, V> setParallelGatewaySender(AbstractGatewaySender pgSender) {
    makeInternal().setParallelGatewaySender(pgSender);
    return this;
  }

  public RegionFactoryImpl<K, V> setSerialGatewaySender(AbstractGatewaySender serialSender) {
    makeInternal().setSerialGatewaySender(serialSender);
    return this;
  }

  public RegionFactoryImpl<K, V> setIndexes(List indexes) {
    makeInternal().setIndexes(indexes);
    return this;
  }

  public RegionFactoryImpl<K, V> addCacheServiceProfile(CacheServiceProfile profile) {
    makeInternal().addCacheServiceProfile(profile);
    return this;
  }

  public RegionFactoryImpl<K, V> addInternalAsyncEventQueueId(String aeqId) {
    makeInternal().addInternalAsyncEventQueueId(aeqId);
    return this;
  }

  public RegionFactoryImpl<K, V> setInternalRegion(final boolean internalRegion) {
    makeInternal().setInternalRegion(internalRegion);
    return this;
  }

  public InternalRegionArguments getInternalRegionArguments() {
    return internalRegionArguments;
  }
}
