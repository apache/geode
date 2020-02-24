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

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.LocalRegion.TestCallable;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisor;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;

/**
 * An internal version of Region Attributes that allows for additional information to be passed to
 * the Region constructors, typically for internal purposes, for example internally GemFire may need
 * use a Region and flag it for internal use only.
 *
 * @since GemFire 4.2.3
 */
public class InternalRegionArguments {
  private boolean isUsedForPartitionedRegionAdmin;
  private boolean isUsedForSerialGatewaySenderQueue;
  private boolean isUsedForParallelGatewaySenderQueue;
  private boolean isInternalRegion;
  private int bucketRedundancy;
  private boolean isUsedForPartitionedRegionBucket;
  private RegionAdvisor partitionedRegionAdvisor;
  private boolean isUsedForMetaRegion = false;
  private boolean metaRegionWithTransactions = false;
  private LoaderHelperFactory loaderHelperFactory;
  private HasCachePerfStats cachePerfStatsHolder;

  private boolean getDestroyLock = true;
  private InputStream snapshotInputStream;
  private InternalDistributedMember imageTarget;
  private boolean recreate;
  private LocalRegion internalMetaRegion;
  private BucketAdvisor bucketAdvisor;
  private PersistenceAdvisor persistenceAdvisor;
  private DiskRegion diskRegion;
  private PartitionedRegion partitionedRegion;
  private TestCallable testCallable;

  private AbstractGatewaySender parallelGatewaySender;
  private AbstractGatewaySender serialGatewaySender;


  private Object userAttribute = null;
  private List indexes;

  private Map<String, CacheServiceProfile> cacheServiceProfiles;

  private Set<String> internalAsyncEventQueueIds;


  public InternalRegionArguments() {}

  /* methods that set and retrieve internal state used to configure a Region */

  public InternalRegionArguments setIsUsedForPartitionedRegionAdmin(boolean adminFlag) {
    this.isUsedForPartitionedRegionAdmin = adminFlag;
    return this;
  }

  public boolean isUsedForPartitionedRegionAdmin() {
    return this.isUsedForPartitionedRegionAdmin;
  }

  public InternalRegionArguments setPartitionedRegionBucketRedundancy(int redundancy) {
    this.isUsedForPartitionedRegionBucket = true;
    this.bucketRedundancy = redundancy;
    return this;
  }

  public InternalRegionArguments setPartitionedRegionAdvisor(RegionAdvisor advisor) {
    this.partitionedRegionAdvisor = advisor;
    return this;
  }

  public RegionAdvisor getPartitionedRegionAdvisor() {
    return this.partitionedRegionAdvisor;
  }

  public InternalRegionArguments setBucketAdvisor(BucketAdvisor advisor) {
    this.bucketAdvisor = advisor;
    return this;
  }

  public BucketAdvisor getBucketAdvisor() {
    return this.bucketAdvisor;
  }

  public InternalRegionArguments setPersistenceAdvisor(PersistenceAdvisor persistenceAdvisor) {
    this.persistenceAdvisor = persistenceAdvisor;
    return this;
  }

  public PersistenceAdvisor getPersistenceAdvisor() {
    return persistenceAdvisor;
  }

  public InternalRegionArguments setDiskRegion(DiskRegion diskRegion) {
    this.diskRegion = diskRegion;
    return this;
  }

  public DiskRegion getDiskRegion() {
    return diskRegion;
  }

  public boolean isUsedForPartitionedRegionBucket() {
    return this.isUsedForPartitionedRegionBucket;
  }

  public InternalRegionArguments setIsUsedForMetaRegion(boolean isMetaRegion) {
    this.isUsedForMetaRegion = isMetaRegion;
    return this;
  }

  public boolean isUsedForMetaRegion() {
    return this.isUsedForMetaRegion;
  }

  public InternalRegionArguments setMetaRegionWithTransactions(boolean metaRegionWithTransactions) {
    this.metaRegionWithTransactions = metaRegionWithTransactions;
    return this;
  }

  public boolean isMetaRegionWithTransactions() {
    return this.metaRegionWithTransactions;
  }

  public int getPartitionedRegionBucketRedundancy() {
    return this.bucketRedundancy;
  }

  public InternalRegionArguments setLoaderHelperFactory(LoaderHelperFactory loaderHelperFactory) {
    this.loaderHelperFactory = loaderHelperFactory;
    return this;
  }

  public LoaderHelperFactory getLoaderHelperFactory() {
    return this.loaderHelperFactory;
  }

  public InternalRegionArguments setDestroyLockFlag(boolean getDestoryLock) {
    this.getDestroyLock = getDestoryLock;
    return this;
  }

  public boolean getDestroyLockFlag() {
    return this.getDestroyLock;
  }

  public InternalRegionArguments setSnapshotInputStream(InputStream snapshotInputStream) {
    this.snapshotInputStream = snapshotInputStream;
    return this;
  }

  public InputStream getSnapshotInputStream() {
    return this.snapshotInputStream;
  }

  public InternalRegionArguments setImageTarget(InternalDistributedMember imageTarget) {
    this.imageTarget = imageTarget;
    return this;
  }

  public InternalDistributedMember getImageTarget() {
    return this.imageTarget;
  }

  public InternalRegionArguments setRecreateFlag(boolean recreate) {
    this.recreate = recreate;
    return this;
  }

  public boolean getRecreateFlag() {
    return this.recreate;
  }

  public InternalRegionArguments setInternalMetaRegion(LocalRegion r) {
    this.internalMetaRegion = r;
    return this;
  }

  public LocalRegion getInternalMetaRegion() {
    return this.internalMetaRegion;
  }

  public HasCachePerfStats getCachePerfStatsHolder() {
    return cachePerfStatsHolder;
  }

  public InternalRegionArguments setCachePerfStatsHolder(HasCachePerfStats cachePerfStatsHolder) {
    this.cachePerfStatsHolder = cachePerfStatsHolder;
    return this;
  }

  public InternalRegionArguments setPartitionedRegion(PartitionedRegion partitionedRegion) {
    this.partitionedRegion = partitionedRegion;
    return this;
  }

  public PartitionedRegion getPartitionedRegion() {
    return this.partitionedRegion;
  }

  public InternalRegionArguments setTestCallable(TestCallable c) {
    this.testCallable = c;
    return this;
  }

  public TestCallable getTestCallable() {
    return this.testCallable;
  }

  public InternalRegionArguments setUserAttribute(Object userAttr) {
    this.userAttribute = userAttr;
    return this;
  }

  public Object getUserAttribute() {
    return this.userAttribute;
  }

  public InternalRegionArguments setIsUsedForSerialGatewaySenderQueue(boolean queueFlag) {
    this.isUsedForSerialGatewaySenderQueue = queueFlag;
    return this;
  }

  public boolean isUsedForSerialGatewaySenderQueue() {
    return this.isUsedForSerialGatewaySenderQueue;
  }

  public InternalRegionArguments setIsUsedForParallelGatewaySenderQueue(boolean queueFlag) {
    this.isUsedForParallelGatewaySenderQueue = queueFlag;
    return this;
  }

  public boolean isUsedForParallelGatewaySenderQueue() {
    return this.isUsedForParallelGatewaySenderQueue;
  }

  public InternalRegionArguments setParallelGatewaySender(AbstractGatewaySender pgSender) {
    this.parallelGatewaySender = pgSender;
    return this;
  }

  public InternalRegionArguments setSerialGatewaySender(AbstractGatewaySender serialSender) {
    this.serialGatewaySender = serialSender;
    return this;
  }

  public AbstractGatewaySender getSerialGatewaySender() {
    return this.serialGatewaySender;
  }

  public AbstractGatewaySender getParallelGatewaySender() {
    return this.parallelGatewaySender;
  }


  public InternalRegionArguments setIndexes(List indexes) {
    if (this.indexes == null && indexes != null) {
      this.indexes = indexes;
    }
    return this;
  }

  public List getIndexes() {
    return this.indexes;
  }

  public InternalRegionArguments addCacheServiceProfile(CacheServiceProfile profile) {
    if (this.cacheServiceProfiles == null) {
      this.cacheServiceProfiles = new HashMap<>();
    }
    this.cacheServiceProfiles.put(profile.getId(), profile);
    return this;
  }

  public Map<String, CacheServiceProfile> getCacheServiceProfiles() {
    return this.cacheServiceProfiles;
  }

  public InternalRegionArguments addInternalAsyncEventQueueId(String aeqId) {
    if (this.internalAsyncEventQueueIds == null) {
      this.internalAsyncEventQueueIds = new HashSet<>();
    }
    this.internalAsyncEventQueueIds.add(aeqId);
    return this;
  }

  public Set<String> getInternalAsyncEventQueueIds() {
    return this.internalAsyncEventQueueIds;
  }

  public boolean isInternalRegion() {
    return isInternalRegion;
  }

  public InternalRegionArguments setInternalRegion(final boolean internalRegion) {
    isInternalRegion = internalRegion;
    return this;
  }
}
