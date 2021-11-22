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
package org.apache.geode.internal.cache.partitioned;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.BucketPersistenceAdvisor;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.ProxyBucketRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager;

public class PersistentBucketRecovererTest {

  private PartitionedRegion partitionedRegion;
  private InternalCache cache;
  private DistributedRegion root;
  private PRHARedundancyProvider provider;
  private InternalResourceManager resourceManager;

  @Before
  public void setUp() {
    partitionedRegion = mock(PartitionedRegion.class, RETURNS_DEEP_STUBS);
    resourceManager = mock(InternalResourceManager.class);
    cache = mock(InternalCache.class);
    root = mock(DistributedRegion.class);

    when(partitionedRegion.getCache()).thenReturn(cache);
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true)).thenReturn(root);

    provider = new PRHARedundancyProvider(partitionedRegion, resourceManager);
  }

  @Test
  public void allBucketsRecoveredFromDiskCountDownLatchIsSet() {
    int numberOfProxyBuckets = 5;

    PersistentBucketRecoverer recoverer =
        new PersistentBucketRecoverer(provider, numberOfProxyBuckets);

    assertThat(recoverer.getAllBucketsRecoveredFromDiskLatch()).isNotNull();
    assertThat(recoverer.getLatchCount()).isEqualTo(numberOfProxyBuckets);
  }

  @Test
  public void latchCanBeCountedDown() {
    int numberOfProxyBuckets = 5;
    PersistentBucketRecoverer recoverer =
        new PersistentBucketRecoverer(provider, numberOfProxyBuckets);

    assertThat(recoverer.getLatchCount()).isEqualTo(numberOfProxyBuckets);
    recoverer.countDown(numberOfProxyBuckets);

    recoverer.await();
  }


  @Test
  public void runLoggingThread() {
    PartitionedRegion baseRegion = mock(PartitionedRegion.class);
    InternalResourceManager internalResourceManager = mock(InternalResourceManager.class);
    InternalCache internalCache = mock(InternalCache.class);
    DistributedRegion prRoot = mock(DistributedRegion.class);

    when(baseRegion.getCache()).thenReturn(internalCache);
    when(baseRegion.getGemFireCache()).thenReturn(internalCache);

    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(internalCache.getDistributedSystem()).thenReturn(distributedSystem);
    when(distributedSystem.getCancelCriterion()).thenReturn(cancelCriterion);
    when(cancelCriterion.isCancelInProgress()).thenReturn(false);

    when(internalCache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true))
        .thenReturn(prRoot);

    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(baseRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getColocatedWith()).thenReturn(null);

    DataPolicy dataPolicy = mock(DataPolicy.class);
    when(baseRegion.getDataPolicy()).thenReturn(dataPolicy);
    when(dataPolicy.withPersistence()).thenReturn(true);

    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    when(regionAttributes.getDataPolicy()).thenReturn(dataPolicy);
    when(baseRegion.getAttributes()).thenReturn(regionAttributes);

    DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
    when(baseRegion.getDiskStore()).thenReturn(diskStore);

    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    when(baseRegion.getRegionAdvisor()).thenReturn(regionAdvisor);

    int numberOfProxyBuckets = 5;

    ProxyBucketRegion[] bucs = new ProxyBucketRegion[numberOfProxyBuckets];

    ProxyBucketRegion bucketRegion1 = mock(ProxyBucketRegion.class);
    ProxyBucketRegion bucketRegion2 = mock(ProxyBucketRegion.class);
    ProxyBucketRegion bucketRegion3 = mock(ProxyBucketRegion.class);
    ProxyBucketRegion bucketRegion4 = mock(ProxyBucketRegion.class);
    ProxyBucketRegion bucketRegion5 = mock(ProxyBucketRegion.class);

    bucs[0] = bucketRegion1;
    bucs[1] = bucketRegion2;
    bucs[2] = bucketRegion3;
    bucs[3] = bucketRegion4;
    bucs[4] = bucketRegion5;
    when(regionAdvisor.getProxyBucketArray()).thenReturn(bucs);

    BucketPersistenceAdvisor persistenceAdvisor = mock(BucketPersistenceAdvisor.class);
    when(bucketRegion1.getPersistenceAdvisor()).thenReturn(persistenceAdvisor);
    when(bucketRegion2.getPersistenceAdvisor()).thenReturn(persistenceAdvisor);
    when(bucketRegion3.getPersistenceAdvisor()).thenReturn(persistenceAdvisor);
    when(bucketRegion4.getPersistenceAdvisor()).thenReturn(persistenceAdvisor);
    when(bucketRegion5.getPersistenceAdvisor()).thenReturn(persistenceAdvisor);
    when(persistenceAdvisor.isClosed()).thenReturn(true);

    PRHARedundancyProvider redundancyProvider =
        new PRHARedundancyProvider(baseRegion, internalResourceManager);

    PersistentBucketRecoverer recoverer =
        new PersistentBucketRecoverer(redundancyProvider, numberOfProxyBuckets);

    assertThat(recoverer.getLatchCount()).isEqualTo(numberOfProxyBuckets);

    recoverer.run();
    assertThat(recoverer.getRegions().isEmpty()).isTrue();
  }
}
