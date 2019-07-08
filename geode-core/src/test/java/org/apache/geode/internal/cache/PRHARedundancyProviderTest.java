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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.InternalPRInfo;
import org.apache.geode.internal.cache.partitioned.LoadProbe;
import org.apache.geode.internal.cache.partitioned.PersistentBucketRecoverer;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;

public class PRHARedundancyProviderTest {

  private InternalCache cache;
  private PartitionedRegion partitionedRegion;
  private InternalResourceManager resourceManager;

  private PRHARedundancyProvider prHaRedundancyProvider;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    partitionedRegion = mock(PartitionedRegion.class);
    resourceManager = mock(InternalResourceManager.class);
  }

  @Test
  public void waitForPersistentBucketRecoveryProceedsWhenPersistentBucketRecovererLatchIsNotSet() {
    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class));

    prHaRedundancyProvider.waitForPersistentBucketRecovery();
  }

  @Test
  public void waitForPersistentBucketRecoveryProceedsAfterLatchCountDown() {
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true))
        .thenReturn(mock(DistributedRegion.class));
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(mock(PartitionAttributes.class));
    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> spy(new ThreadlessPersistentBucketRecoverer(a, b)));
    prHaRedundancyProvider.createPersistentBucketRecoverer(1);
    prHaRedundancyProvider.getPersistentBucketRecoverer().countDown();

    prHaRedundancyProvider.waitForPersistentBucketRecovery();

    verify(prHaRedundancyProvider.getPersistentBucketRecoverer()).await();
  }

  @Test
  public void buildPartitionedRegionInfo() {
    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class));
    when(partitionedRegion.getRedundancyTracker())
        .thenReturn(mock(PartitionedRegionRedundancyTracker.class));
    when(partitionedRegion.getRedundancyTracker().getActualRedundancy()).thenReturn(33);
    when(partitionedRegion.getRedundancyTracker().getLowRedundancyBuckets()).thenReturn(3);
    when(partitionedRegion.getRedundantCopies()).thenReturn(12);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    when(partitionedRegion.getRegionAdvisor().adviseDataStore()).thenReturn(new HashSet<>());
    when(partitionedRegion.getRegionAdvisor().getCreatedBucketsCount()).thenReturn(17);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(42);

    InternalPRInfo internalPRInfo =
        prHaRedundancyProvider.buildPartitionedRegionInfo(false, mock(LoadProbe.class));

    assertThat(internalPRInfo.getConfiguredBucketCount()).isEqualTo(42);
    assertThat(internalPRInfo.getCreatedBucketCount()).isEqualTo(17);
    assertThat(internalPRInfo.getLowRedundancyBucketCount()).isEqualTo(3);
    assertThat(internalPRInfo.getConfiguredRedundantCopies()).isEqualTo(12);
    assertThat(internalPRInfo.getActualRedundantCopies()).isEqualTo(33);
  }

  private static class ThreadlessPersistentBucketRecoverer extends PersistentBucketRecoverer {

    public ThreadlessPersistentBucketRecoverer(
        PRHARedundancyProvider prhaRedundancyProvider, int proxyBuckets) {
      super(prhaRedundancyProvider, proxyBuckets);
    }

    @Override
    public void startLoggingThread() {
      // do nothing
    }
  }
}
