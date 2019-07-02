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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.partitioned.InternalPRInfo;
import org.apache.geode.internal.cache.partitioned.LoadProbe;
import org.apache.geode.internal.cache.partitioned.PersistentBucketRecoverer;

public class PRHARedundancyProviderTest {

  private PartitionedRegion partitionedRegion;
  private PRHARedundancyProvider prHaRedundancyProvider;

  @Before
  public void setUp() {
    partitionedRegion = mock(PartitionedRegion.class, RETURNS_DEEP_STUBS);
    InternalCache cache = mock(InternalCache.class);
    DistributedRegion rootRegion = mock(DistributedRegion.class);

    when(partitionedRegion.getCache()).thenReturn(cache);
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true)).thenReturn(rootRegion);

    prHaRedundancyProvider = spy(new PRHARedundancyProvider(partitionedRegion));
  }

  @Test
  public void waitForPersistentBucketRecoveryProceedsWhenPersistentBucketRecovererLatchIsNotSet() {
    PersistentBucketRecoverer recoverer = mock(PersistentBucketRecoverer.class);
    when(prHaRedundancyProvider.getPersistentBucketRecoverer()).thenReturn(recoverer);

    prHaRedundancyProvider.waitForPersistentBucketRecovery();
  }

  @Test
  public void waitForPersistentBucketRecoveryProceedsAfterLatchCountDown() {
    PersistentBucketRecoverer recoverer =
        spy(new PersistentBucketRecoverer(prHaRedundancyProvider, 1));
    when(prHaRedundancyProvider.getPersistentBucketRecoverer()).thenReturn(recoverer);
    prHaRedundancyProvider.getPersistentBucketRecoverer().countDown();

    prHaRedundancyProvider.waitForPersistentBucketRecovery();

    verify(recoverer).await();
  }

  @Test
  public void buildPartitionedRegionInfo() {
    when(partitionedRegion.getRegionAdvisor().adviseDataStore()).thenReturn(new HashSet<>());
    when(partitionedRegion.getRegionAdvisor().getProxyBucketArray())
        .thenReturn(new ProxyBucketRegion[] {});

    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(42);
    when(partitionedRegion.getRegionAdvisor().getCreatedBucketsCount()).thenReturn(17);
    when(partitionedRegion.getRedundancyTracker().getLowRedundancyBuckets()).thenReturn(3);
    when(partitionedRegion.getRedundantCopies()).thenReturn(12);
    when(partitionedRegion.getRedundancyTracker().getActualRedundancy()).thenReturn(33);

    InternalPRInfo internalPRInfo =
        prHaRedundancyProvider.buildPartitionedRegionInfo(false, mock(LoadProbe.class));

    assertThat(internalPRInfo.getConfiguredBucketCount()).isEqualTo(42);
    assertThat(internalPRInfo.getCreatedBucketCount()).isEqualTo(17);
    assertThat(internalPRInfo.getLowRedundancyBucketCount()).isEqualTo(3);
    assertThat(internalPRInfo.getConfiguredRedundantCopies()).isEqualTo(12);
    assertThat(internalPRInfo.getActualRedundantCopies()).isEqualTo(33);
  }
}
