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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;

public class ProxyBucketRegionTest {

  @Test
  public void shouldBeMockable() throws Exception {
    ProxyBucketRegion mockProxyBucketRegion = mock(ProxyBucketRegion.class);
    BucketAdvisor mockBucketAdvisor = mock(BucketAdvisor.class);

    when(mockProxyBucketRegion.getBucketAdvisor()).thenReturn(mockBucketAdvisor);

    assertThat(mockProxyBucketRegion.getBucketAdvisor()).isSameAs(mockBucketAdvisor);
  }

  @Test
  public void testRecoverFromDisk() throws Exception {
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    InternalRegionArguments internalRegionArguments = mock(InternalRegionArguments.class);
    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    DataPolicy dp = mock(DataPolicy.class);
    RegionAttributes ra = mock(RegionAttributes.class);
    DiskStoreImpl ds = mock(DiskStoreImpl.class);
    DiskInitFile dif = mock(DiskInitFile.class);
    DiskRegion dr = mock(DiskRegion.class);
    DistributionManager dm = mock(DistributionManager.class);
    DistributionConfig config = mock(DistributionConfig.class);
    CancelCriterion cancel = mock(CancelCriterion.class);

    when(internalRegionArguments.getPartitionedRegionAdvisor()).thenReturn(regionAdvisor);
    when(regionAdvisor.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getColocatedWith()).thenReturn(null);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getGemFireCache()).thenReturn(cache);
    when(cache.getInternalDistributedSystem()).thenReturn(ids);
    when(ids.getDistributionManager()).thenReturn(dm);
    when(partitionedRegion.getDataPolicy()).thenReturn(dp);
    when(dp.withPersistence()).thenReturn(true);
    when(cache.getInternalDistributedSystem()).thenReturn(ids);
    when(partitionedRegion.getAttributes()).thenReturn(ra);
    when(ra.getEvictionAttributes()).thenReturn(null);
    when(partitionedRegion.getDiskStore()).thenReturn(ds);
    when(ds.getDiskInitFile()).thenReturn(dif);
    when(dif.createDiskRegion(any(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(),
        anyBoolean(), any(), any(), any(), any(), any(), anyString(), anyInt(), any(),
        anyBoolean())).thenReturn(dr);
    when(dm.getConfig()).thenReturn(config);
    when(config.getAckWaitThreshold()).thenReturn(10);
    when(cache.getCancelCriterion()).thenReturn(cancel);
    when(regionAdvisor.isInitialized()).thenReturn(true);

    ProxyBucketRegion proxyBucketRegion =
        new ProxyBucketRegion(0, partitionedRegion, internalRegionArguments);

    proxyBucketRegion.recoverFromDisk();
    proxyBucketRegion.recoverFromDisk();
    verify(regionAdvisor, times(1))
        .isInitialized();
  }
}
