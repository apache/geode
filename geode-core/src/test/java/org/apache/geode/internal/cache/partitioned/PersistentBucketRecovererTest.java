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

import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
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
}
