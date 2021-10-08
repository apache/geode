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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionRegionConfig;
import org.apache.geode.internal.cache.PartitionedRegion;

public class CreateMissingBucketsTaskTest {

  private CreateMissingBucketsTask task;
  private PRHARedundancyProvider prhaRedundancyProvider;

  private PartitionedRegion partitionedRegion;
  private PartitionRegionConfig partitionRegionConfig;
  private DistributedRegion prRoot;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);


  @Before
  public void setup() {

    prhaRedundancyProvider = mock(PRHARedundancyProvider.class);

    task = new CreateMissingBucketsTask(prhaRedundancyProvider);

    InternalCache cache = mock(InternalCache.class);

    partitionRegionConfig = mock(PartitionRegionConfig.class);
    partitionedRegion = mock(PartitionedRegion.class);
    prRoot = mock(DistributedRegion.class);

    when(cache.getRegion(anyString(), anyBoolean())).thenReturn(prRoot);
    when(partitionedRegion.getCache()).thenReturn(cache);

    when(prRoot.get(any())).thenReturn(partitionRegionConfig);

  }

  @Test
  public void testWaitForColocationIsCompleted() {
    when(partitionRegionConfig.isColocationComplete()).thenReturn(true);
    assertThat(task.waitForColocationCompleted(partitionedRegion)).isTrue();
  }

  @Test
  public void testWaitForColocationNotCompleted() {
    when(partitionRegionConfig.isColocationComplete()).thenReturn(false);
    assertThat(task.waitForColocationCompleted(partitionedRegion)).isFalse();
  }

  @Test
  public void testCreateMissingBuckets() {
    PartitionedRegion leaderRegion = mock(PartitionedRegion.class);
    PartitionedRegion.getPrIdToPR().put(1, leaderRegion);

    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getColocatedWith()).thenReturn("region2");
    when(partitionRegionConfig.getPRId()).thenReturn(1);

    PartitionAttributes leaderPartitionAttributes = mock(PartitionAttributes.class);
    when(leaderRegion.getPartitionAttributes()).thenReturn(leaderPartitionAttributes);
    when(leaderPartitionAttributes.getColocatedWith()).thenReturn(null);

    task.createMissingBuckets(partitionedRegion);

    verify(partitionedRegion).getTotalNumberOfBuckets();
  }

  @Test
  public void testTaskRun() {

    when(prhaRedundancyProvider.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(partitionRegionConfig.isColocationComplete()).thenReturn(true);

    PartitionedRegion leaderRegion = mock(PartitionedRegion.class);
    PartitionedRegion.getPrIdToPR().put(1, leaderRegion);

    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getColocatedWith()).thenReturn("region2");
    when(partitionRegionConfig.getPRId()).thenReturn(1);

    PartitionAttributes leaderPartitionAttributes = mock(PartitionAttributes.class);
    when(leaderRegion.getPartitionAttributes()).thenReturn(leaderPartitionAttributes);
    when(leaderPartitionAttributes.getColocatedWith()).thenReturn(null);

    PartitionedRegion.RecoveryLock lock = mock(PartitionedRegion.RecoveryLock.class);

    when(leaderRegion.getRecoveryLock()).thenReturn(lock);

    task.run2();

    verify(partitionedRegion).getTotalNumberOfBuckets();
  }
}
