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

import static org.apache.geode.internal.cache.partitioned.CreateMissingBucketsTask.MAX_NUMBER_INTERVALS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.internal.cache.BucketAdvisor;
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
  public void waitForColocationCompletedReturnsTrueWhenColocationIsComplete() {
    when(partitionRegionConfig.isColocationComplete()).thenReturn(true);
    assertThat(task.waitForColocationCompleted(partitionedRegion)).isTrue();
    assertThat(task.getRetryCount()).isEqualTo(0);

  }

  @Test
  public void waitForColocationCompletedReturnsTrueWhenColocationIsCompleteAfter3rdRetry() {
    when(partitionRegionConfig.isColocationComplete()).thenReturn(false, false, true);
    assertThat(task.waitForColocationCompleted(partitionedRegion)).isTrue();
    assertThat(task.getRetryCount()).isEqualTo(2);
  }

  @Test
  public void waitForColocationCompletedReturnsFalseWhenColocationIsNotCompleteAfterTimeout() {
    when(partitionRegionConfig.isColocationComplete()).thenReturn(false);
    assertThat(task.waitForColocationCompleted(partitionedRegion)).isFalse();
    assertThat(task.getRetryCount()).isEqualTo(MAX_NUMBER_INTERVALS);
  }

  @Test
  public void createMissingBucketsDoesNotCreateBucketsWhenLeaderAndChildRegionHaveSameRedundancy() {
    PartitionedRegion leaderRegion = mock(PartitionedRegion.class);
    PartitionedRegion.getPrIdToPR().put(1, leaderRegion);

    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getColocatedWith()).thenReturn("region2");
    when(partitionRegionConfig.getPRId()).thenReturn(1);

    PartitionAttributes leaderPartitionAttributes = mock(PartitionAttributes.class);
    when(leaderRegion.getPartitionAttributes()).thenReturn(leaderPartitionAttributes);
    when(leaderPartitionAttributes.getColocatedWith()).thenReturn(null);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(2);

    RegionAdvisor regionAdvisor1 = mock(RegionAdvisor.class);
    RegionAdvisor regionAdvisor2 = mock(RegionAdvisor.class);

    when(leaderRegion.getRegionAdvisor()).thenReturn(regionAdvisor1);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(regionAdvisor2);

    BucketAdvisor bucketAdvisor1 = mock(BucketAdvisor.class);
    BucketAdvisor bucketAdvisor2 = mock(BucketAdvisor.class);

    when(regionAdvisor1.getBucketAdvisor(eq(0))).thenReturn(bucketAdvisor1);
    when(regionAdvisor1.getBucketAdvisor(eq(1))).thenReturn(bucketAdvisor2);

    when(regionAdvisor2.getBucketAdvisor(eq(0))).thenReturn(bucketAdvisor1);
    when(regionAdvisor2.getBucketAdvisor(eq(1))).thenReturn(bucketAdvisor2);

    when(bucketAdvisor1.getBucketRedundancy()).thenReturn(2);
    when(bucketAdvisor2.getBucketRedundancy()).thenReturn(2);

    task.createMissingBuckets(partitionedRegion);

    verify(partitionedRegion, never()).getRedundancyProvider();
  }

  @Test
  public void createMissingBucketsCreatesBucketsWhenLeaderAndChildRegionHaveDifferentRedundancy() {
    PartitionedRegion leaderRegion = mock(PartitionedRegion.class);
    PartitionedRegion.getPrIdToPR().put(1, leaderRegion);

    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getColocatedWith()).thenReturn("region2");
    when(partitionRegionConfig.getPRId()).thenReturn(1);

    PartitionAttributes leaderPartitionAttributes = mock(PartitionAttributes.class);
    when(leaderRegion.getPartitionAttributes()).thenReturn(leaderPartitionAttributes);
    when(leaderPartitionAttributes.getColocatedWith()).thenReturn(null);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(2);

    RegionAdvisor regionAdvisor1 = mock(RegionAdvisor.class);
    RegionAdvisor regionAdvisor2 = mock(RegionAdvisor.class);

    when(leaderRegion.getRegionAdvisor()).thenReturn(regionAdvisor1);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(regionAdvisor2);

    BucketAdvisor bucketAdvisor1 = mock(BucketAdvisor.class);
    BucketAdvisor bucketAdvisor2 = mock(BucketAdvisor.class);
    BucketAdvisor bucketAdvisor3 = mock(BucketAdvisor.class);
    BucketAdvisor bucketAdvisor4 = mock(BucketAdvisor.class);

    when(regionAdvisor1.getBucketAdvisor(eq(0))).thenReturn(bucketAdvisor1);
    when(regionAdvisor1.getBucketAdvisor(eq(1))).thenReturn(bucketAdvisor2);

    when(regionAdvisor2.getBucketAdvisor(eq(0))).thenReturn(bucketAdvisor3);
    when(regionAdvisor2.getBucketAdvisor(eq(1))).thenReturn(bucketAdvisor4);

    when(bucketAdvisor1.getBucketRedundancy()).thenReturn(2);
    when(bucketAdvisor2.getBucketRedundancy()).thenReturn(2);
    when(bucketAdvisor3.getBucketRedundancy()).thenReturn(1);
    when(bucketAdvisor4.getBucketRedundancy()).thenReturn(2);

    when(partitionedRegion.getRedundancyProvider()).thenReturn(prhaRedundancyProvider);

    task.createMissingBuckets(partitionedRegion);

    verify(prhaRedundancyProvider).createBucketAtomically(eq(0), eq(0), eq(true), any());
  }

  @Test
  public void testTaskRunColocationCompletedCreatesBucketsWhenLeaderAndChildRegionHaveDifferentRedundancy() {

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

    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(2);

    RegionAdvisor regionAdvisor1 = mock(RegionAdvisor.class);
    RegionAdvisor regionAdvisor2 = mock(RegionAdvisor.class);

    when(leaderRegion.getRegionAdvisor()).thenReturn(regionAdvisor1);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(regionAdvisor2);

    BucketAdvisor bucketAdvisor1 = mock(BucketAdvisor.class);
    BucketAdvisor bucketAdvisor2 = mock(BucketAdvisor.class);
    BucketAdvisor bucketAdvisor3 = mock(BucketAdvisor.class);
    BucketAdvisor bucketAdvisor4 = mock(BucketAdvisor.class);

    when(regionAdvisor1.getBucketAdvisor(eq(0))).thenReturn(bucketAdvisor1);
    when(regionAdvisor1.getBucketAdvisor(eq(1))).thenReturn(bucketAdvisor2);

    when(regionAdvisor2.getBucketAdvisor(eq(0))).thenReturn(bucketAdvisor3);
    when(regionAdvisor2.getBucketAdvisor(eq(1))).thenReturn(bucketAdvisor4);

    when(bucketAdvisor1.getBucketRedundancy()).thenReturn(2);
    when(bucketAdvisor2.getBucketRedundancy()).thenReturn(2);
    when(bucketAdvisor3.getBucketRedundancy()).thenReturn(1);
    when(bucketAdvisor4.getBucketRedundancy()).thenReturn(2);

    when(partitionedRegion.getRedundancyProvider()).thenReturn(prhaRedundancyProvider);

    task.run2();

    verify(prhaRedundancyProvider).createBucketAtomically(eq(0), eq(0), eq(true), any());
    verify(lock).unlock();
  }

  @Test
  public void testTaskRunDoesNotCreateBucketsWhenColocationNotCompleted() {

    when(prhaRedundancyProvider.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(partitionRegionConfig.isColocationComplete()).thenReturn(false);

    PartitionedRegion leaderRegion = mock(PartitionedRegion.class);
    PartitionedRegion.getPrIdToPR().put(1, leaderRegion);

    task.run2();

    verify(leaderRegion, never()).getRecoveryLock();
  }

  @Test
  public void testTaskRunCheckThatLockIsUnlockedWhenThrownException() {

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
    when(partitionedRegion.getTotalNumberOfBuckets()).thenThrow(new RuntimeException("Fail"));

    assertThatThrownBy(() -> task.run2()).isInstanceOf(RuntimeException.class);

    verify(lock).unlock();

  }
}
