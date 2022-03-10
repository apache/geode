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

import static org.apache.geode.distributed.internal.DistributionAdvisor.ILLEGAL_SERIAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.ProxyBucketRegion;

public class RegionAdvisorJUnitTest {

  private PartitionedRegion partitionedRegion;
  private RegionAdvisor regionAdvisor;
  private final int[] serials = new int[] {ILLEGAL_SERIAL, ILLEGAL_SERIAL, ILLEGAL_SERIAL};

  @Before
  public void setUp() throws Exception {
    partitionedRegion = mock(PartitionedRegion.class);
    regionAdvisor = new RegionAdvisor(partitionedRegion);
  }

  @Test
  public void getBucketSerials_shouldReturnAnArrayOfIllegalSerials_whenBucketsAreNull() {
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getTotalNumBuckets()).thenReturn(serials.length);

    assertThat(regionAdvisor.getBucketSerials()).containsExactly(serials);
  }

  @Test
  public void processProfilesQueuedDuringInitialization_shouldNotThrowIndexOutOfBoundsException() {
    RegionAdvisor.QueuedBucketProfile queuedBucketProfile =
        new RegionAdvisor.QueuedBucketProfile(mock(InternalDistributedMember.class), serials, true);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(regionAdvisor.getDistributionManager()).thenReturn(distributionManager);
    when(distributionManager.isCurrentMember(any())).thenReturn(true);
    regionAdvisor.preInitQueue.add(queuedBucketProfile);

    ProxyBucketRegion proxyBucketRegion = mock(ProxyBucketRegion.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(proxyBucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
    regionAdvisor.buckets =
        new ProxyBucketRegion[] {proxyBucketRegion, proxyBucketRegion, proxyBucketRegion};

    regionAdvisor.processProfilesQueuedDuringInitialization();

    verify(bucketAdvisor, times(0)).removeIdWithSerial(any(InternalDistributedMember.class),
        anyInt(), anyBoolean());
  }

  @Test
  public void testRemoveIdAndBucketWithNonNullPreInitQueue() {
    int bucketId = 0;
    int serial = 1234;
    boolean regionDestroyed = true;
    InternalDistributedMember memberId = mock(InternalDistributedMember.class);
    Bucket bucket = mock(Bucket.class);
    when(bucket.isHosting()).thenReturn(true);
    when(bucket.getId()).thenReturn(bucketId);
    int version = 0;
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getTotalNumBuckets()).thenReturn(1);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getColocatedWith()).thenReturn(null);
    InternalCache internalCache = mock(InternalCache.class);
    when(partitionedRegion.getCache()).thenReturn(internalCache);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(internalCache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(regionAdvisor.getDistributionManager()).thenReturn(distributionManager);
    when(distributionManager.isCurrentMember(any())).thenReturn(true);
    DataPolicy dataPolicy = mock(DataPolicy.class);
    when(partitionedRegion.getDataPolicy()).thenReturn(dataPolicy);
    when(dataPolicy.withPersistence()).thenReturn(false);
    regionAdvisor.initializeRegionAdvisor();
    BucketAdvisor.BucketProfile bucketProfile = new BucketAdvisor.BucketProfile(memberId, version, bucket);
//    regionAdvisor.putBucketProfile(bucketId, bucketProfile);
    assertThat(regionAdvisor.preInitQueue).isNotNull();
//    assertThat(regionAdvisor.preInitQueue.size()).isEqualTo(1);
//    assertThat(regionAdvisor.getBucketAdvisor(bucketId).getNumProfiles()).isEqualTo(0);

    regionAdvisor.removeIdAndBucket(bucketId, memberId, serial, regionDestroyed);
    assertThat(regionAdvisor.preInitQueue).isNotNull();
    assertThat(regionAdvisor.preInitQueue.size()).isEqualTo(1);

    regionAdvisor.processProfilesQueuedDuringInitialization();
    assertThat(regionAdvisor.preInitQueue).isNull();
    assertThat(regionAdvisor.getBucketAdvisor(bucketId).getNumProfiles()).isEqualTo(0);
  }

  @Test
  public void testRemoveIdAndBucketWithNullPreInitQueue() {
    int bucketId = 0;
    int serial = 5678;
    boolean regionDestroyed = true;
    InternalDistributedMember memberId = mock(InternalDistributedMember.class);
    Bucket bucket = mock(Bucket.class);
    when(bucket.isHosting()).thenReturn(true);
    when(bucket.getId()).thenReturn(bucketId);
    int version = 0;
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getTotalNumBuckets()).thenReturn(1);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionAttributes.getColocatedWith()).thenReturn(null);
    InternalCache internalCache = mock(InternalCache.class);
    when(partitionedRegion.getCache()).thenReturn(internalCache);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(internalCache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(regionAdvisor.getDistributionManager()).thenReturn(distributionManager);
    when(distributionManager.isCurrentMember(any())).thenReturn(true);
    DataPolicy dataPolicy = mock(DataPolicy.class);
    when(partitionedRegion.getDataPolicy()).thenReturn(dataPolicy);
    when(dataPolicy.withPersistence()).thenReturn(false);
    regionAdvisor.initializeRegionAdvisor();
    BucketAdvisor.BucketProfile bucketProfile = new BucketAdvisor.BucketProfile(memberId, version, bucket);
//    regionAdvisor.putBucketProfile(bucketId, bucketProfile);
    assertThat(regionAdvisor.preInitQueue).isNotNull();
//    assertThat(regionAdvisor.preInitQueue.size()).isEqualTo(1);
//    assertThat(regionAdvisor.getBucketAdvisor(bucketId).getNumProfiles()).isEqualTo(0);

    regionAdvisor.processProfilesQueuedDuringInitialization();
    assertThat(regionAdvisor.preInitQueue).isNull();
    assertThat(regionAdvisor.getBucketAdvisor(bucketId).getNumProfiles()).isEqualTo(0);
//    assertThat(regionAdvisor.getBucketAdvisor(bucketId).getProfile(memberId)).isEqualTo(bucketProfile);

    regionAdvisor.removeIdAndBucket(bucketId, memberId, serial, regionDestroyed);
    assertThat(regionAdvisor.preInitQueue).isNull();
    assertThat(regionAdvisor.getBucketAdvisor(bucketId).getNumProfiles()).isEqualTo(0);
  }
}
