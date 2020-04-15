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

import static com.google.common.collect.ImmutableMap.of;
import static org.apache.geode.internal.cache.CacheServerImpl.CACHE_SERVER_BIND_ADDRESS_NOT_AVAILABLE_EXCEPTION_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;

public class BucketAdvisorTest {

  @Test
  public void shouldBeMockable() throws Exception {
    BucketAdvisor mockBucketAdvisor = mock(BucketAdvisor.class);
    InternalDistributedMember mockInternalDistributedMember = mock(InternalDistributedMember.class);

    when(mockBucketAdvisor.basicGetPrimaryMember()).thenReturn(mockInternalDistributedMember);
    when(mockBucketAdvisor.getBucketRedundancy()).thenReturn(1);

    assertThat(mockBucketAdvisor.basicGetPrimaryMember()).isEqualTo(mockInternalDistributedMember);
    assertThat(mockBucketAdvisor.getBucketRedundancy()).isEqualTo(1);
  }

  @Test
  public void whenServerStopsAfterTheFirstIsRunningCheckThenItShouldNotBeAddedToLocations() {
    InternalCache mockCache = mock(InternalCache.class);
    ProxyBucketRegion mockBucket = mock(ProxyBucketRegion.class);
    RegionAdvisor mockRegionAdvisor = mock(RegionAdvisor.class);
    PartitionedRegion mockPartitionedRegion = mock(PartitionedRegion.class);
    @SuppressWarnings("rawtypes")
    PartitionAttributes mockPartitionAttributes = mock(PartitionAttributes.class);
    DistributionManager mockDistributionManager = mock(DistributionManager.class);
    List<CacheServer> cacheServers = new ArrayList<>();
    CacheServerImpl mockCacheServer = mock(CacheServerImpl.class);
    cacheServers.add(mockCacheServer);

    when(mockRegionAdvisor.getPartitionedRegion()).thenReturn(mockPartitionedRegion);
    when(mockPartitionedRegion.getPartitionAttributes()).thenReturn(mockPartitionAttributes);
    when(mockBucket.getCache()).thenReturn(mockCache);
    when(mockCache.getCacheServers()).thenReturn(cacheServers);
    when(mockPartitionAttributes.getColocatedWith()).thenReturn(null);
    when(mockBucket.getDistributionManager()).thenReturn(mockDistributionManager);
    doNothing().when(mockDistributionManager).addMembershipListener(any());
    when(mockCacheServer.isRunning()).thenReturn(true);
    when(mockCacheServer.getExternalAddress()).thenThrow(
        new IllegalStateException(CACHE_SERVER_BIND_ADDRESS_NOT_AVAILABLE_EXCEPTION_MESSAGE));

    BucketAdvisor bucketAdvisor = BucketAdvisor.createBucketAdvisor(mockBucket, mockRegionAdvisor);
    assertThat(bucketAdvisor.getBucketServerLocations(0).size()).isEqualTo(0);
  }

  @Test
  public void whenServerThrowsIllegalStateExceptionWithoutBindAddressMsgThenExceptionMustBeThrown() {
    InternalCache mockCache = mock(InternalCache.class);
    ProxyBucketRegion mockBucket = mock(ProxyBucketRegion.class);
    RegionAdvisor mockRegionAdvisor = mock(RegionAdvisor.class);
    PartitionedRegion mockPartitionedRegion = mock(PartitionedRegion.class);
    @SuppressWarnings("rawtypes")
    PartitionAttributes mockPartitionAttributes = mock(PartitionAttributes.class);
    DistributionManager mockDistributionManager = mock(DistributionManager.class);
    List<CacheServer> cacheServers = new ArrayList<>();
    CacheServerImpl mockCacheServer = mock(CacheServerImpl.class);
    cacheServers.add(mockCacheServer);

    when(mockRegionAdvisor.getPartitionedRegion()).thenReturn(mockPartitionedRegion);
    when(mockPartitionedRegion.getPartitionAttributes()).thenReturn(mockPartitionAttributes);
    when(mockBucket.getCache()).thenReturn(mockCache);
    when(mockCache.getCacheServers()).thenReturn(cacheServers);
    when(mockPartitionAttributes.getColocatedWith()).thenReturn(null);
    when(mockBucket.getDistributionManager()).thenReturn(mockDistributionManager);
    doNothing().when(mockDistributionManager).addMembershipListener(any());
    when(mockCacheServer.isRunning()).thenReturn(true);
    when(mockCacheServer.getExternalAddress()).thenThrow(new IllegalStateException());

    BucketAdvisor bucketAdvisor = BucketAdvisor.createBucketAdvisor(mockBucket, mockRegionAdvisor);
    assertThatThrownBy(() -> bucketAdvisor.getBucketServerLocations(0))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void volunteerForPrimaryIgnoresMissingPrimaryElector() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getId()).thenReturn(new InternalDistributedMember("localhost", 321));

    Bucket bucket = mock(Bucket.class);
    when(bucket.isHosting()).thenReturn(true);
    when(bucket.isPrimary()).thenReturn(false);
    when(bucket.getDistributionManager()).thenReturn(distributionManager);

    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(partitionedRegion.getRedundantCopies()).thenReturn(0);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(new PartitionAttributesImpl());
    when(partitionedRegion.getRedundancyTracker())
        .thenReturn(mock(PartitionedRegionRedundancyTracker.class));

    InternalDistributedMember missingElectorId = new InternalDistributedMember("localhost", 123);

    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    when(regionAdvisor.getPartitionedRegion()).thenReturn(partitionedRegion);
    // hasPartitionedRegion() is invoked twice - once in initializePrimaryElector() and then in
    // volunteerForPrimary(). Returning true first simulates a elector being
    // there when createBucketAtomically() initiates creation of a bucket. Returning
    // false the second time simulates the elector closing its region/cache before
    // we get to the point of volunteering for primary
    when(regionAdvisor.hasPartitionedRegion(Mockito.any(InternalDistributedMember.class)))
        .thenReturn(true,
            false);

    BucketAdvisor advisor = BucketAdvisor.createBucketAdvisor(bucket, regionAdvisor);
    BucketAdvisor advisorSpy = spy(advisor);
    doCallRealMethod().when(advisorSpy).exchangeProfiles();
    doCallRealMethod().when(advisorSpy).volunteerForPrimary();
    doReturn(true).when(advisorSpy).initializationGate();
    doReturn(true).when(advisorSpy).isHosting();

    BucketAdvisor.VolunteeringDelegate volunteeringDelegate =
        mock(BucketAdvisor.VolunteeringDelegate.class);
    advisorSpy.setVolunteeringDelegate(volunteeringDelegate);
    advisorSpy.initializePrimaryElector(missingElectorId);
    assertEquals(missingElectorId, advisorSpy.getPrimaryElector());
    advisorSpy.volunteerForPrimary();
    verify(volunteeringDelegate).volunteerForPrimary();
  }

  BucketAdvisor mockBucketAdvisorWithShadowBucketsDestroyedMap(Map<String, Boolean> shadowBuckets) {
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getId()).thenReturn(new InternalDistributedMember("localhost", 321));

    Bucket bucket = mock(Bucket.class);
    when(bucket.isHosting()).thenReturn(true);
    when(bucket.isPrimary()).thenReturn(false);
    when(bucket.getDistributionManager()).thenReturn(distributionManager);

    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(partitionedRegion.getRedundantCopies()).thenReturn(0);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(new PartitionAttributesImpl());
    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    when(regionAdvisor.getPartitionedRegion()).thenReturn(partitionedRegion);

    BucketAdvisor bucketAdvisor = BucketAdvisor.createBucketAdvisor(bucket, regionAdvisor);
    bucketAdvisor.destroyedShadowBuckets.putAll(shadowBuckets);

    return bucketAdvisor;
  }

  @Test
  public void markAllShadowBucketsAsNonDestroyedShouldClearTheShadowBucketsDestroyedMap() {
    Map<String, Boolean> buckets = of("/b1", false, "/b2", true);
    BucketAdvisor bucketAdvisor = mockBucketAdvisorWithShadowBucketsDestroyedMap(buckets);

    assertThat(bucketAdvisor.destroyedShadowBuckets).isNotEmpty();
    bucketAdvisor.markAllShadowBucketsAsNonDestroyed();
    assertThat(bucketAdvisor.destroyedShadowBuckets).isEmpty();
  }

  @Test
  public void markAllShadowBucketsAsDestroyedShouldSetTheFlagAsTrueForEveryKnownShadowBucket() {
    Map<String, Boolean> buckets = of("/b1", false, "/b2", false, "/b3", false);
    BucketAdvisor bucketAdvisor = mockBucketAdvisorWithShadowBucketsDestroyedMap(buckets);

    bucketAdvisor.destroyedShadowBuckets.forEach((k, v) -> assertThat(v).isFalse());
    bucketAdvisor.markAllShadowBucketsAsDestroyed();
    bucketAdvisor.destroyedShadowBuckets.forEach((k, v) -> assertThat(v).isTrue());
  }

  @Test
  public void markShadowBucketAsDestroyedShouldSetTheFlagAsTrueOnlyForTheSpecificBucket() {
    Map<String, Boolean> buckets = of("/b1", false);
    BucketAdvisor bucketAdvisor = mockBucketAdvisorWithShadowBucketsDestroyedMap(buckets);

    // Known Shadow Bucket
    assertThat(bucketAdvisor.destroyedShadowBuckets.get("/b1")).isFalse();
    bucketAdvisor.markShadowBucketAsDestroyed("/b1");
    assertThat(bucketAdvisor.destroyedShadowBuckets.get("/b1")).isTrue();

    // Unknown Shadow Bucket
    assertThat(bucketAdvisor.destroyedShadowBuckets.get("/b5")).isNull();
    bucketAdvisor.markShadowBucketAsDestroyed("/b5");
    assertThat(bucketAdvisor.destroyedShadowBuckets.get("/b5")).isTrue();
  }

  @Test
  public void isShadowBucketDestroyedShouldReturnCorrectly() {
    Map<String, Boolean> buckets = of("/b1", true, "/b2", false);
    BucketAdvisor bucketAdvisor = mockBucketAdvisorWithShadowBucketsDestroyedMap(buckets);

    // Known Shadow Buckets
    assertThat(bucketAdvisor.isShadowBucketDestroyed("/b1")).isTrue();
    assertThat(bucketAdvisor.isShadowBucketDestroyed("/b2")).isFalse();

    // Unknown Shadow Bucket
    assertThat(bucketAdvisor.isShadowBucketDestroyed("/b5")).isFalse();
  }
}
