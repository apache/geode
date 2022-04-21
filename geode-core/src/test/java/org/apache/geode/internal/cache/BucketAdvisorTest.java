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
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.cache.CacheServerImpl.CACHE_SERVER_BIND_ADDRESS_NOT_AVAILABLE_EXCEPTION_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;

class BucketAdvisorTest {
  @Mock
  private PartitionedRegion parent;
  @Mock
  private PartitionedRegion child1;
  @Mock
  private PartitionedRegion child2;
  @Mock
  private PartitionedRegion grandChild1_1;
  @Mock
  private PartitionedRegion grandChild1_2;
  @Mock
  private PartitionedRegion grandChild2_1;
  @Mock
  private RegionAdvisor parentRegionAdvisor;
  @Mock
  private RegionAdvisor child1RegionAdvisor;
  @Mock
  private RegionAdvisor child2RegionAdvisor;
  @Mock
  private RegionAdvisor grandChild1_1RegionAdvisor;
  @Mock
  private RegionAdvisor grandChild1_2RegionAdvisor;
  @Mock
  private RegionAdvisor grandChild2_1RegionAdvisor;
  @Mock
  private Bucket parentBucket;
  @Mock
  private Bucket child1Bucket;
  @Mock
  private Bucket child2Bucket;
  @Mock
  private Bucket grandChild1_1Bucket;
  @Mock
  private Bucket grandChild1_2Bucket;
  @Mock
  private Bucket grandChild2_1Bucket;

  private BucketAdvisor parentBucketAdvisor;
  private BucketAdvisor child1BucketAdvisor;
  private BucketAdvisor child2BucketAdvisor;
  private BucketAdvisor grandChild1_1BucketAdvisor;
  private BucketAdvisor grandChild1_2BucketAdvisor;
  private BucketAdvisor grandChild2_1BucketAdvisor;
  private AutoCloseable closeable;

  private final List<PartitionedRegion> parentColocatedRegions = new ArrayList<>();
  private final List<PartitionedRegion> child1ColocatedRegions = new ArrayList<>();
  private final List<PartitionedRegion> child2ColocatedRegions = new ArrayList<>();

  @Test
  void shouldBeMockable() throws Exception {
    BucketAdvisor mockBucketAdvisor = mock(BucketAdvisor.class);
    InternalDistributedMember mockInternalDistributedMember = mock(InternalDistributedMember.class);

    when(mockBucketAdvisor.basicGetPrimaryMember()).thenReturn(mockInternalDistributedMember);
    when(mockBucketAdvisor.getBucketRedundancy()).thenReturn(1);

    assertThat(mockBucketAdvisor.basicGetPrimaryMember()).isEqualTo(mockInternalDistributedMember);
    assertThat(mockBucketAdvisor.getBucketRedundancy()).isEqualTo(1);
  }

  @Test
  void whenServerStopsAfterTheFirstIsRunningCheckThenItShouldNotBeAddedToLocations() {
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
  void whenServerThrowsIllegalStateExceptionWithoutBindAddressMsgThenExceptionMustBeThrown() {
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
  void volunteerForPrimaryIgnoresMissingPrimaryElector() {
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
    assertThat(missingElectorId).isEqualTo(advisorSpy.getPrimaryElector());
    advisorSpy.volunteerForPrimary();
    verify(volunteeringDelegate).volunteerForPrimary();
  }

  private BucketAdvisor mockBucketAdvisorWithShadowBucketsDestroyedMap(
      Map<String, Boolean> shadowBuckets) {
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
  void markAllShadowBucketsAsNonDestroyedShouldClearTheShadowBucketsDestroyedMap() {
    Map<String, Boolean> buckets = of(SEPARATOR + "b1", false, SEPARATOR + "b2", true);
    BucketAdvisor bucketAdvisor = mockBucketAdvisorWithShadowBucketsDestroyedMap(buckets);

    assertThat(bucketAdvisor.destroyedShadowBuckets).isNotEmpty();
    bucketAdvisor.markAllShadowBucketsAsNonDestroyed();
    assertThat(bucketAdvisor.destroyedShadowBuckets).isEmpty();
  }

  @Test
  void markAllShadowBucketsAsDestroyedShouldSetTheFlagAsTrueForEveryKnownShadowBucket() {
    Map<String, Boolean> buckets =
        of(SEPARATOR + "b1", false, SEPARATOR + "b2", false, SEPARATOR + "b3", false);
    BucketAdvisor bucketAdvisor = mockBucketAdvisorWithShadowBucketsDestroyedMap(buckets);

    bucketAdvisor.destroyedShadowBuckets.forEach((k, v) -> assertThat(v).isFalse());
    bucketAdvisor.markAllShadowBucketsAsDestroyed();
    bucketAdvisor.destroyedShadowBuckets.forEach((k, v) -> assertThat(v).isTrue());
  }

  @Test
  void markShadowBucketAsDestroyedShouldSetTheFlagAsTrueOnlyForTheSpecificBucket() {
    Map<String, Boolean> buckets = of(SEPARATOR + "b1", false);
    BucketAdvisor bucketAdvisor = mockBucketAdvisorWithShadowBucketsDestroyedMap(buckets);

    // Known Shadow Bucket
    assertThat(bucketAdvisor.destroyedShadowBuckets.get(SEPARATOR + "b1")).isFalse();
    bucketAdvisor.markShadowBucketAsDestroyed(SEPARATOR + "b1");
    assertThat(bucketAdvisor.destroyedShadowBuckets.get(SEPARATOR + "b1")).isTrue();

    // Unknown Shadow Bucket
    assertThat(bucketAdvisor.destroyedShadowBuckets.get(SEPARATOR + "b5")).isNull();
    bucketAdvisor.markShadowBucketAsDestroyed(SEPARATOR + "b5");
    assertThat(bucketAdvisor.destroyedShadowBuckets.get(SEPARATOR + "b5")).isTrue();
  }

  @Test
  void isShadowBucketDestroyedShouldReturnCorrectly() {
    Map<String, Boolean> buckets = of(SEPARATOR + "b1", true, SEPARATOR + "b2", false);
    BucketAdvisor bucketAdvisor = mockBucketAdvisorWithShadowBucketsDestroyedMap(buckets);

    // Known Shadow Buckets
    assertThat(bucketAdvisor.isShadowBucketDestroyed(SEPARATOR + "b1")).isTrue();
    assertThat(bucketAdvisor.isShadowBucketDestroyed(SEPARATOR + "b2")).isFalse();

    // Unknown Shadow Bucket
    assertThat(bucketAdvisor.isShadowBucketDestroyed(SEPARATOR + "b5")).isFalse();
  }

  @Test
  void testGetAllHostingMembersReturnsNoMembersWhenBucketAdvisorHasNoProfiles() {
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
    bucketAdvisor.setInitialized();

    assertThat(bucketAdvisor.adviseInitialized().isEmpty()).isTrue();
  }

  @Test
  void testGetAllHostingMembersReturnsMemberWhenBucketAdvisorHasOneProfileWithHostingBucket() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    InternalDistributedMember memberId = new InternalDistributedMember("localhost", 321);

    when(distributionManager.getId()).thenReturn(memberId);

    Bucket bucket = mock(Bucket.class);
    when(bucket.isHosting()).thenReturn(true);
    when(bucket.isPrimary()).thenReturn(false);
    when(bucket.getDistributionManager()).thenReturn(distributionManager);

    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(partitionedRegion.getRedundantCopies()).thenReturn(0);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(new PartitionAttributesImpl());
    when(partitionedRegion.getRedundancyTracker())
        .thenReturn(mock(PartitionedRegionRedundancyTracker.class));

    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    when(regionAdvisor.getPartitionedRegion()).thenReturn(partitionedRegion);

    BucketAdvisor bucketAdvisor = BucketAdvisor.createBucketAdvisor(bucket, regionAdvisor);
    bucketAdvisor.setInitialized();

    BucketAdvisor.BucketProfile bp = new BucketAdvisor.BucketProfile(memberId, 0, bucket);

    assertThat(bucketAdvisor.putProfile(bp, true)).isTrue();
    assertThat(bucketAdvisor.adviseInitialized().size()).isEqualTo(1);
  }

  @Test
  void testGetAllHostingMembersReturnsMemberWhenBucketAdvisorHasTwoProfilesAndOneIsHostingBucket() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    InternalDistributedMember memberId = new InternalDistributedMember("localhost", 321);
    InternalDistributedMember memberId2 = new InternalDistributedMember("localhost", 323);

    when(distributionManager.getId()).thenReturn(memberId);

    Bucket bucket = mock(Bucket.class);
    when(bucket.isHosting()).thenReturn(true);
    when(bucket.isPrimary()).thenReturn(false);
    when(bucket.getDistributionManager()).thenReturn(distributionManager);

    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(partitionedRegion.getRedundantCopies()).thenReturn(0);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(new PartitionAttributesImpl());
    when(partitionedRegion.getRedundancyTracker())
        .thenReturn(mock(PartitionedRegionRedundancyTracker.class));

    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    when(regionAdvisor.getPartitionedRegion()).thenReturn(partitionedRegion);

    BucketAdvisor bucketAdvisor = BucketAdvisor.createBucketAdvisor(bucket, regionAdvisor);

    BucketAdvisor.BucketProfile bp = new BucketAdvisor.BucketProfile(memberId, 0, bucket);
    BucketAdvisor.BucketProfile bp2 = new BucketAdvisor.BucketProfile(memberId2, 0, bucket);
    bp2.isHosting = false;
    bp2.isInitializing = true;
    bp2.isPrimary = false;

    bucketAdvisor.setInitialized();
    assertThat(bucketAdvisor.putProfile(bp, true)).isTrue();
    assertThat(bucketAdvisor.putProfile(bp2, true)).isTrue();

    assertThat(bucketAdvisor.adviseInitialized().size()).isEqualTo(1);
  }

  @Test
  void checkIfAllColocatedChildBucketsBecomePrimaryReturnsTrueIfAllChildBucketsArePrimary() {
    initSetup();

    boolean allChildBucketsBecomesPrimary =
        parentBucketAdvisor.checkIfAllColocatedChildBucketsBecomePrimary();
    assertThat(allChildBucketsBecomesPrimary).isTrue();
  }

  @Test
  void checkIfAllColocatedChildBucketsBecomePrimaryReturnsFalseIfChild1BucketIsNotPrimary() {
    initSetup();
    doReturn(false).when(child1BucketAdvisor).isPrimary();

    boolean allChildBucketsBecomesPrimary =
        parentBucketAdvisor.checkIfAllColocatedChildBucketsBecomePrimary();
    assertThat(allChildBucketsBecomesPrimary).isFalse();
  }

  @Test
  void checkIfAllColocatedChildBucketsBecomePrimaryReturnsFalseIfChild2BucketIsNotPrimary() {
    initSetup();
    doReturn(false).when(child2BucketAdvisor).isPrimary();

    boolean allChildBucketsBecomesPrimary =
        parentBucketAdvisor.checkIfAllColocatedChildBucketsBecomePrimary();
    assertThat(allChildBucketsBecomesPrimary).isFalse();
  }

  @Test
  void checkIfAllColocatedChildBucketsBecomePrimaryReturnsFalseIfGrandChild1_1BucketIsNotPrimary() {
    initSetup();
    doReturn(false).when(grandChild1_1BucketAdvisor).isPrimary();

    boolean allChildBucketsBecomesPrimary =
        parentBucketAdvisor.checkIfAllColocatedChildBucketsBecomePrimary();
    assertThat(allChildBucketsBecomesPrimary).isFalse();
  }

  @Test
  void checkIfAllColocatedChildBucketsBecomePrimaryReturnsFalseIfGrandChild1_2BucketIsNotPrimary() {
    initSetup();
    doReturn(false).when(grandChild1_2BucketAdvisor).isPrimary();

    boolean allChildBucketsBecomesPrimary =
        parentBucketAdvisor.checkIfAllColocatedChildBucketsBecomePrimary();
    assertThat(allChildBucketsBecomesPrimary).isFalse();
  }

  @Test
  void checkIfAllColocatedChildBucketsBecomePrimaryReturnsFalseIfGrandChild2_1BucketIsNotPrimary() {
    initSetup();
    doReturn(false).when(grandChild2_1BucketAdvisor).isPrimary();

    boolean allChildBucketsBecomesPrimary =
        parentBucketAdvisor.checkIfAllColocatedChildBucketsBecomePrimary();
    assertThat(allChildBucketsBecomesPrimary).isFalse();
  }

  @Test
  void checkIfAllColocatedChildBucketsBecomePrimaryReturnsFalseIfParentIsNotPrimary() {
    initSetup();
    doReturn(false).when(parentBucketAdvisor).isPrimary();

    boolean allChildBucketsBecomesPrimary =
        parentBucketAdvisor.checkIfAllColocatedChildBucketsBecomePrimary();
    assertThat(allChildBucketsBecomesPrimary).isFalse();
  }

  private void initSetup() {
    parentColocatedRegions.add(child1);
    parentColocatedRegions.add(child2);
    child1ColocatedRegions.add(grandChild1_1);
    child1ColocatedRegions.add(grandChild1_2);
    child2ColocatedRegions.add(grandChild2_1);

    when(parentRegionAdvisor.getPartitionedRegion()).thenReturn(parent);
    when(child1RegionAdvisor.getPartitionedRegion()).thenReturn(child1);
    when(child2RegionAdvisor.getPartitionedRegion()).thenReturn(child2);
    when(grandChild1_1RegionAdvisor.getPartitionedRegion()).thenReturn(grandChild1_1);
    when(grandChild1_2RegionAdvisor.getPartitionedRegion()).thenReturn(grandChild1_2);
    when(grandChild2_1RegionAdvisor.getPartitionedRegion()).thenReturn(grandChild2_1);

    when(parentRegionAdvisor.getBucket(any(Integer.class))).thenReturn(parentBucket);
    when(child1RegionAdvisor.getBucket(any(Integer.class))).thenReturn(child1Bucket);
    when(child2RegionAdvisor.getBucket(any(Integer.class))).thenReturn(child2Bucket);
    when(grandChild1_1RegionAdvisor.getBucket(any(Integer.class))).thenReturn(grandChild1_1Bucket);
    when(grandChild1_2RegionAdvisor.getBucket(any(Integer.class))).thenReturn(grandChild1_2Bucket);
    when(grandChild2_1RegionAdvisor.getBucket(any(Integer.class))).thenReturn(grandChild2_1Bucket);

    List<PartitionedRegion> regions =
        Arrays.asList(parent, child1, child2, grandChild1_1, grandChild1_2, grandChild2_1);
    for (PartitionedRegion partitionedRegion : regions) {
      when(partitionedRegion.getPartitionAttributes()).thenReturn(new PartitionAttributesImpl());
    }
    List<Bucket> buckets = Arrays.asList(parentBucket, child1Bucket, child2Bucket,
        grandChild1_1Bucket, grandChild1_2Bucket, grandChild2_1Bucket);
    DistributionManager distributionManager = mock(DistributionManager.class);
    for (Bucket bucket : buckets) {
      when(bucket.getDistributionManager()).thenReturn(distributionManager);
    }

    when(parent.getRegionAdvisor()).thenReturn(parentRegionAdvisor);
    when(child1.getRegionAdvisor()).thenReturn(child1RegionAdvisor);
    when(child2.getRegionAdvisor()).thenReturn(child2RegionAdvisor);
    when(grandChild1_1.getRegionAdvisor()).thenReturn(grandChild1_1RegionAdvisor);
    when(grandChild1_2.getRegionAdvisor()).thenReturn(grandChild1_2RegionAdvisor);
    when(grandChild2_1.getRegionAdvisor()).thenReturn(grandChild2_1RegionAdvisor);

    List<BucketAdvisor> bucketAdvisors = new ArrayList<>();
    parentBucketAdvisor = spy(BucketAdvisor.createBucketAdvisor(parentBucket, parentRegionAdvisor));
    bucketAdvisors.add(parentBucketAdvisor);
    child1BucketAdvisor = spy(BucketAdvisor.createBucketAdvisor(child1Bucket, child1RegionAdvisor));
    bucketAdvisors.add(child1BucketAdvisor);
    child2BucketAdvisor = spy(BucketAdvisor.createBucketAdvisor(child2Bucket, child2RegionAdvisor));
    bucketAdvisors.add(child2BucketAdvisor);
    grandChild1_1BucketAdvisor =
        spy(BucketAdvisor.createBucketAdvisor(grandChild1_1Bucket, grandChild1_1RegionAdvisor));
    bucketAdvisors.add(grandChild1_1BucketAdvisor);
    grandChild1_2BucketAdvisor =
        spy(BucketAdvisor.createBucketAdvisor(grandChild1_2Bucket, grandChild1_2RegionAdvisor));
    bucketAdvisors.add(grandChild1_2BucketAdvisor);
    grandChild2_1BucketAdvisor =
        spy(BucketAdvisor.createBucketAdvisor(grandChild2_1Bucket, grandChild2_1RegionAdvisor));
    bucketAdvisors.add(grandChild2_1BucketAdvisor);

    doReturn(parentColocatedRegions).when(parentBucketAdvisor).getColocateNonShadowChildRegions();
    doReturn(child1ColocatedRegions).when(child1BucketAdvisor).getColocateNonShadowChildRegions();
    doReturn(child2ColocatedRegions).when(child2BucketAdvisor).getColocateNonShadowChildRegions();
    doReturn(Collections.EMPTY_LIST).when(grandChild1_1BucketAdvisor)
        .getColocateNonShadowChildRegions();
    doReturn(Collections.EMPTY_LIST).when(grandChild1_2BucketAdvisor)
        .getColocateNonShadowChildRegions();
    doReturn(Collections.EMPTY_LIST).when(grandChild2_1BucketAdvisor)
        .getColocateNonShadowChildRegions();

    when(parentBucket.getBucketAdvisor()).thenReturn(parentBucketAdvisor);
    when(child1Bucket.getBucketAdvisor()).thenReturn(child1BucketAdvisor);
    when(child2Bucket.getBucketAdvisor()).thenReturn(child2BucketAdvisor);
    when(grandChild1_1Bucket.getBucketAdvisor()).thenReturn(grandChild1_1BucketAdvisor);
    when(grandChild1_2Bucket.getBucketAdvisor()).thenReturn(grandChild1_2BucketAdvisor);
    when(grandChild2_1Bucket.getBucketAdvisor()).thenReturn(grandChild2_1BucketAdvisor);

    for (BucketAdvisor bucketAdvisor : bucketAdvisors) {
      doReturn(true).when(bucketAdvisor).isPrimary();
    }
  }

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void close() throws Exception {
    closeable.close();
  }

}
