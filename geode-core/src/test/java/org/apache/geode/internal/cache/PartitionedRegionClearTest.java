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

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static org.apache.geode.internal.cache.EnumListenerEvent.AFTER_REGION_CLEAR;
import static org.apache.geode.internal.cache.FilterProfile.NO_PROFILES;
import static org.apache.geode.internal.cache.PartitionedRegionClear.CLEAR_OPERATION;
import static org.apache.geode.internal.cache.PartitionedRegionClearMessage.OperationType.OP_LOCK_FOR_PR_CLEAR;
import static org.apache.geode.internal.cache.PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR;
import static org.apache.geode.internal.cache.PartitionedRegionClearMessage.OperationType.OP_UNLOCK_FOR_PR_CLEAR;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.OngoingStubbing;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion.RetryTimeKeeper;
import org.apache.geode.internal.cache.PartitionedRegionClear.AssignBucketsToPartitions;
import org.apache.geode.internal.cache.PartitionedRegionClear.ColocationLeaderRegionProvider;
import org.apache.geode.internal.cache.PartitionedRegionClear.PartitionedRegionClearListener;
import org.apache.geode.internal.cache.PartitionedRegionClear.UpdateAttributesProcessorFactory;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.serialization.KnownVersion;

public class PartitionedRegionClearTest {

  private AssignBucketsToPartitions assignBucketsToPartitions;
  private BucketAdvisor bucketAdvisor;
  private InternalCache cache;
  private ColocationLeaderRegionProvider colocationLeaderRegionProvider;
  private PartitionedRegionDataStore dataStore;
  private DistributedLockService distributedLockService;
  private DistributionManager distributionManager;
  private InternalCacheEvent event;
  private FilterProfile filterProfile;
  private FilterRoutingInfo filterRoutingInfo1;
  private FilterRoutingInfo filterRoutingInfo2;
  private InternalDistributedMember member;
  private PartitionedRegion partitionedRegion;
  private RegionAdvisor regionAdvisor;
  private RetryTimeKeeper retryTimeKeeper;
  private UpdateAttributesProcessor updateAttributesProcessor;
  private UpdateAttributesProcessorFactory updateAttributesProcessorFactory;

  private PartitionedRegionClear partitionedRegionClear;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    assignBucketsToPartitions = mock(AssignBucketsToPartitions.class);
    bucketAdvisor = mock(BucketAdvisor.class);
    cache = mock(InternalCache.class);
    colocationLeaderRegionProvider = mock(ColocationLeaderRegionProvider.class);
    dataStore = mock(PartitionedRegionDataStore.class);
    distributedLockService = mock(DistributedLockService.class);
    distributionManager = mock(DistributionManager.class);
    event = mock(InternalCacheEvent.class);
    filterProfile = mock(FilterProfile.class);
    filterRoutingInfo1 = mock(FilterRoutingInfo.class);
    filterRoutingInfo2 = mock(FilterRoutingInfo.class);
    member = mock(InternalDistributedMember.class);
    partitionedRegion = mock(PartitionedRegion.class);
    regionAdvisor = mock(RegionAdvisor.class);
    retryTimeKeeper = mock(RetryTimeKeeper.class);
    updateAttributesProcessor = mock(UpdateAttributesProcessor.class);
    updateAttributesProcessorFactory = mock(UpdateAttributesProcessorFactory.class);
  }

  @Test
  public void acquireDistributedClearLock_locksPartitionedRegionNameForClearOperation() {
    // arrange
    when(distributedLockService.lock(anyString(), anyLong(), anyLong())).thenReturn(true);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");

    String lockName = CLEAR_OPERATION + partitionedRegion.getName();

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.acquireDistributedClearLock(lockName);

    // assert
    verify(distributedLockService).lock(lockName, -1, -1);
  }

  @Test
  public void releaseDistributedClearLock_unlocksPartitionedRegionNameForClearOperation() {
    // arrange
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    doNothing().when(distributedLockService).unlock(anyString());

    String lockName = CLEAR_OPERATION + partitionedRegion.getName();

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.releaseDistributedClearLock(lockName);

    // assert
    verify(distributedLockService).unlock(lockName);
  }

  @Test
  public void clearRegion_clearsLocalBuckets() throws Exception {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(emptySet())
        .when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(eq(event), eq(OP_PR_CLEAR), anyBoolean());

    // act
    spyPartitionedRegionClear.clearRegion(event);

    // assert
    verify(spyPartitionedRegionClear).clearLocalBuckets(event);
  }

  @Test
  public void clearRegion_sendsPartitionedRegionClearMessage() throws Exception {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(emptySet())
        .when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(eq(event), eq(OP_PR_CLEAR), anyBoolean());

    // act
    spyPartitionedRegionClear.clearRegion(event);

    // assert
    verify(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(event, OP_PR_CLEAR);
  }

  @Test
  public void waitForPrimary_doesNotWaitForBucketsRecovery_whenAllPrimaryBucketsAreFound() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);

    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    bucketRegions
        .forEach(bucketRegion -> when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor));
    when(dataStore.getAllLocalBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.waitForPrimary(retryTimeKeeper);

    // assert
    verify(retryTimeKeeper, never()).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimary_waitsForBucketsRecovery_whenRetryingToFindPrimaryBuckets() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);

    when(bucketAdvisor.hasPrimary())
        .thenReturn(false)
        .thenReturn(true);
    bucketRegions
        .forEach(bucketRegion -> when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor));
    when(dataStore.getAllLocalBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.waitForPrimary(retryTimeKeeper);

    // assert
    verify(retryTimeKeeper).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimary_throwsPartitionedRegionPartialClearException_whenRetryTimerIsOverMaximum() {
    // arrange
    BucketRegion bucketRegion = mock(BucketRegion.class);

    when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
    when(dataStore.getAllLocalBucketRegions()).thenReturn(singleton(bucketRegion));
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    when(retryTimeKeeper.overMaximum()).thenReturn(true);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    Throwable thrown = catchThrowable(() -> partitionedRegionClear.waitForPrimary(retryTimeKeeper));

    // assert
    assertThat(thrown)
        .isInstanceOf(PartitionedRegionPartialClearException.class)
        .hasMessage(
            "Unable to find primary bucket region during clear operation on prRegion region.");
  }

  @Test
  public void waitForPrimary_doesNotWaitForBucketsRecovery_whenRetryTimerIsOverMaximum() {
    // arrange
    BucketRegion bucketRegion = mock(BucketRegion.class);

    when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
    when(dataStore.getAllLocalBucketRegions()).thenReturn(singleton(bucketRegion));
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    when(retryTimeKeeper.overMaximum()).thenReturn(true);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    Throwable thrown = catchThrowable(() -> partitionedRegionClear.waitForPrimary(retryTimeKeeper));
    assertThat(thrown).isNotNull();

    // assert
    verify(retryTimeKeeper, never()).waitForBucketsRecovery();
  }

  @Test
  public void clearLocalBuckets_clearsLocalPrimaryBuckets() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);

    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    stubBucketRegions(bucketRegions, bucketAdvisor, 0, 1, 0);
    when(dataStore.getAllLocalBucketRegions()).thenReturn(bucketRegions);
    when(dataStore.getAllLocalPrimaryBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    doNothing().when(dataStore).lockBucketCreationForRegionClear();

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.clearLocalBuckets(event);

    // assert
    ArgumentCaptor<RegionEventImpl> captor = forClass(RegionEventImpl.class);
    for (BucketRegion bucketRegion : bucketRegions) {
      verify(bucketRegion).cmnClearRegion(captor.capture(), eq(false), eq(true));
      Region<?, ?> region = captor.getValue().getRegion();
      assertThat(region).isEqualTo(bucketRegion);
    }
  }

  @Test
  public void clearLocalBuckets_returnsAllClearedBuckets_whenMembershipChangeForcesRetry() {
    // arrange
    Set<BucketRegion> firstBuckets = bucketRegions(2);
    Set<BucketRegion> secondBuckets = bucketRegions(3);
    Set<BucketRegion> allBuckets = union(firstBuckets, secondBuckets);
    InternalCacheEvent event = regionEvent();

    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    when(dataStore.getAllLocalBucketRegions()).thenReturn(firstBuckets);
    when(dataStore.getAllLocalPrimaryBucketRegions()).thenReturn(firstBuckets);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    doNothing().when(dataStore).lockBucketCreationForRegionClear();

    stubBucketRegions(firstBuckets, bucketAdvisor, 0, 1, 0);
    stubBucketRegions(secondBuckets, bucketAdvisor, 2, 1);

    // After the first try, add 3 extra buckets to the local bucket regions
    when(dataStore.getAllLocalBucketRegions())
        .thenReturn(firstBuckets)
        .thenReturn(allBuckets);
    when(dataStore.getAllLocalPrimaryBucketRegions())
        .thenReturn(firstBuckets)
        .thenReturn(allBuckets);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    when(spyPartitionedRegionClear.getAndClearMembershipChange())
        .thenReturn(true)
        .thenReturn(false);

    // act
    Set<Integer> bucketsCleared = spyPartitionedRegionClear.clearLocalBuckets(event);

    // assert
    assertThat(bucketsCleared).hasSameSizeAs(allBuckets);
  }

  @Test
  public void clearLocalBuckets_retriesClearOnNonClearedLocalPrimaryBuckets_whenMembershipChanges() {
    // arrange
    Set<BucketRegion> firstBuckets = bucketRegions(2);
    Set<BucketRegion> secondBuckets = bucketRegions(3);
    Set<BucketRegion> allBuckets = union(firstBuckets, secondBuckets);
    InternalCacheEvent event = regionEvent();

    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    when(dataStore.getAllLocalBucketRegions()).thenReturn(firstBuckets);
    when(dataStore.getAllLocalPrimaryBucketRegions()).thenReturn(firstBuckets);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    doNothing().when(dataStore).lockBucketCreationForRegionClear();

    stubBucketRegions(firstBuckets, bucketAdvisor, 0, 1, 0);
    stubBucketRegions(secondBuckets, bucketAdvisor, 2, 1);

    // After the first try, add 3 extra buckets to the local bucket regions
    when(dataStore.getAllLocalBucketRegions())
        .thenReturn(firstBuckets)
        .thenReturn(allBuckets);
    when(dataStore.getAllLocalPrimaryBucketRegions())
        .thenReturn(firstBuckets)
        .thenReturn(allBuckets);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    when(spyPartitionedRegionClear.getAndClearMembershipChange())
        .thenReturn(true)
        .thenReturn(false);

    // act
    spyPartitionedRegionClear.clearLocalBuckets(event);

    // assert
    ArgumentCaptor<RegionEventImpl> captor = forClass(RegionEventImpl.class);
    for (BucketRegion bucketRegion : allBuckets) {
      verify(bucketRegion).cmnClearRegion(captor.capture(), eq(false), eq(true));
      Region<?, ?> region = captor.getValue().getRegion();
      assertThat(region).isEqualTo(bucketRegion);
    }
  }

  @Test
  public void doAfterClear_setsLocalFilterInfoOnRegionClearEvent() {
    // arrange
    InternalCacheEvent event = regionEvent();

    when(filterProfile.getFilterRoutingInfoPart1(event, NO_PROFILES, emptySet()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(filterRoutingInfo1, event))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.doAfterClear(event);

    // assert
    verify(event).setLocalFilterInfo(any());
  }

  @Test
  public void doAfterClear_notifiesClients() {
    // arrange
    InternalCacheEvent event = regionEvent();

    when(filterProfile.getFilterRoutingInfoPart1(event, NO_PROFILES, emptySet()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(filterRoutingInfo1, event))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.doAfterClear(event);

    // assert
    verify(partitionedRegion).notifyBridgeClients(event);
  }

  @Test
  public void doAfterClear_dispatchesAfterRegionClearListenerEvents() {
    // arrange
    InternalCacheEvent event = regionEvent();

    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.doAfterClear(event);

    // assert
    verify(partitionedRegion).dispatchListenerEvent(AFTER_REGION_CLEAR, event);
  }

  @Test
  public void lockLocalPrimaryBucketsUnderLock_locksLocalPrimaryBuckets() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);

    when(dataStore.getAllLocalPrimaryBucketRegions()).thenReturn(bucketRegions);
    when(distributionManager.isCurrentMember(member)).thenReturn(true);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.lockLocalPrimaryBucketsUnderLock(member);

    // assert
    for (BucketRegion bucketRegion : bucketRegions) {
      verify(bucketRegion)
          .lockLocallyForClear(
              partitionedRegion.getDistributionManager(), partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void lockLocalPrimaryBucketsUnderLock_doesNotLockLocalPrimaryBuckets_whenMemberIsNotCurrent() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);

    when(distributionManager.isCurrentMember(member)).thenReturn(false);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.lockLocalPrimaryBucketsUnderLock(member);

    // assert
    for (BucketRegion bucketRegion : bucketRegions) {
      verify(bucketRegion, never())
          .lockLocallyForClear(
              partitionedRegion.getDistributionManager(), partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void unlockLocalPrimaryBucketsUnderLock_unlocksLocalPrimaryBuckets() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);

    when(dataStore.getAllLocalPrimaryBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    partitionedRegionClear.setLockedForTesting(member);

    // act
    partitionedRegionClear.unlockLocalPrimaryBucketsUnderLock();

    // assert
    bucketRegions
        .forEach(bucketRegion -> verify(bucketRegion).releaseLockLocallyForClear(null));
  }

  @Test
  public void unlockLocalPrimaryBucketsUnderLock_doesNothing_whenNotLocked() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.unlockLocalPrimaryBucketsUnderLock();

    // assert
    bucketRegions
        .forEach(bucketRegion -> verify(bucketRegion, never()).releaseLockLocallyForClear(null));
  }

  @Test
  public void sendPartitionedRegionClearMessage_distributedMessage() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();
    InternalDistributedMember otherMember = mock(InternalDistributedMember.class);
    Node node = mock(Node.class);
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    Region<String, PartitionRegionConfig> prRoot = uncheckedCast(mock(Region.class));
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);

    Set<Node> configNodes = singleton(node);
    Set<InternalDistributedMember> prNodes = singleton(otherMember);

    when(distributionManager.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(distributionManager.getId()).thenReturn(member);
    when(distributionManager.getStats()).thenReturn(mock(DMStats.class));
    when(cache.getTxManager()).thenReturn(txManager);
    when(partitionRegionConfig.getNodes()).thenReturn(configNodes);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getPRRoot()).thenReturn(prRoot);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(regionAdvisor);
    when(partitionedRegion.getSystem()).thenReturn(system);
    // noinspection SuspiciousMethodCalls
    when(prRoot.get(any())).thenReturn(partitionRegionConfig);
    when(node.getMemberId()).thenReturn(otherMember);
    when(regionAdvisor.adviseAllPRNodes()).thenReturn(prNodes);
    when(system.getDistributionManager()).thenReturn(distributionManager);
    when(txManager.isDistributed()).thenReturn(false);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.sendPartitionedRegionClearMessage(event, OP_PR_CLEAR);

    // assert
    verify(distributionManager).putOutgoing(any());
  }

  @Test
  public void doClear_assignsAllPrimaryBuckets_underDistributedLock() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(distributionManager.getId())
        .thenReturn(member);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(mock(UpdateAttributesProcessor.class));
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(event);

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    InOrder inOrder = inOrder(spyPartitionedRegionClear);
    inOrder.verify(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    inOrder.verify(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    inOrder.verify(spyPartitionedRegionClear).releaseDistributedClearLock(any());
  }

  @Test
  public void doClear_invokesCacheWriter_whenCacheWriteParameterIsTrue() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(distributionManager.getId())
        .thenReturn(member);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(mock(UpdateAttributesProcessor.class));
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(event);

    // act
    spyPartitionedRegionClear.doClear(event, true);

    // assert
    verify(spyPartitionedRegionClear).invokeCacheWriter(event);
  }

  @Test
  public void doClear_doesNotInvokeCacheWriter_whenCacheWriteParameterIsFalse() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(distributionManager.getId())
        .thenReturn(member);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(mock(UpdateAttributesProcessor.class));
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(event);

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    verify(spyPartitionedRegionClear, never()).invokeCacheWriter(event);
  }

  @Test
  public void doClear_throwsPartitionedRegionPartialClearException_whenNoBucketsAreCleared() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(distributionManager.getId())
        .thenReturn(member);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(partitionedRegion.getTotalNumberOfBuckets())
        .thenReturn(1);
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(updateAttributesProcessor);
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doNothing().when(spyPartitionedRegionClear).unlockLocalPrimaryBucketsUnderLock();
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).clearRegion(event);
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    Throwable thrown =
        catchThrowable(() -> spyPartitionedRegionClear.doClear(event, false));

    // assert
    assertThat(thrown)
        .isInstanceOf(PartitionedRegionPartialClearException.class)
        .hasMessage(
            "Unable to clear all the buckets from the partitioned region prRegion, either data (buckets) moved or member departed.");
  }

  @Test
  public void doClear_throwsUnsupportedOperationException_whenOneBucketPrimaryMemberIsTooOld() {
    // arrange
    InternalDistributedMember latestMember = mock(InternalDistributedMember.class);
    InternalDistributedMember oldMember = mock(InternalDistributedMember.class);

    when(latestMember.getVersion()).thenReturn(KnownVersion.getCurrentVersion());
    when(oldMember.getName()).thenReturn("oldMember");
    when(oldMember.getVersion()).thenReturn(KnownVersion.GEODE_1_11_0);
    when(partitionedRegion.getBucketPrimary(0)).thenReturn(latestMember);
    when(partitionedRegion.getBucketPrimary(1)).thenReturn(oldMember);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(2);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    Throwable thrown =
        catchThrowable(() -> partitionedRegionClear.doClear(event, false));

    // assert
    assertThat(thrown)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Server(s) [oldMember] version was too old (< GEODE 1.14.0) for partitioned region clear");
  }

  @Test
  public void handleClearFromDepartedMember_unlocksLocalPrimaryBucketsUnderLock_whenRequesterMemberDeparts() {
    // arrange
    InternalDistributedMember member = mock(InternalDistributedMember.class);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    partitionedRegionClear.setLockedForTesting(member);

    // partial mocking to verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    // act
    spyPartitionedRegionClear.handleClearFromDepartedMember(member);

    // assert
    verify(spyPartitionedRegionClear).unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void handleClearFromDepartedMember_doesNotUnlockLocalPrimaryBucketsUnderLock_whenNonRequesterMemberDeparts() {
    // arrange
    InternalDistributedMember nonRequesterMember = mock(InternalDistributedMember.class);
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    partitionedRegionClear.setLockedForTesting(requesterMember);

    // partial mocking to verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    // act
    spyPartitionedRegionClear.handleClearFromDepartedMember(nonRequesterMember);

    // assert
    verify(spyPartitionedRegionClear, never()).unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void create_addsMembershipListener() {
    // arrange
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    // act
    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // assert
    MembershipListener membershipListener =
        partitionedRegionClear.getPartitionedRegionClearListenerForTesting();
    verify(distributionManager).addMembershipListener(membershipListener);
  }

  @Test
  public void memberDeparted_unlocksLocalPrimaryBucketsUnderLock_whenRequesterDeparts() {
    // arrange
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);

    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);
    partitionedRegionClear.setLockedForTesting(requesterMember);

    // partial mocking to verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    MembershipListener membershipListener =
        new PartitionedRegionClearListener(spyPartitionedRegionClear);

    // act
    membershipListener.memberDeparted(distributionManager, requesterMember, true);

    // assert
    verify(spyPartitionedRegionClear).unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void memberDeparted_doesNotUnlockLocalPrimaryBucketsUnderLock_whenNonRequesterMemberDeparts() {
    // arrange
    InternalDistributedMember nonRequesterMember = mock(InternalDistributedMember.class);
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);
    partitionedRegionClear.setLockedForTesting(requesterMember);

    // partial mocking to verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    MembershipListener membershipListener =
        new PartitionedRegionClearListener(spyPartitionedRegionClear);

    // act
    membershipListener.memberDeparted(distributionManager, nonRequesterMember, true);

    // assert
    verify(spyPartitionedRegionClear, never()).unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void doClear_locksLocalPrimaryBuckets() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(member);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(updateAttributesProcessor);
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing()
        .when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    verify(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(member);
  }

  @Test
  public void doClear_sends_OP_LOCK_FOR_PR_CLEAR() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(member);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(updateAttributesProcessor);
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing()
        .when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(event, OP_LOCK_FOR_PR_CLEAR);
  }

  @Test
  public void doClear_sends_OP_PR_CLEAR() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(member);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(updateAttributesProcessor);
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing()
        .when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    verify(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), eq(OP_PR_CLEAR));
  }

  @Test
  public void doClear_unlocksLocalPrimaryBucketsUnderLock() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(member);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(updateAttributesProcessor);
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing()
        .when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    verify(spyPartitionedRegionClear).unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void doClear_sends_OP_UNLOCK_FOR_PR_CLEAR() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(member);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(filterRoutingInfo1);
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(filterRoutingInfo2);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(updateAttributesProcessor);
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing()
        .when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(eq(event), eq(OP_UNLOCK_FOR_PR_CLEAR), anyBoolean());
  }

  @Test
  public void doClear_sendsPartitionedRegionClearMessages_underCorrectLockOrdering() {
    // arrange
    InternalCacheEvent event = cloneableRegionEvent();

    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(member);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(mock(FilterRoutingInfo.class));
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(mock(FilterRoutingInfo.class));
    when(updateAttributesProcessorFactory.create(any()))
        .thenReturn(updateAttributesProcessor);
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing()
        .when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    InOrder inOrder = inOrder(spyPartitionedRegionClear);
    inOrder.verify(spyPartitionedRegionClear)
        .lockLocalPrimaryBucketsUnderLock(any());
    inOrder.verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OP_LOCK_FOR_PR_CLEAR));
    inOrder.verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OP_PR_CLEAR));
    inOrder.verify(spyPartitionedRegionClear)
        .unlockLocalPrimaryBucketsUnderLock();
    inOrder.verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OP_UNLOCK_FOR_PR_CLEAR), anyBoolean());
  }

  private Set<BucketRegion> bucketRegions(int bucketCount) {
    Set<BucketRegion> bucketRegions = new HashSet<>();
    for (int i = 0; i < bucketCount; i++) {
      BucketRegion bucketRegion = mock(BucketRegion.class);
      bucketRegions.add(bucketRegion);
    }
    return bucketRegions;
  }

  private static RegionEventImpl cloneableRegionEvent() {
    RegionEventImpl event = regionEvent();
    when(event.clone()).thenReturn(event);
    return event;
  }

  private static RegionEventImpl regionEvent() {
    return mock(RegionEventImpl.class);
  }

  @SafeVarargs
  private static <T> Set<T> union(Set<T>... values) {
    Set<T> union = new HashSet<>();
    for (Set<T> set : values) {
      union.addAll(set);
    }
    return union;
  }

  private static void stubBucketRegions(Set<BucketRegion> bucketRegions,
      BucketAdvisor bucketAdvisor, int startId, int... sizes) {
    int id = requireZeroOrGreater(startId);
    for (BucketRegion bucketRegion : bucketRegions) {
      when(bucketRegion.getBucketAdvisor()).thenReturn(requireNonNull(bucketAdvisor));
      when(bucketRegion.getId()).thenReturn(id);
      OngoingStubbing<Integer> whenBucketRegionSize = when(bucketRegion.size());
      for (int size : requireOneOrMore(sizes)) {
        whenBucketRegionSize = whenBucketRegionSize.thenReturn(size);
      }
      id++;
    }
  }

  private static int requireZeroOrGreater(int value) {
    assertThat(value).isGreaterThanOrEqualTo(0);
    return value;
  }

  private static int[] requireOneOrMore(int... values) {
    assertThat(values).hasSizeGreaterThan(0);
    return values;
  }
}
