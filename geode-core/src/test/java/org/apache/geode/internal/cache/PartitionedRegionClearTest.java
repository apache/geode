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
import static org.apache.geode.internal.cache.FilterProfile.NO_PROFILES;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

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
import org.apache.geode.internal.cache.PartitionedRegionClearMessage.OperationType;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.serialization.KnownVersion;

public class PartitionedRegionClearTest {

  private AssignBucketsToPartitions assignBucketsToPartitions;
  private InternalCache cache;
  private ColocationLeaderRegionProvider colocationLeaderRegionProvider;
  private DistributedLockService distributedLockService;
  private DistributionManager distributionManager;
  private InternalCacheEvent event;
  private FilterProfile filterProfile;
  private InternalDistributedMember internalDistributedMember;
  private PartitionedRegion partitionedRegion;
  private RegionAdvisor regionAdvisor;
  private UpdateAttributesProcessorFactory updateAttributesProcessorFactory;

  private PartitionedRegionClear partitionedRegionClear;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    assignBucketsToPartitions = mock(AssignBucketsToPartitions.class);
    cache = mock(InternalCache.class);
    colocationLeaderRegionProvider = mock(ColocationLeaderRegionProvider.class);
    distributedLockService = mock(DistributedLockService.class);
    distributionManager = mock(DistributionManager.class);
    event = mock(InternalCacheEvent.class);
    filterProfile = mock(FilterProfile.class);
    internalDistributedMember = mock(InternalDistributedMember.class);
    partitionedRegion = mock(PartitionedRegion.class);
    regionAdvisor = mock(RegionAdvisor.class);
    updateAttributesProcessorFactory = mock(UpdateAttributesProcessorFactory.class);
  }

  @Test
  public void acquireDistributedClearLockGetsDistributedLock() {
    // arrange
    when(distributedLockService.lock(anyString(), anyLong(), anyLong())).thenReturn(true);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    String lockName = PartitionedRegionClear.CLEAR_OPERATION + partitionedRegion.getName();
    partitionedRegionClear.acquireDistributedClearLock(lockName);

    // assert
    verify(distributedLockService).lock(lockName, -1, -1);
  }

  @Test
  public void releaseDistributedClearLockReleasesDistributedLock() {
    // arrange
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    String lockName = PartitionedRegionClear.CLEAR_OPERATION + partitionedRegion.getName();
    partitionedRegionClear.releaseDistributedClearLock(lockName);

    // assert
    verify(distributedLockService).unlock(lockName);
  }

  @Test
  public void clearRegionClearsLocalAndSendsMessageForRemote() throws Exception {
    // arrange
    InternalCacheEvent event = regionClearEvent();

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(emptySet())
        .when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(event, OperationType.OP_PR_CLEAR);

    // act
    spyPartitionedRegionClear.clearRegion(event);

    // assert
    verify(spyPartitionedRegionClear)
        .clearLocalBuckets(event);
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(event, OperationType.OP_PR_CLEAR);
  }

  @Test
  public void waitForPrimaryReturnsAfterFindingAllPrimary() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    Set<BucketRegion> bucketRegions = bucketRegions(2);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    RetryTimeKeeper retryTimer = mock(RetryTimeKeeper.class);

    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    for (BucketRegion bucketRegion : bucketRegions) {
      when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
    }
    when(dataStore.getAllLocalBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.waitForPrimary(retryTimer);

    // assert
    verify(retryTimer, never()).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimaryReturnsAfterRetryForPrimary() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    Set<BucketRegion> bucketRegions = bucketRegions(2);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    RetryTimeKeeper retryTimer = mock(RetryTimeKeeper.class);

    when(bucketAdvisor.hasPrimary())
        .thenReturn(false)
        .thenReturn(true);
    for (BucketRegion bucketRegion : bucketRegions) {
      when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
    }
    when(dataStore.getAllLocalBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.waitForPrimary(retryTimer);

    // assert
    verify(retryTimer).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimaryThrowsPartitionedRegionPartialClearException() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    BucketRegion bucketRegion = mock(BucketRegion.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    RetryTimeKeeper retryTimer = mock(RetryTimeKeeper.class);

    when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
    when(dataStore.getAllLocalBucketRegions()).thenReturn(singleton(bucketRegion));
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    when(retryTimer.overMaximum()).thenReturn(true);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    Throwable thrown = catchThrowable(() -> partitionedRegionClear.waitForPrimary(retryTimer));

    // assert
    assertThat(thrown)
        .isInstanceOf(PartitionedRegionPartialClearException.class)
        .hasMessage(
            "Unable to find primary bucket region during clear operation on prRegion region.");
    verify(retryTimer, never()).waitForBucketsRecovery();
  }

  @Test
  public void clearRegionLocalCallsClearOnLocalPrimaryBucketRegions() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    Set<BucketRegion> bucketRegions = bucketRegions(2);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);

    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    int i = 0;
    for (BucketRegion bucketRegion : bucketRegions) {
      when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
      when(bucketRegion.getId()).thenReturn(i);
      when(bucketRegion.size())
          .thenReturn(1)
          .thenReturn(0);
      i++;
    }
    when(dataStore.getAllLocalBucketRegions()).thenReturn(bucketRegions);
    when(dataStore.getAllLocalPrimaryBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    doNothing().when(dataStore).lockBucketCreationForRegionClear();

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    Set<Integer> bucketsCleared = partitionedRegionClear.clearLocalBuckets(event);

    // assert
    assertThat(bucketsCleared).hasSameSizeAs(bucketRegions);
    ArgumentCaptor<RegionEventImpl> captor = forClass(RegionEventImpl.class);
    for (BucketRegion bucketRegion : bucketRegions) {
      verify(bucketRegion).cmnClearRegion(captor.capture(), eq(false), eq(true));
      Region<?, ?> region = captor.getValue().getRegion();
      assertThat(region).isEqualTo(bucketRegion);
    }
  }

  @Test
  public void clearRegionLocalRetriesClearOnNonClearedLocalPrimaryBucketRegionsWhenMembershipChanges() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    Set<BucketRegion> firstBuckets = bucketRegions(2);
    Set<BucketRegion> secondBuckets = bucketRegions(3);
    Set<BucketRegion> allBuckets = new HashSet<>();
    allBuckets.addAll(firstBuckets);
    allBuckets.addAll(secondBuckets);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    RegionEventImpl event = mock(RegionEventImpl.class);

    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    when(dataStore.getAllLocalBucketRegions()).thenReturn(firstBuckets);
    when(dataStore.getAllLocalPrimaryBucketRegions()).thenReturn(firstBuckets);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    doNothing().when(dataStore).lockBucketCreationForRegionClear();

    int i = 0;
    for (BucketRegion bucketRegion : firstBuckets) {
      when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
      when(bucketRegion.getId()).thenReturn(i);
      when(bucketRegion.size())
          .thenReturn(1)
          .thenReturn(0);
      i++;
    }
    for (BucketRegion bucketRegion : secondBuckets) {
      when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
      when(bucketRegion.getId()).thenReturn(i);
      when(bucketRegion.size()).thenReturn(1);
      i++;
    }

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
    ArgumentCaptor<RegionEventImpl> captor = forClass(RegionEventImpl.class);
    for (BucketRegion bucketRegion : allBuckets) {
      verify(bucketRegion).cmnClearRegion(captor.capture(), eq(false), eq(true));
      Region<?, ?> region = captor.getValue().getRegion();
      assertThat(region).isEqualTo(bucketRegion);
    }
    verify(spyPartitionedRegionClear, times(2)).getAndClearMembershipChange();
  }

  @Test
  public void doAfterClearCallsNotifyClientsWhenClientHaveInterests() {
    // arrange
    FilterProfile filterProfile = mock(FilterProfile.class);
    FilterRoutingInfo filterRoutingInfo = mock(FilterRoutingInfo.class);
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    when(filterProfile.getFilterRoutingInfoPart1(regionEvent, NO_PROFILES, emptySet()))
        .thenReturn(filterRoutingInfo);
    when(filterProfile.getFilterRoutingInfoPart2(filterRoutingInfo, regionEvent))
        .thenReturn(filterRoutingInfo);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getFilterProfile()).thenReturn(filterProfile);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(true);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.doAfterClear(regionEvent);

    // assert
    verify(regionEvent).setLocalFilterInfo(any());
    verify(partitionedRegion).notifyBridgeClients(regionEvent);
  }

  @Test
  public void doAfterClearDispatchesListenerEvents() {
    // arrange
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.hasListener()).thenReturn(true);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.doAfterClear(regionEvent);

    verify(partitionedRegion)
        .dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_CLEAR, regionEvent);
  }

  @Test
  public void obtainClearLockLocalGetsLockOnPrimaryBuckets() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);

    when(dataStore.getAllLocalPrimaryBucketRegions()).thenReturn(bucketRegions);
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(true);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.lockLocalPrimaryBucketsUnderLock(internalDistributedMember);

    // assert
    assertThat(partitionedRegionClear.getLockRequesterForTesting())
        .isSameAs(internalDistributedMember);
    for (BucketRegion bucketRegion : bucketRegions) {
      verify(bucketRegion)
          .lockLocallyForClear(partitionedRegion.getDistributionManager(),
              partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void obtainClearLockLocalDoesNotGetLocksOnPrimaryBucketsWhenMemberIsNotCurrent() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);

    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(false);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.lockLocalPrimaryBucketsUnderLock(internalDistributedMember);

    // assert
    assertThat(partitionedRegionClear.getLockRequesterForTesting()).isNull();
    for (BucketRegion bucketRegion : bucketRegions) {
      verify(bucketRegion, never())
          .lockLocallyForClear(partitionedRegion.getDistributionManager(),
              partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void releaseClearLockLocalReleasesLockOnPrimaryBuckets() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);

    when(dataStore.getAllLocalPrimaryBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    partitionedRegionClear.setLockedForTesting(internalDistributedMember);

    // act
    partitionedRegionClear.unlockLocalPrimaryBucketsUnderLock();

    // assert
    for (BucketRegion bucketRegion : bucketRegions) {
      verify(bucketRegion).releaseLockLocallyForClear(null);
    }
  }

  @Test
  public void releaseClearLockLocalDoesNotReleaseLocksOnPrimaryBucketsWhenMemberIsNotCurrent() {
    // arrange
    Set<BucketRegion> bucketRegions = bucketRegions(2);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear.unlockLocalPrimaryBucketsUnderLock();

    // assert
    assertThat(partitionedRegionClear.getLockRequesterForTesting()).isNull();
    for (BucketRegion bucketRegion : bucketRegions) {
      verify(bucketRegion, never()).releaseLockLocallyForClear(null);
    }
  }

  @Test
  public void sendPartitionedRegionClearMessageSendsClearMessageToPRNodes() {
    // arrange
    InternalCacheEvent event = regionClearEvent();
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    Node node = mock(Node.class);
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    Region<String, PartitionRegionConfig> prRoot = uncheckedCast(mock(Region.class));
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);

    Set<InternalDistributedMember> prNodes = singleton(member);
    Set<Node> configNodes = singleton(node);

    when(distributionManager.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    when(distributionManager.getStats()).thenReturn(mock(DMStats.class));
    when(cache.getTxManager()).thenReturn(txManager);
    when(partitionRegionConfig.getNodes()).thenReturn(configNodes);
    when(partitionedRegion.getCache()).thenReturn(this.cache);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getPRRoot()).thenReturn(prRoot);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(regionAdvisor);
    when(partitionedRegion.getSystem()).thenReturn(system);
    when(prRoot.get(any())).thenReturn(partitionRegionConfig);
    when(node.getMemberId()).thenReturn(member);
    when(regionAdvisor.adviseAllPRNodes()).thenReturn(prNodes);
    when(system.getDistributionManager()).thenReturn(distributionManager);
    when(txManager.isDistributed()).thenReturn(false);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // act
    partitionedRegionClear
        .sendPartitionedRegionClearMessage(event, OperationType.OP_PR_CLEAR);

    // assert
    verify(distributionManager).putOutgoing(any());
  }

  @Test
  public void doClearAcquiresAndReleasesDistributedClearLockAndCreatesAllPrimaryBuckets() {
    // arrange
    InternalCacheEvent event = regionClearEvent();

    when(cache.getAsyncEventQueues(false)).thenReturn(emptySet());
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
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
    verify(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    verify(spyPartitionedRegionClear).releaseDistributedClearLock(any());
    verify(spyPartitionedRegionClear).assignAllPrimaryBuckets();
  }

  @Test
  public void doClearInvokesCacheWriterWhenCacheWriteIsSet() {
    // arrange
    InternalCacheEvent event = regionClearEvent();

    when(cache.getAsyncEventQueues(false)).thenReturn(emptySet());
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
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
  public void doClearDoesNotInvokesCacheWriterWhenCacheWriteIsNotSet() {
    // arrange
    InternalCacheEvent event = regionClearEvent();

    when(cache.getAsyncEventQueues(false)).thenReturn(emptySet());
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
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
  public void doClearObtainsAndReleasesLockForClearWhenRegionHasListener() {
    // arrange
    when(cache.getAsyncEventQueues(false)).thenReturn(emptySet());
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doNothing().when(spyPartitionedRegionClear).unlockLocalPrimaryBucketsUnderLock();
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(event);
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    verify(spyPartitionedRegionClear)
        .lockLocalPrimaryBucketsUnderLock(any());
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_LOCK_FOR_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_UNLOCK_FOR_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void doClearObtainsAndReleasesLockForClearWhenRegionHasClientInterest() {
    // arrange
    when(cache.getAsyncEventQueues(false)).thenReturn(emptySet());
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doNothing().when(spyPartitionedRegionClear).unlockLocalPrimaryBucketsUnderLock();
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(event);
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    verify(spyPartitionedRegionClear)
        .lockLocalPrimaryBucketsUnderLock(any());
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_LOCK_FOR_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_UNLOCK_FOR_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void doClearObtainsLockForClearWhenRegionHasNoListenerAndNoClientInterest() {
    // arrange
    when(cache.getAsyncEventQueues(false)).thenReturn(emptySet());
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    doNothing().when(distributedLockService).unlock(anyString());

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing()
        .when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing()
        .when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing()
        .when(spyPartitionedRegionClear).lockLocalPrimaryBucketsUnderLock(any());
    doNothing()
        .when(spyPartitionedRegionClear).unlockLocalPrimaryBucketsUnderLock();
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).clearRegion(event);
    doReturn(emptySet())
        .when(spyPartitionedRegionClear).sendPartitionedRegionClearMessage(any(), any());

    // act
    spyPartitionedRegionClear.doClear(event, false);

    // assert
    verify(spyPartitionedRegionClear)
        .lockLocalPrimaryBucketsUnderLock(any());
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_LOCK_FOR_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_UNLOCK_FOR_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void doClearThrowsPartitionedRegionPartialClearException() {
    // arrange
    when(cache.getAsyncEventQueues(false)).thenReturn(emptySet());
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(1);
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
  public void doClearThrowsUnsupportedOperationException() {
    // arrange
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    InternalDistributedMember oldMember = mock(InternalDistributedMember.class);
    Node node = mock(Node.class);
    Node oldNode = mock(Node.class);

    Set<Node> configNodes = new HashSet<>();
    configNodes.add(node);
    configNodes.add(oldNode);

    Set<InternalDistributedMember> prNodes = new HashSet<>();
    prNodes.add(member);
    prNodes.add(oldMember);

    when(member.getVersion()).thenReturn(KnownVersion.getCurrentVersion());
    when(oldMember.getName()).thenReturn("oldMember");
    when(oldMember.getVersion()).thenReturn(KnownVersion.GEODE_1_11_0);
    when(partitionedRegion.getBucketPrimary(0)).thenReturn(member);
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
  public void handleClearFromDepartedMemberReleasesTheLockForRequesterDeparture() {
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
  public void handleClearFromDepartedMemberDoesNotReleasesTheLockForNonRequesterDeparture() {
    // arrange
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    partitionedRegionClear.setLockedForTesting(requesterMember);

    // partial mocking to verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    // act
    spyPartitionedRegionClear.handleClearFromDepartedMember(member);

    // assert
    verify(spyPartitionedRegionClear, never()).unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void partitionedRegionClearRegistersMembershipListener() {
    // arrange
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);

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
  public void lockRequesterDepartureReleasesTheLock() {
    // arrange
    InternalDistributedMember member = mock(InternalDistributedMember.class);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    partitionedRegionClear.setLockedForTesting(member);

    // act
    PartitionedRegionClearListener partitionedRegionClearListener =
        partitionedRegionClear.getPartitionedRegionClearListenerForTesting();
    partitionedRegionClearListener.memberDeparted(distributionManager, member, true);

    // assert
    assertThat(partitionedRegionClear.getAndClearMembershipChange()).isTrue();
    assertThat(partitionedRegionClear.getLockRequesterForTesting()).isNull();
  }

  @Test
  public void nonLockRequesterDepartureDoesNotReleasesTheLock() {
    // arrange
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = PartitionedRegionClear.create(partitionedRegion,
        distributedLockService, colocationLeaderRegionProvider, assignBucketsToPartitions,
        updateAttributesProcessorFactory);

    partitionedRegionClear.setLockedForTesting(requesterMember);

    // act
    PartitionedRegionClearListener partitionedRegionClearListener =
        partitionedRegionClear.getPartitionedRegionClearListenerForTesting();
    partitionedRegionClearListener.memberDeparted(distributionManager, member, true);

    // assert
    assertThat(partitionedRegionClear.getAndClearMembershipChange()).isTrue();
    assertThat(partitionedRegionClear.getLockRequesterForTesting()).isNotNull();
  }

  @Test
  public void doClearAcquiresLockForClearWhenHasAnyClientsInterestedIsTrue() {
    // arrange
    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(partitionedRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(partitionedRegion.hasAnyClientsInterested())
        .thenReturn(true);
    when(partitionedRegion.hasListener())
        .thenReturn(false);
    when(filterProfile.getFilterRoutingInfoPart1(any(), any(), any()))
        .thenReturn(mock(FilterRoutingInfo.class));
    when(filterProfile.getFilterRoutingInfoPart2(any(), any()))
        .thenReturn(mock(FilterRoutingInfo.class));
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
        .lockLocalPrimaryBucketsUnderLock(any());
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_LOCK_FOR_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_UNLOCK_FOR_PR_CLEAR));
  }

  @Test
  public void doClearAcquiresLockForClearWhenHasListenerIsTrue() {
    // arrange
    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(partitionedRegion.hasAnyClientsInterested())
        .thenReturn(false);
    when(partitionedRegion.hasListener())
        .thenReturn(true);
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
        .lockLocalPrimaryBucketsUnderLock(any());
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_LOCK_FOR_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(any(), eq(OperationType.OP_UNLOCK_FOR_PR_CLEAR));
    verify(spyPartitionedRegionClear)
        .unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void doClear_locksLocalPrimaryBuckets_whenHasAnyClientsInterestedAndHasListenerAreFalse() {
    // arrange
    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(partitionedRegion.hasAnyClientsInterested())
        .thenReturn(false);
    when(partitionedRegion.hasListener())
        .thenReturn(false);
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
        .lockLocalPrimaryBucketsUnderLock(internalDistributedMember);
  }

  @Test
  public void doClear_sendsOP_LOCK_FOR_PR_CLEAR_whenHasAnyClientsInterestedAndHasListenerAreFalse() {
    // arrange
    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(partitionedRegion.hasAnyClientsInterested())
        .thenReturn(false);
    when(partitionedRegion.hasListener())
        .thenReturn(false);
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
        .sendPartitionedRegionClearMessage(event, OperationType.OP_LOCK_FOR_PR_CLEAR);
  }

  @Test
  public void doClear_sendsOP_UNLOCK_FOR_PR_CLEAR_whenHasAnyClientsInterestedAndHasListenerAreFalse() {
    // arrange
    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(partitionedRegion.hasAnyClientsInterested())
        .thenReturn(false);
    when(partitionedRegion.hasListener())
        .thenReturn(false);
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
        .sendPartitionedRegionClearMessage(event, OperationType.OP_UNLOCK_FOR_PR_CLEAR);
    verify(spyPartitionedRegionClear)
        .unlockLocalPrimaryBucketsUnderLock();
  }

  @Test
  public void doClear_unlocksLocalPrimaryBuckets_whenHasAnyClientsInterestedAndHasListenerAreFalse() {
    // arrange
    when(cache.getAsyncEventQueues(false))
        .thenReturn(emptySet());
    when(colocationLeaderRegionProvider.getLeaderRegion(any()))
        .thenReturn(partitionedRegion);
    when(distributedLockService.lock(anyString(), anyLong(), anyLong()))
        .thenReturn(true);
    when(distributionManager.getId())
        .thenReturn(internalDistributedMember);
    when(partitionedRegion.getCache())
        .thenReturn(cache);
    when(partitionedRegion.getDistributionManager())
        .thenReturn(distributionManager);
    when(partitionedRegion.getName())
        .thenReturn("prRegion");
    when(partitionedRegion.hasAnyClientsInterested())
        .thenReturn(false);
    when(partitionedRegion.hasListener())
        .thenReturn(false);
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

  private Set<BucketRegion> bucketRegions(int bucketCount) {
    Set<BucketRegion> bucketRegions = new HashSet<>();
    for (int i = 0; i < bucketCount; i++) {
      BucketRegion bucketRegion = mock(BucketRegion.class);
      bucketRegions.add(bucketRegion);
    }
    return bucketRegions;
  }

  private InternalCacheEvent regionClearEvent() {
    RegionEventImpl event = mock(RegionEventImpl.class);
    when(event.clone()).thenReturn(event);
    return event;
  }
}
