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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion.RetryTimeKeeper;
import org.apache.geode.internal.cache.PartitionedRegionClear.PartitionedRegionClearListener;
import org.apache.geode.internal.cache.PartitionedRegionClearMessage.OperationType;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.serialization.KnownVersion;

public class PartitionedRegionClearTest {

  private GemFireCacheImpl cache;
  private HashSet<AsyncEventQueue> allAEQs = new HashSet<>();
  private PartitionedRegionClear partitionedRegionClear;
  private DistributionManager distributionManager;
  private PartitionedRegion partitionedRegion;
  private RegionAdvisor regionAdvisor;
  private InternalDistributedMember internalDistributedMember;

  @Before
  public void setUp() {

    cache = mock(GemFireCacheImpl.class);
    distributionManager = mock(DistributionManager.class);
    internalDistributedMember = mock(InternalDistributedMember.class);
    partitionedRegion = mock(PartitionedRegion.class);
    regionAdvisor = mock(RegionAdvisor.class);

    when(distributionManager.getDistributionManagerId()).thenReturn(internalDistributedMember);
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    when(internalDistributedMember.getVersion()).thenReturn(KnownVersion.CURRENT);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(cache.getAsyncEventQueues(false)).thenReturn(allAEQs);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    when(partitionedRegion.getRegionAdvisor()).thenReturn(regionAdvisor);
    when(regionAdvisor.getDistributionManager()).thenReturn(distributionManager);

    partitionedRegionClear = new PartitionedRegionClear(partitionedRegion);
  }

  @Test
  public void isLockedForListenerAndClientNotificationReturnsTrueWhenLocked() {
    // arrange
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(true);
    partitionedRegionClear.obtainClearLockLocal(internalDistributedMember);

    // act
    boolean result = partitionedRegionClear.isLockedForListenerAndClientNotification();

    // assert
    assertThat(result).isTrue();
  }

  @Test
  public void isLockedForListenerAndClientNotificationReturnsFalseWhenMemberNotInTheSystemRequestsLock() {
    // arrange
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(false);

    // act
    boolean result = partitionedRegionClear.isLockedForListenerAndClientNotification();

    // assert
    assertThat(result).isFalse();
  }

  @Test
  public void acquireDistributedClearLockGetsDistributedLock() {
    // arrange
    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    String lockName = PartitionedRegionClear.CLEAR_OPERATION + partitionedRegion.getName();
    when(partitionedRegion.getPartitionedRegionLockService()).thenReturn(distributedLockService);

    // act
    partitionedRegionClear.acquireDistributedClearLock(lockName);

    // assert
    verify(distributedLockService).lock(lockName, -1, -1);
  }

  @Test
  public void releaseDistributedClearLockReleasesDistributedLock() {
    // arrange
    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    String lockName = PartitionedRegionClear.CLEAR_OPERATION + partitionedRegion.getName();
    when(partitionedRegion.getPartitionedRegionLockService()).thenReturn(distributedLockService);

    // act
    partitionedRegionClear.releaseDistributedClearLock(lockName);

    // assert
    verify(distributedLockService).unlock(lockName);
  }

  @Test
  public void obtainLockForClearGetsLocalLockAndSendsMessageForRemote() throws Exception {
    // arrange
    Region<String, PartitionRegionConfig> region = uncheckedCast(mock(Region.class));
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    when(partitionedRegion.getPRRoot())
        .thenReturn(region);
    when(regionEvent.clone())
        .thenReturn(mock(RegionEventImpl.class));
    doReturn(emptySet())
        .when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(regionEvent,
            OperationType.OP_LOCK_FOR_PR_CLEAR);

    // act
    spyPartitionedRegionClear.obtainLockForClear(regionEvent);

    // assert
    verify(spyPartitionedRegionClear)
        .obtainClearLockLocal(internalDistributedMember);
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(regionEvent, OperationType.OP_LOCK_FOR_PR_CLEAR);
  }

  @Test
  public void releaseLockForClearReleasesLocalLockAndSendsMessageForRemote() throws Exception {
    // arrange
    Region<String, PartitionRegionConfig> region = uncheckedCast(mock(Region.class));
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    when(partitionedRegion.getPRRoot())
        .thenReturn(region);
    when(regionEvent.clone())
        .thenReturn(mock(RegionEventImpl.class));

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(emptySet())
        .when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(regionEvent,
            OperationType.OP_UNLOCK_FOR_PR_CLEAR);

    // act
    spyPartitionedRegionClear.releaseLockForClear(regionEvent);

    // assert
    verify(spyPartitionedRegionClear)
        .releaseClearLockLocal();
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(regionEvent, OperationType.OP_UNLOCK_FOR_PR_CLEAR);
  }

  @Test
  public void clearRegionClearsLocalAndSendsMessageForRemote() throws Exception {
    // arrange
    Region<String, PartitionRegionConfig> region = uncheckedCast(mock(Region.class));
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    when(partitionedRegion.getPRRoot())
        .thenReturn(region);
    when(regionEvent.clone())
        .thenReturn(mock(RegionEventImpl.class));

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(emptySet())
        .when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(regionEvent, OperationType.OP_PR_CLEAR);

    // act
    spyPartitionedRegionClear.clearRegion(regionEvent);

    // assert
    verify(spyPartitionedRegionClear)
        .clearRegionLocal(regionEvent);
    verify(spyPartitionedRegionClear)
        .sendPartitionedRegionClearMessage(regionEvent, OperationType.OP_PR_CLEAR);
  }

  @Test
  public void waitForPrimaryReturnsAfterFindingAllPrimary() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    RetryTimeKeeper retryTimer = mock(RetryTimeKeeper.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    setupBucketRegions(dataStore, bucketAdvisor);

    // act
    partitionedRegionClear.waitForPrimary(retryTimer);

    // assert
    verify(retryTimer, never()).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimaryReturnsAfterRetryForPrimary() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    RetryTimeKeeper retryTimer = mock(RetryTimeKeeper.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(false).thenReturn(true);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    setupBucketRegions(dataStore, bucketAdvisor);

    // act
    partitionedRegionClear.waitForPrimary(retryTimer);

    // assert
    verify(retryTimer).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimaryThrowsPartitionedRegionPartialClearException() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    RetryTimeKeeper retryTimer = mock(RetryTimeKeeper.class);
    when(partitionedRegion.getDataStore())
        .thenReturn(dataStore);
    when(retryTimer.overMaximum())
        .thenReturn(true);
    setupBucketRegions(dataStore, bucketAdvisor);

    // act
    Throwable thrown = catchThrowable(() -> partitionedRegionClear.waitForPrimary(retryTimer));

    // assert
    assertThat(thrown)
        .isInstanceOf(PartitionedRegionPartialClearException.class)
        .hasMessage(
            "Unable to find primary bucket region during clear operation on prRegion region.");
    verify(retryTimer, never())
        .waitForBucketsRecovery();
  }

  @Test
  public void clearRegionLocalCallsClearOnLocalPrimaryBucketRegions() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    doNothing().when(dataStore).lockBucketCreationForRegionClear();
    Set<BucketRegion> buckets = setupBucketRegions(dataStore, bucketAdvisor);

    // act
    Set<Integer> bucketsCleared = partitionedRegionClear.clearRegionLocal(regionEvent);

    // assert
    assertThat(bucketsCleared).hasSameSizeAs(buckets);
    ArgumentCaptor<RegionEventImpl> captor = forClass(RegionEventImpl.class);
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion).cmnClearRegion(captor.capture(), eq(false), eq(true));
      Region<?, ?> region = captor.getValue().getRegion();
      assertThat(region).isEqualTo(bucketRegion);
    }
  }

  @Test
  public void clearRegionLocalRetriesClearOnNonClearedLocalPrimaryBucketRegionsWhenMembershipChanges() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    when(bucketAdvisor.hasPrimary())
        .thenReturn(true);
    doNothing()
        .when(dataStore)
        .lockBucketCreationForRegionClear();

    Set<BucketRegion> buckets = setupBucketRegions(dataStore, bucketAdvisor);

    Set<BucketRegion> allBuckets = new HashSet<>(buckets);
    for (int i = 0; i < 3; i++) {
      BucketRegion bucketRegion = mock(BucketRegion.class);
      when(bucketRegion.getBucketAdvisor())
          .thenReturn(bucketAdvisor);
      when(bucketRegion.getId())
          .thenReturn(i + buckets.size());
      when(bucketRegion.size())
          .thenReturn(1);
      allBuckets.add(bucketRegion);
    }

    // After the first try, add 3 extra buckets to the local bucket regions
    when(dataStore.getAllLocalBucketRegions())
        .thenReturn(buckets)
        .thenReturn(allBuckets);
    when(dataStore.getAllLocalPrimaryBucketRegions())
        .thenReturn(buckets)
        .thenReturn(allBuckets);
    when(partitionedRegion.getDataStore())
        .thenReturn(dataStore);

    // partial mocking to stub some methods
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    when(spyPartitionedRegionClear.getMembershipChange())
        .thenReturn(true)
        .thenReturn(false);

    // act
    Set<Integer> bucketsCleared = spyPartitionedRegionClear.clearRegionLocal(regionEvent);

    // assert
    assertThat(bucketsCleared).hasSameSizeAs(allBuckets);
    ArgumentCaptor<RegionEventImpl> captor = forClass(RegionEventImpl.class);
    for (BucketRegion bucketRegion : allBuckets) {
      verify(bucketRegion)
          .cmnClearRegion(captor.capture(), eq(false), eq(true));
      Region<?, ?> region = captor.getValue().getRegion();
      assertThat(region).isEqualTo(bucketRegion);
    }
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
    when(partitionedRegion.getFilterProfile()).thenReturn(filterProfile);
    when(partitionedRegion.hasAnyClientsInterested())
        .thenReturn(true);

    // act
    partitionedRegionClear.doAfterClear(regionEvent);

    // assert
    verify(regionEvent)
        .setLocalFilterInfo(any());
    verify(partitionedRegion)
        .notifyBridgeClients(regionEvent);
  }

  @Test
  public void doAfterClearDispatchesListenerEvents() {
    // arrange
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener())
        .thenReturn(true);

    // act
    partitionedRegionClear.doAfterClear(regionEvent);

    verify(partitionedRegion)
        .dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_CLEAR, regionEvent);
  }

  @Test
  public void obtainClearLockLocalGetsLockOnPrimaryBuckets() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);

    when(bucketAdvisor.hasPrimary())
        .thenReturn(true);
    when(distributionManager.isCurrentMember(internalDistributedMember))
        .thenReturn(true);
    when(partitionedRegion.getDataStore())
        .thenReturn(dataStore);

    Set<BucketRegion> buckets = setupBucketRegions(dataStore, bucketAdvisor);

    // act
    partitionedRegionClear.obtainClearLockLocal(internalDistributedMember);

    // assert
    // TODO: encapsulate lockForListenerAndClientNotification
    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isSameAs(internalDistributedMember);
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion)
          .lockLocallyForClear(partitionedRegion.getDistributionManager(),
              partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void obtainClearLockLocalDoesNotGetLocksOnPrimaryBucketsWhenMemberIsNotCurrent() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);

    when(bucketAdvisor.hasPrimary())
        .thenReturn(true);
    when(distributionManager.isCurrentMember(internalDistributedMember))
        .thenReturn(false);
    when(partitionedRegion.getDataStore())
        .thenReturn(dataStore);

    Set<BucketRegion> buckets = setupBucketRegions(dataStore, bucketAdvisor);

    // act
    partitionedRegionClear.obtainClearLockLocal(internalDistributedMember);

    // assert
    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNull();
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, never())
          .lockLocallyForClear(partitionedRegion.getDistributionManager(),
              partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void releaseClearLockLocalReleasesLockOnPrimaryBuckets() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);

    when(bucketAdvisor.hasPrimary())
        .thenReturn(true);
    when(distributionManager.isCurrentMember(internalDistributedMember))
        .thenReturn(true);
    when(partitionedRegion.getDataStore())
        .thenReturn(dataStore);

    Set<BucketRegion> buckets = setupBucketRegions(dataStore, bucketAdvisor);

    partitionedRegionClear.lockForListenerAndClientNotification
        .setLocked(internalDistributedMember);

    // act
    partitionedRegionClear.releaseClearLockLocal();

    // assert
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion)
          .releaseLockLocallyForClear(null);
    }
  }

  @Test
  public void releaseClearLockLocalDoesNotReleaseLocksOnPrimaryBucketsWhenMemberIsNotCurrent() {
    // arrange
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);

    when(bucketAdvisor.hasPrimary())
        .thenReturn(true);
    when(partitionedRegion.getDataStore())
        .thenReturn(dataStore);

    Set<BucketRegion> buckets = setupBucketRegions(dataStore, bucketAdvisor);

    // act
    partitionedRegionClear.releaseClearLockLocal();

    // assert
    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNull();
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, never())
          .releaseLockLocallyForClear(null);
    }
  }

  @Test
  public void sendPartitionedRegionClearMessageSendsClearMessageToPRNodes() {
    // arrange
    InternalCache internalCache = mock(InternalCache.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    Node node = mock(Node.class);
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    Region<String, PartitionRegionConfig> prRoot = uncheckedCast(mock(Region.class));
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);

    Set<InternalDistributedMember> prNodes = singleton(member);
    Set<Node> configNodes = singleton(node);

    when(distributionManager.getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
    when(distributionManager.getStats())
        .thenReturn(mock(DMStats.class));
    when(internalCache.getTxManager())
        .thenReturn(txManager);
    when(member.getVersion())
        .thenReturn(KnownVersion.getCurrentVersion());
    when(node.getMemberId())
        .thenReturn(member);
    when(partitionRegionConfig.getNodes())
        .thenReturn(configNodes);
    when(partitionedRegion.getPRRoot())
        .thenReturn(prRoot);
    when(regionAdvisor.adviseAllPRNodes())
        .thenReturn(prNodes);
    when(regionEvent.clone())
        .thenReturn(mock(RegionEventImpl.class));
    when(partitionedRegion.getSystem())
        .thenReturn(system);
    when(prRoot.get(anyString()))
        .thenReturn(partitionRegionConfig);
    when(system.getDistributionManager())
        .thenReturn(distributionManager);
    when(txManager.isDistributed())
        .thenReturn(false);
    when(partitionedRegion.getCache())
        .thenReturn(internalCache);

    // act
    partitionedRegionClear
        .sendPartitionedRegionClearMessage(regionEvent, OperationType.OP_PR_CLEAR);

    // assert
    verify(distributionManager)
        .putOutgoing(any());
  }

  @Test
  public void doClearAcquiresAndReleasesDistributedClearLockAndCreatesAllPrimaryBuckets() {
    // arrange
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    // act
    spyPartitionedRegionClear.doClear(regionEvent, false);

    // assert
    verify(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    verify(spyPartitionedRegionClear).releaseDistributedClearLock(any());
    verify(spyPartitionedRegionClear).assignAllPrimaryBuckets();
  }

  @Test
  public void doClearInvokesCacheWriterWhenCacheWriteIsSet() {
    // arrange
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    // act
    spyPartitionedRegionClear.doClear(regionEvent, true);

    // assert
    verify(spyPartitionedRegionClear).invokeCacheWriter(regionEvent);
  }

  @Test
  public void doClearDoesNotInvokesCacheWriterWhenCacheWriteIsNotSet() {
    // arrange
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    // act
    spyPartitionedRegionClear.doClear(regionEvent, false);

    // assert
    verify(spyPartitionedRegionClear, never()).invokeCacheWriter(regionEvent);
  }

  @Test
  public void doClearObtainsAndReleasesLockForClearWhenRegionHasListener() {
    // arrange
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    when(partitionedRegion.hasListener()).thenReturn(true);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    // act
    spyPartitionedRegionClear.doClear(regionEvent, false);

    // assert
    verify(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    verify(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
  }

  @Test
  public void doClearObtainsAndReleasesLockForClearWhenRegionHasClientInterest() {
    // arrange
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(true);
    when(partitionedRegion.hasListener()).thenReturn(false);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    // act
    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    // assert
    verify(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    verify(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
  }

  @Test
  public void doClearDoesNotObtainLockForClearWhenRegionHasNoListenerAndNoClientInterest() {
    // arrange
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    when(partitionedRegion.hasListener()).thenReturn(false);

    // partial mocking to stub some methods and verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    // act
    spyPartitionedRegionClear.doClear(regionEvent, false);

    // assert
    verify(spyPartitionedRegionClear, never()).obtainLockForClear(regionEvent);
    verify(spyPartitionedRegionClear, never()).releaseLockForClear(regionEvent);
  }

  @Test
  public void doClearThrowsPartitionedRegionPartialClearException() {
    // arrange
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);

    when(partitionedRegion.hasListener()).thenReturn(false);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(1);
    when(partitionedRegion.getName()).thenReturn("prRegion");

    // partial mocking to stub some methods
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(emptySet()).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    // act
    Throwable thrown =
        catchThrowable(() -> spyPartitionedRegionClear.doClear(regionEvent, false));

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

    Set<InternalDistributedMember> prNodes = new HashSet<>();
    prNodes.add(member);
    prNodes.add(oldMember);

    Set<Node> configNodes = new HashSet<>();
    configNodes.add(node);
    configNodes.add(oldNode);

    InternalCache cache = mock(InternalCache.class);
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    Region<String, PartitionRegionConfig> prRoot = uncheckedCast(mock(Region.class));
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);

    when(member.getName()).thenReturn("member");
    when(oldMember.getName()).thenReturn("oldMember");
    when(node.getMemberId()).thenReturn(member);
    when(oldNode.getMemberId()).thenReturn(oldMember);
    when(member.getVersion()).thenReturn(KnownVersion.getCurrentVersion());
    when(oldMember.getVersion()).thenReturn(KnownVersion.GEODE_1_11_0);

    when(cache.getTxManager()).thenReturn(txManager);
    when(distributionManager.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(distributionManager.getStats()).thenReturn(mock(DMStats.class));
    when(partitionRegionConfig.getNodes()).thenReturn(configNodes);
    when(partitionedRegion.getBucketPrimary(0)).thenReturn(member);
    when(partitionedRegion.getBucketPrimary(1)).thenReturn(oldMember);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    when(partitionedRegion.getPRRoot()).thenReturn(prRoot);
    when(partitionedRegion.getSystem()).thenReturn(system);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(2);
    when(partitionedRegion.hasListener()).thenReturn(false);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    when(prRoot.get(anyString())).thenReturn(partitionRegionConfig);
    when(regionAdvisor.adviseAllPRNodes()).thenReturn(prNodes);
    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    when(system.getDistributionManager()).thenReturn(distributionManager);
    when(txManager.isDistributed()).thenReturn(false);

    // partial mocking to stub some methods
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(singleton("2")).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    // act
    Throwable thrown =
        catchThrowable(() -> spyPartitionedRegionClear.doClear(regionEvent, false));

    // assert
    assertThat(thrown)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "A server's [oldMember] version was too old (< GEODE 1.14.0) for : Partitioned Region Clear");
  }

  @Test
  public void handleClearFromDepartedMemberReleasesTheLockForRequesterDeparture() {
    // arrange
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(member);

    // partial mocking to verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    // act
    spyPartitionedRegionClear.handleClearFromDepartedMember(member);

    // assert
    verify(spyPartitionedRegionClear).releaseClearLockLocal();
  }

  @Test
  public void handleClearFromDepartedMemberDoesNotReleasesTheLockForNonRequesterDeparture() {
    // arrange
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(requesterMember);

    // partial mocking to verify
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    // act
    spyPartitionedRegionClear.handleClearFromDepartedMember(member);

    // assert
    verify(spyPartitionedRegionClear, never()).releaseClearLockLocal();
  }

  @Test
  public void partitionedRegionClearRegistersMembershipListener() {
    // assert
    MembershipListener membershipListener =
        partitionedRegionClear.getPartitionedRegionClearListener();
    verify(distributionManager).addMembershipListener(membershipListener);
  }

  @Test
  public void lockRequesterDepartureReleasesTheLock() {
    // arrange
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(member);
    PartitionedRegionClearListener partitionedRegionClearListener =
        partitionedRegionClear.getPartitionedRegionClearListener();

    // act
    partitionedRegionClearListener.memberDeparted(distributionManager, member, true);

    // assert
    assertThat(partitionedRegionClear.getMembershipChange())
        .isTrue();
    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNull();
  }

  @Test
  public void nonLockRequesterDepartureDoesNotReleasesTheLock() {
    // arrange
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(requesterMember);
    PartitionedRegionClearListener partitionedRegionClearListener =
        partitionedRegionClear.getPartitionedRegionClearListener();

    // act
    partitionedRegionClearListener.memberDeparted(distributionManager, member, true);

    // assert
    assertThat(partitionedRegionClear.getMembershipChange())
        .isTrue();
    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNotNull();
  }

  private Set<BucketRegion> setupBucketRegions(
      PartitionedRegionDataStore dataStore,
      BucketAdvisor bucketAdvisor) {
    return setupBucketRegions(dataStore, bucketAdvisor, 2);
  }

  private Set<BucketRegion> setupBucketRegions(
      PartitionedRegionDataStore dataStore,
      BucketAdvisor bucketAdvisor,
      int bucketCount) {
    Set<BucketRegion> bucketRegions = new HashSet<>();

    for (int i = 0; i < bucketCount; i++) {
      BucketRegion bucketRegion = mock(BucketRegion.class);

      when(bucketRegion.getBucketAdvisor())
          .thenReturn(bucketAdvisor);
      when(bucketRegion.getId())
          .thenReturn(i);
      when(bucketRegion.size())
          .thenReturn(1)
          .thenReturn(0);

      bucketRegions.add(bucketRegion);
    }

    when(dataStore.getAllLocalBucketRegions())
        .thenReturn(bucketRegions);
    when(dataStore.getAllLocalPrimaryBucketRegions())
        .thenReturn(bucketRegions);

    return bucketRegions;
  }
}
