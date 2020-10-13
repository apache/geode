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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.ServerVersionMismatchException;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.serialization.KnownVersion;

public class PartitionedRegionClearTest {


  private PartitionedRegionClear partitionedRegionClear;
  private DistributionManager distributionManager;
  private PartitionedRegion partitionedRegion;
  private RegionAdvisor regionAdvisor;
  private InternalDistributedMember internalDistributedMember;

  @Before
  public void setUp() {

    partitionedRegion = mock(PartitionedRegion.class);
    distributionManager = mock(DistributionManager.class);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");

    partitionedRegionClear = new PartitionedRegionClear(partitionedRegion);
    internalDistributedMember = mock(InternalDistributedMember.class);
    when(internalDistributedMember.getVersion()).thenReturn(KnownVersion.CURRENT);
    regionAdvisor = mock(RegionAdvisor.class);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(regionAdvisor);
    when(regionAdvisor.getDistributionManager()).thenReturn(distributionManager);
    when(distributionManager.getDistributionManagerId()).thenReturn(internalDistributedMember);
    when(distributionManager.getId()).thenReturn(internalDistributedMember);

  }

  private Set<BucketRegion> setupBucketRegions(
      PartitionedRegionDataStore partitionedRegionDataStore,
      BucketAdvisor bucketAdvisor) {
    final int numBuckets = 2;
    Set<BucketRegion> bucketRegions = new HashSet<>();
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion bucketRegion = mock(BucketRegion.class);
      when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
      when(bucketRegion.size()).thenReturn(1).thenReturn(0);
      when(bucketRegion.getId()).thenReturn(i);
      bucketRegions.add(bucketRegion);
    }

    when(partitionedRegionDataStore.getAllLocalBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegionDataStore.getAllLocalPrimaryBucketRegions()).thenReturn(bucketRegions);

    return bucketRegions;
  }

  @Test
  public void isLockedForListenerAndClientNotificationReturnsTrueWhenLocked() {
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(true);
    partitionedRegionClear.obtainClearLockLocal(internalDistributedMember);

    assertThat(partitionedRegionClear.isLockedForListenerAndClientNotification()).isTrue();
  }

  @Test
  public void isLockedForListenerAndClientNotificationReturnsFalseWhenMemberNotInTheSystemRequestsLock() {
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(false);

    assertThat(partitionedRegionClear.isLockedForListenerAndClientNotification()).isFalse();
  }

  @Test
  public void acquireDistributedClearLockGetsDistributedLock() {
    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    String lockName = PartitionedRegionClear.CLEAR_OPERATION + partitionedRegion.getName();
    when(partitionedRegion.getPartitionedRegionLockService()).thenReturn(distributedLockService);

    partitionedRegionClear.acquireDistributedClearLock(lockName);

    verify(distributedLockService, times(1)).lock(lockName, -1, -1);
  }

  @Test
  public void releaseDistributedClearLockReleasesDistributedLock() {
    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    String lockName = PartitionedRegionClear.CLEAR_OPERATION + partitionedRegion.getName();
    when(partitionedRegion.getPartitionedRegionLockService()).thenReturn(distributedLockService);

    partitionedRegionClear.releaseDistributedClearLock(lockName);

    verify(distributedLockService, times(1)).unlock(lockName);
  }

  @Test
  public void obtainLockForClearGetsLocalLockAndSendsMessageForRemote() throws Exception {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    Region<String, PartitionRegionConfig> region = mock(Region.class);
    when(partitionedRegion.getPRRoot()).thenReturn(region);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(regionEvent,
            PartitionedRegionClearMessage.OperationType.OP_LOCK_FOR_PR_CLEAR);

    spyPartitionedRegionClear.obtainLockForClear(regionEvent);

    verify(spyPartitionedRegionClear, times(1)).obtainClearLockLocal(internalDistributedMember);
    verify(spyPartitionedRegionClear, times(1)).sendPartitionedRegionClearMessage(regionEvent,
        PartitionedRegionClearMessage.OperationType.OP_LOCK_FOR_PR_CLEAR);
  }

  @Test
  public void releaseLockForClearReleasesLocalLockAndSendsMessageForRemote() throws Exception {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    Region<String, PartitionRegionConfig> region = mock(Region.class);
    when(partitionedRegion.getPRRoot()).thenReturn(region);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(regionEvent,
            PartitionedRegionClearMessage.OperationType.OP_UNLOCK_FOR_PR_CLEAR);

    spyPartitionedRegionClear.releaseLockForClear(regionEvent);

    verify(spyPartitionedRegionClear, times(1)).releaseClearLockLocal();
    verify(spyPartitionedRegionClear, times(1)).sendPartitionedRegionClearMessage(regionEvent,
        PartitionedRegionClearMessage.OperationType.OP_UNLOCK_FOR_PR_CLEAR);
  }

  @Test
  public void clearRegionClearsLocalAndSendsMessageForRemote() throws Exception {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    Region<String, PartitionRegionConfig> region = mock(Region.class);
    when(partitionedRegion.getPRRoot()).thenReturn(region);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(regionEvent,
            PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR);

    spyPartitionedRegionClear.clearRegion(regionEvent);

    verify(spyPartitionedRegionClear, times(1)).clearRegionLocal(regionEvent);
    verify(spyPartitionedRegionClear, times(1)).sendPartitionedRegionClearMessage(regionEvent,
        PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR);
  }

  @Test
  public void waitForPrimaryReturnsAfterFindingAllPrimary() {
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    PartitionedRegion.RetryTimeKeeper retryTimer = mock(PartitionedRegion.RetryTimeKeeper.class);

    partitionedRegionClear.waitForPrimary(retryTimer);

    verify(retryTimer, times(0)).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimaryReturnsAfterRetryForPrimary() {
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(false).thenReturn(true);
    setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    PartitionedRegion.RetryTimeKeeper retryTimer = mock(PartitionedRegion.RetryTimeKeeper.class);

    partitionedRegionClear.waitForPrimary(retryTimer);

    verify(retryTimer, times(1)).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimaryThrowsPartitionedRegionPartialClearException() {
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    PartitionedRegion.RetryTimeKeeper retryTimer = mock(PartitionedRegion.RetryTimeKeeper.class);
    when(retryTimer.overMaximum()).thenReturn(true);

    Throwable thrown = catchThrowable(() -> partitionedRegionClear.waitForPrimary(retryTimer));

    assertThat(thrown)
        .isInstanceOf(PartitionedRegionPartialClearException.class)
        .hasMessage(
            "Unable to find primary bucket region during clear operation on prRegion region.");
    verify(retryTimer, times(0)).waitForBucketsRecovery();
  }

  @Test
  public void clearRegionLocalCallsClearOnLocalPrimaryBucketRegions() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    doNothing().when(partitionedRegionDataStore).lockBucketCreationForRegionClear();
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);

    Set<Integer> bucketsCleared = partitionedRegionClear.clearRegionLocal(regionEvent);

    assertThat(bucketsCleared).hasSize(buckets.size());

    ArgumentCaptor<RegionEventImpl> argument = ArgumentCaptor.forClass(RegionEventImpl.class);
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(1)).cmnClearRegion(argument.capture(), eq(false), eq(true));
      RegionEventImpl bucketRegionEvent = argument.getValue();
      assertThat(bucketRegionEvent.getRegion()).isEqualTo(bucketRegion);
    }
  }

  @Test
  public void clearRegionLocalRetriesClearOnNonClearedLocalPrimaryBucketRegionsWhenMembershipChanges() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    doNothing().when(partitionedRegionDataStore).lockBucketCreationForRegionClear();
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);

    final int numExtraBuckets = 3;
    Set<BucketRegion> extraBuckets = new HashSet<>();
    for (int i = 0; i < numExtraBuckets; i++) {
      BucketRegion bucketRegion = mock(BucketRegion.class);
      when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
      when(bucketRegion.size()).thenReturn(1);
      when(bucketRegion.getId()).thenReturn(i + buckets.size());
      extraBuckets.add(bucketRegion);
    }
    Set<BucketRegion> allBuckets = new HashSet<>(buckets);
    allBuckets.addAll(extraBuckets);

    // After the first try, add 3 extra buckets to the local bucket regions
    when(partitionedRegionDataStore.getAllLocalBucketRegions()).thenReturn(buckets)
        .thenReturn(allBuckets);
    when(partitionedRegionDataStore.getAllLocalPrimaryBucketRegions()).thenReturn(buckets)
        .thenReturn(allBuckets);

    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    when(spyPartitionedRegionClear.getMembershipChange()).thenReturn(true).thenReturn(false);

    Set<Integer> bucketsCleared = spyPartitionedRegionClear.clearRegionLocal(regionEvent);

    int expectedClears = allBuckets.size();
    assertThat(bucketsCleared).hasSize(expectedClears);

    ArgumentCaptor<RegionEventImpl> argument = ArgumentCaptor.forClass(RegionEventImpl.class);
    for (BucketRegion bucketRegion : allBuckets) {
      verify(bucketRegion, times(1)).cmnClearRegion(argument.capture(), eq(false), eq(true));
      RegionEventImpl bucketRegionEvent = argument.getValue();
      assertThat(bucketRegionEvent.getRegion()).isEqualTo(bucketRegion);
    }
  }

  @Test
  public void doAfterClearCallsNotifyClientsWhenClientHaveInterests() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(true);
    FilterProfile filterProfile = mock(FilterProfile.class);
    FilterRoutingInfo filterRoutingInfo = mock(FilterRoutingInfo.class);
    when(filterProfile.getFilterRoutingInfoPart1(regionEvent, FilterProfile.NO_PROFILES,
        Collections.emptySet())).thenReturn(filterRoutingInfo);
    when(filterProfile.getFilterRoutingInfoPart2(filterRoutingInfo, regionEvent)).thenReturn(
        filterRoutingInfo);
    when(partitionedRegion.getFilterProfile()).thenReturn(filterProfile);

    partitionedRegionClear.doAfterClear(regionEvent);

    verify(regionEvent, times(1)).setLocalFilterInfo(any());
    verify(partitionedRegion, times(1)).notifyBridgeClients(regionEvent);
  }

  @Test
  public void doAfterClearDispatchesListenerEvents() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(true);

    partitionedRegionClear.doAfterClear(regionEvent);

    verify(partitionedRegion, times(1)).dispatchListenerEvent(
        EnumListenerEvent.AFTER_REGION_CLEAR, regionEvent);
  }

  @Test
  public void obtainClearLockLocalGetsLockOnPrimaryBuckets() {
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(true);

    partitionedRegionClear.obtainClearLockLocal(internalDistributedMember);

    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isSameAs(internalDistributedMember);
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(1)).lockLocallyForClear(partitionedRegion.getDistributionManager(),
          partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void obtainClearLockLocalDoesNotGetLocksOnPrimaryBucketsWhenMemberIsNotCurrent() {
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(false);

    partitionedRegionClear.obtainClearLockLocal(internalDistributedMember);

    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNull();
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(0)).lockLocallyForClear(partitionedRegion.getDistributionManager(),
          partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void releaseClearLockLocalReleasesLockOnPrimaryBuckets() {
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(true);
    partitionedRegionClear.lockForListenerAndClientNotification
        .setLocked(internalDistributedMember);

    partitionedRegionClear.releaseClearLockLocal();

    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(1)).releaseLockLocallyForClear(null);
    }
  }

  @Test
  public void releaseClearLockLocalDoesNotReleaseLocksOnPrimaryBucketsWhenMemberIsNotCurrent() {
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);

    partitionedRegionClear.releaseClearLockLocal();

    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNull();
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(0)).releaseLockLocallyForClear(null);
    }
  }

  @Test
  public void sendPartitionedRegionClearMessageSendsClearMessageToPRNodes() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    Region<String, PartitionRegionConfig> prRoot = mock(Region.class);
    when(partitionedRegion.getPRRoot()).thenReturn(prRoot);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    Set<InternalDistributedMember> prNodes = Collections.singleton(member);
    Node node = mock(Node.class);
    when(node.getMemberId()).thenReturn(member);
    Set<Node> configNodes = Collections.singleton(node);
    when(regionAdvisor.adviseAllPRNodes()).thenReturn(prNodes);
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    when(partitionRegionConfig.getNodes()).thenReturn(configNodes);
    when(prRoot.get(any())).thenReturn(partitionRegionConfig);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getSystem()).thenReturn(internalDistributedSystem);
    InternalCache internalCache = mock(InternalCache.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);
    when(txManager.isDistributed()).thenReturn(false);
    when(internalCache.getTxManager()).thenReturn(txManager);
    when(partitionedRegion.getCache()).thenReturn(internalCache);
    when(member.getVersion()).thenReturn(KnownVersion.getCurrentVersion());
    when(distributionManager.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(distributionManager.getStats()).thenReturn(mock(DMStats.class));

    partitionedRegionClear.sendPartitionedRegionClearMessage(regionEvent,
        PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR);

    verify(distributionManager, times(1)).putOutgoing(any());
  }



  @Test
  public void doClearAcquiresAndReleasesDistributedClearLockAndCreatesAllPrimaryBuckets() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    spyPartitionedRegionClear.doClear(regionEvent, false);

    verify(spyPartitionedRegionClear, times(1)).acquireDistributedClearLock(any());
    verify(spyPartitionedRegionClear, times(1)).releaseDistributedClearLock(any());
    verify(spyPartitionedRegionClear, times(1)).assignAllPrimaryBuckets();
  }

  @Test
  public void doClearInvokesCacheWriterWhenCacheWriteIsSet() {
    boolean cacheWrite = true;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);
    verify(spyPartitionedRegionClear, times(1)).invokeCacheWriter(regionEvent);
  }

  @Test
  public void doClearDoesNotInvokesCacheWriterWhenCacheWriteIsNotSet() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    verify(spyPartitionedRegionClear, times(0)).invokeCacheWriter(regionEvent);
  }

  @Test
  public void doClearObtainsAndReleasesLockForClearWhenRegionHasListener() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(true);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    verify(spyPartitionedRegionClear, times(1)).obtainLockForClear(regionEvent);
    verify(spyPartitionedRegionClear, times(1)).releaseLockForClear(regionEvent);
  }

  @Test
  public void doClearObtainsAndReleasesLockForClearWhenRegionHasClientInterest() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(false);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(true);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    verify(spyPartitionedRegionClear, times(1)).obtainLockForClear(regionEvent);
    verify(spyPartitionedRegionClear, times(1)).releaseLockForClear(regionEvent);
  }

  @Test
  public void doClearDoesNotObtainLockForClearWhenRegionHasNoListenerAndNoClientInterest() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(false);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    verify(spyPartitionedRegionClear, times(0)).obtainLockForClear(regionEvent);
    verify(spyPartitionedRegionClear, times(0)).releaseLockForClear(regionEvent);
  }

  @Test
  public void doClearThrowsPartitionedRegionPartialClearException() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(false);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(1);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(Collections.EMPTY_SET).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    Throwable thrown =
        catchThrowable(() -> spyPartitionedRegionClear.doClear(regionEvent, cacheWrite));

    assertThat(thrown)
        .isInstanceOf(PartitionedRegionPartialClearException.class)
        .hasMessage(
            "Unable to clear all the buckets from the partitioned region prRegion, either data (buckets) moved or member departed.");
  }

  @Test
  public void doClearThrowsServerVersionMismatchException() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(false);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(2);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(Collections.singleton("2")).when(spyPartitionedRegionClear).clearRegion(regionEvent);

    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    Region<String, PartitionRegionConfig> prRoot = mock(Region.class);
    when(partitionedRegion.getPRRoot()).thenReturn(prRoot);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    InternalDistributedMember oldMember = mock(InternalDistributedMember.class);
    Set<InternalDistributedMember> prNodes = new HashSet<>();
    prNodes.add(member);
    prNodes.add(oldMember);
    Node node = mock(Node.class);
    Node oldNode = mock(Node.class);
    when(member.getName()).thenReturn("member");
    when(oldMember.getName()).thenReturn("oldMember");
    when(node.getMemberId()).thenReturn(member);
    when(oldNode.getMemberId()).thenReturn(oldMember);
    Set<Node> configNodes = new HashSet<>();
    configNodes.add(node);
    configNodes.add(oldNode);
    when(partitionedRegion.getBucketPrimary(0)).thenReturn(member);
    when(partitionedRegion.getBucketPrimary(1)).thenReturn(oldMember);

    when(regionAdvisor.adviseAllPRNodes()).thenReturn(prNodes);
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    when(partitionRegionConfig.getNodes()).thenReturn(configNodes);
    when(prRoot.get(any())).thenReturn(partitionRegionConfig);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getSystem()).thenReturn(internalDistributedSystem);
    InternalCache internalCache = mock(InternalCache.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);
    when(txManager.isDistributed()).thenReturn(false);
    when(internalCache.getTxManager()).thenReturn(txManager);
    when(partitionedRegion.getCache()).thenReturn(internalCache);
    when(oldMember.getVersion()).thenReturn(KnownVersion.GEODE_1_11_0);
    when(member.getVersion()).thenReturn(KnownVersion.getCurrentVersion());
    when(distributionManager.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(distributionManager.getStats()).thenReturn(mock(DMStats.class));


    Throwable thrown =
        catchThrowable(() -> spyPartitionedRegionClear.doClear(regionEvent, cacheWrite));

    assertThat(thrown)
        .isInstanceOf(ServerVersionMismatchException.class)
        .hasMessage(
            "A server's [oldMember] version was too old (< GEODE 1.14.0) for : Partitioned Region Clear");
  }



  @Test
  public void handleClearFromDepartedMemberReleasesTheLockForRequesterDeparture() {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(member);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    spyPartitionedRegionClear.handleClearFromDepartedMember(member);

    verify(spyPartitionedRegionClear, times(1)).releaseClearLockLocal();
  }

  @Test
  public void handleClearFromDepartedMemberDoesNotReleasesTheLockForNonRequesterDeparture() {
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(requesterMember);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    spyPartitionedRegionClear.handleClearFromDepartedMember(member);

    verify(spyPartitionedRegionClear, times(0)).releaseClearLockLocal();
  }

  @Test
  public void partitionedRegionClearRegistersMembershipListener() {
    MembershipListener membershipListener =
        partitionedRegionClear.getPartitionedRegionClearListener();

    verify(distributionManager, times(1)).addMembershipListener(membershipListener);
  }

  @Test
  public void lockRequesterDepartureReleasesTheLock() {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(member);
    PartitionedRegionClear.PartitionedRegionClearListener partitionedRegionClearListener =
        partitionedRegionClear.getPartitionedRegionClearListener();

    partitionedRegionClearListener.memberDeparted(distributionManager, member, true);

    assertThat(partitionedRegionClear.getMembershipChange()).isTrue();
    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNull();
  }

  @Test
  public void nonLockRequesterDepartureDoesNotReleasesTheLock() {
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(requesterMember);
    PartitionedRegionClear.PartitionedRegionClearListener partitionedRegionClearListener =
        partitionedRegionClear.getPartitionedRegionClearListener();

    partitionedRegionClearListener.memberDeparted(distributionManager, member, true);

    assertThat(partitionedRegionClear.getMembershipChange()).isTrue();
    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNotNull();
  }
}
