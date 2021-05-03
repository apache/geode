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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.OperationAbortedException;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.InternalAsyncEventQueue;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion.RetryTimeKeeper;
import org.apache.geode.internal.cache.PartitionedRegionClearMessage.OperationType;
import org.apache.geode.internal.cache.PartitionedRegionClearMessage.PartitionedRegionClearResponse;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class PartitionedRegionClear {

  private static final Logger logger = LogService.getLogger();

  @VisibleForTesting
  static final String CLEAR_OPERATION = "_clearOperation";

  private final Duration retryTime = Duration.ofMinutes(2);

  private final AtomicReference<InternalDistributedMember> lockRequester =
      new AtomicReference<>();

  private final PartitionedRegionClearListener partitionedRegionClearListener =
      new PartitionedRegionClearListener();

  private final PartitionedRegion partitionedRegion;
  private final DistributedLockService distributedLockService;
  private final ColocationLeaderRegionProvider colocationLeaderRegionProvider;
  private final AssignBucketsToPartitions assignBucketsToPartitions;
  private final UpdateAttributesProcessorFactory updateAttributesProcessorFactory;

  public static PartitionedRegionClear create(PartitionedRegion partitionedRegion) {
    PartitionedRegionClear partitionedRegionClear = new PartitionedRegionClear(partitionedRegion);
    partitionedRegionClear.registerListener();
    return partitionedRegionClear;
  }

  @VisibleForTesting
  static PartitionedRegionClear create(PartitionedRegion partitionedRegion,
      DistributedLockService distributedLockService,
      ColocationLeaderRegionProvider colocationLeaderRegionProvider,
      AssignBucketsToPartitions assignBucketsToPartitions,
      UpdateAttributesProcessorFactory updateAttributesProcessorFactory) {
    PartitionedRegionClear partitionedRegionClear =
        new PartitionedRegionClear(partitionedRegion, distributedLockService,
            colocationLeaderRegionProvider,
            assignBucketsToPartitions, updateAttributesProcessorFactory);
    partitionedRegionClear.registerListener();
    return partitionedRegionClear;
  }

  private PartitionedRegionClear(PartitionedRegion partitionedRegion) {
    this(partitionedRegion,
        partitionedRegion.getPartitionedRegionLockService(),
        ColocationHelper::getLeaderRegion,
        PartitionRegionHelper::assignBucketsToPartitions,
        pr -> new UpdateAttributesProcessor(pr, true));
  }

  private PartitionedRegionClear(PartitionedRegion partitionedRegion,
      DistributedLockService distributedLockService,
      ColocationLeaderRegionProvider colocationLeaderRegionProvider,
      AssignBucketsToPartitions assignBucketsToPartitions,
      UpdateAttributesProcessorFactory updateAttributesProcessorFactory) {
    this.partitionedRegion = partitionedRegion;
    this.distributedLockService = distributedLockService;
    this.colocationLeaderRegionProvider = colocationLeaderRegionProvider;
    this.assignBucketsToPartitions = assignBucketsToPartitions;
    this.updateAttributesProcessorFactory = updateAttributesProcessorFactory;
  }

  @VisibleForTesting
  void acquireDistributedClearLock(String clearLock) {
    try {
      distributedLockService.lock(clearLock, -1, -1);
    } catch (IllegalStateException e) {
      partitionedRegion.lockCheckReadiness();
      throw e;
    }
  }

  @VisibleForTesting
  void releaseDistributedClearLock(String clearLock) {
    try {
      distributedLockService.unlock(clearLock);
    } catch (IllegalStateException e) {
      partitionedRegion.lockCheckReadiness();
    } catch (Exception e) {
      logger.warn("Caught exception while unlocking clear distributed lock.", e);
    }
  }

  /**
   * clears local primaries and send message to remote primaries to clear
   */
  @VisibleForTesting
  Set<Integer> clearRegion(InternalCacheEvent regionEvent) {
    // this includes all local primary buckets and their remote secondaries
    Set<Integer> localPrimaryBuckets = clearLocalBuckets(regionEvent);
    // this includes all remote primary buckets and their secondaries
    Set<Integer> remotePrimaryBuckets =
        sendPartitionedRegionClearMessage(regionEvent, OperationType.OP_PR_CLEAR);

    Set<Integer> allBucketsCleared = new HashSet<>();
    allBucketsCleared.addAll(localPrimaryBuckets);
    allBucketsCleared.addAll(remotePrimaryBuckets);
    return allBucketsCleared;
  }

  @VisibleForTesting
  void waitForPrimary(RetryTimeKeeper retryTimer) {
    boolean retry;
    do {
      retry = false;
      for (BucketRegion bucketRegion : partitionedRegion.getDataStore()
          .getAllLocalBucketRegions()) {
        if (!bucketRegion.getBucketAdvisor().hasPrimary()) {
          if (retryTimer.overMaximum()) {
            throw new PartitionedRegionPartialClearException(String.format(
                "Unable to find primary bucket region during clear operation on %s region.",
                partitionedRegion.getName()));
          }
          retryTimer.waitForBucketsRecovery();
          retry = true;
        }
      }
    } while (retry);
  }

  /**
   * this clears all local primary buckets (each will distribute the clear operation to its
   * secondary members) and all of their remote secondaries
   */
  @VisibleForTesting
  Set<Integer> clearLocalBuckets(InternalCacheEvent regionEvent) {
    CachePerfStats stats = partitionedRegion.getCachePerfStats();
    long startTime = stats != null ? System.nanoTime() : 0;
    partitionedRegionClearListener.setMembershipChange(false);
    // Synchronized to handle the requester departure.
    synchronized (lockRequester) {
      if (partitionedRegion.getDataStore() != null) {
        return clearLocalBucketsUnderLock(regionEvent, stats, startTime);
      }
      // Non data-store with client queue and listener
      doAfterClear(regionEvent); // TODO:KIRK: doAfterClear
    }
    return emptySet();
  }

  private Set<Integer> clearLocalBucketsUnderLock(InternalCacheEvent regionEvent,
      CachePerfStats stats, long startTime) {
    boolean incStats = stats != null;
    partitionedRegion.getDataStore().lockBucketCreationForRegionClear();
    try {
      Set<Integer> clearedBuckets = doClearRegion(regionEvent);
      incStats = incStats && !clearedBuckets.isEmpty();
      doAfterClear(regionEvent); // TODO:KIRK: doAfterClear
      return clearedBuckets;
    } finally {
      partitionedRegion.getDataStore().unlockBucketCreationForRegionClear();
      if (incStats) {
        partitionedRegion.getRegionCachePerfStats().incRegionClearCount();
        partitionedRegion.getRegionCachePerfStats()
            .incPartitionedRegionClearLocalDuration(System.nanoTime() - startTime);
      }
    }
  }

  private Set<Integer> doClearRegion(InternalCacheEvent regionEvent) {
    Set<Integer> clearedBuckets = new HashSet<>();
    RetryTimeKeeper retryTimeKeeper = new RetryTimeKeeper(retryTime.toMillis());
    boolean retry;
    do {
      waitForPrimary(retryTimeKeeper);
      attemptToClearAllBuckets(regionEvent, clearedBuckets);
      retry = getAndClearMembershipChange();
      retryTimeKeeper.reset();
    } while (retry);
    return clearedBuckets;
  }

  private void attemptToClearAllBuckets(InternalCacheEvent regionEvent,
      Collection<Integer> clearedBuckets) {
    for (BucketRegion localPrimaryBucketRegion : partitionedRegion.getDataStore()
        .getAllLocalPrimaryBucketRegions()) {
      if (isNotEmpty(localPrimaryBucketRegion)) {
        RegionEventImpl bucketRegionEvent =
            new RegionEventImpl(localPrimaryBucketRegion, Operation.REGION_CLEAR, null,
                false, partitionedRegion.getMyId(), regionEvent.getEventId());
        localPrimaryBucketRegion.cmnClearRegion(bucketRegionEvent, false, true);
      }
      clearedBuckets.add(localPrimaryBucketRegion.getId());
    }
  }

  @VisibleForTesting
  boolean getAndClearMembershipChange() {
    if (partitionedRegionClearListener.getMembershipChange()) {
      // Retry and reset the membership change status.
      partitionedRegionClearListener.setMembershipChange(false);
      return true;
    }
    return false;
  }

  @VisibleForTesting
  void doAfterClear(InternalCacheEvent regionEvent) {
    if (partitionedRegion.hasAnyClientsInterested()) {// TODO:KIRK: doAfterClear: remove conditional
      notifyClients(regionEvent);
    }

    if (partitionedRegion.hasListener()) { // TODO:KIRK: doAfterClear: remove conditional
      partitionedRegion.dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_CLEAR, regionEvent);
    }
  }

  /**
   * obtain locks for all local buckets
   */
  @VisibleForTesting
  void lockLocalPrimaryBucketsUnderLock(InternalDistributedMember requester) {
    synchronized (lockRequester) {
      // Check if the member is still part of the distributed system
      if (!partitionedRegion.getDistributionManager().isCurrentMember(requester)) {
        return;
      }

      lockRequester.set(requester);
      if (partitionedRegion.getDataStore() != null) {
        lockLocalPrimaryBuckets();
      }
    }
  }

  private void lockLocalPrimaryBuckets() {
    for (BucketRegion localPrimaryBucketRegion : partitionedRegion.getDataStore()
        .getAllLocalPrimaryBucketRegions()) {
      try {
        localPrimaryBucketRegion.lockLocallyForClear(partitionedRegion.getDistributionManager(),
            partitionedRegion.getMyId(), null);
      } catch (Exception ex) {
        partitionedRegion.checkClosed();
      }
    }
  }

  @VisibleForTesting
  void unlockLocalPrimaryBucketsUnderLock() {
    synchronized (lockRequester) {
      if (lockRequester.get() == null) {
        // The member has left.
        return;
      }
      try {
        if (partitionedRegion.getDataStore() != null) {
          unlockLocalPrimaryBuckets();
        }
      } finally {
        lockRequester.set(null);
      }
    }
  }

  private void unlockLocalPrimaryBuckets() {
    for (BucketRegion localPrimaryBucketRegion : partitionedRegion.getDataStore()
        .getAllLocalPrimaryBucketRegions()) {
      try {
        localPrimaryBucketRegion.releaseLockLocallyForClear(null);
      } catch (Exception ex) {
        logger.debug(
            "Unable to acquire clear lock for bucket region " + localPrimaryBucketRegion
                .getName(),
            ex.getMessage());
        partitionedRegion.checkClosed();
      }
    }
  }

  @VisibleForTesting
  Set<Integer> sendPartitionedRegionClearMessage(InternalCacheEvent event, OperationType op) {
    RegionEventImpl eventForLocalClear = (RegionEventImpl) cast(event).clone();
    eventForLocalClear.setOperation(Operation.REGION_LOCAL_CLEAR);
    do {
      try {
        return attemptToSendPartitionedRegionClearMessage(event, op);
      } catch (ForceReattemptException ignored) {
        // retry
      }
    } while (true);
  }

  /**
   * @return buckets that are cleared. empty set if any exception happened
   */
  @VisibleForTesting
  Set<Integer> attemptToSendPartitionedRegionClearMessage(InternalCacheEvent event,
      OperationType op)
      throws ForceReattemptException {
    if (partitionedRegion.getPRRoot() == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Partition region {} failed to initialize. Remove its profile from remote members.",
            partitionedRegion);
      }
      updateAttributesProcessorFactory
          .create(partitionedRegion)
          .distribute(false);
      return emptySet();
    }

    return sendClearMessage(event, op, getConfigRecipients());
  }

  private Set<InternalDistributedMember> getConfigRecipients() {
    final Set<InternalDistributedMember> configRecipients =
        new HashSet<>(partitionedRegion.getRegionAdvisor().adviseAllPRNodes());
    try {
      final PartitionRegionConfig config =
          partitionedRegion.getPRRoot().get(partitionedRegion.getRegionIdentifier());

      if (config != null) {
        for (Node node : config.getNodes()) {
          InternalDistributedMember memberId = node.getMemberId();
          if (!memberId.equals(partitionedRegion.getMyId())) {
            configRecipients.add(memberId);
          }
        }
      }
    } catch (CancelException ignored) {
      // ignore
    }
    return configRecipients;
  }

  private Set<Integer> sendClearMessage(InternalCacheEvent event, OperationType op,
      Set<InternalDistributedMember> recipients)
      throws ForceReattemptException {
    try {
      PartitionedRegionClearResponse clearResponse =
          new PartitionedRegionClearResponse(partitionedRegion.getSystem(), recipients);
      PartitionedRegionClearMessage clearMessage =
          new PartitionedRegionClearMessage(recipients, partitionedRegion, clearResponse, op,
              (RegionEventImpl) event);

      clearMessage.send();

      clearResponse.waitForRepliesUninterruptibly();
      return clearResponse.getBucketsCleared();
    } catch (ReplyException e) {
      handleReplyException(e);
    }
    return emptySet();
  }

  private void handleReplyException(ReplyException e) throws ForceReattemptException {
    Throwable cause = e.getCause();
    if (cause instanceof ForceReattemptException) {
      throw (ForceReattemptException) cause;
    }
    if (cause instanceof PartitionedRegionPartialClearException) {
      throw (PartitionedRegionPartialClearException) cause;
    }
    logger.warn(
        "Distribution of partitioned region clear did not complete without exception",
        e);
  }

  @VisibleForTesting
  void doClear(RegionEventImpl regionEvent, boolean cacheWrite) {
    requireMemberVersionsSupportPartitionRegionClear();

    // distributed lock to make sure only one clear op is in progress in the cluster.
    String lockName = CLEAR_OPERATION + partitionedRegion.getName();
    acquireDistributedClearLock(lockName);
    long clearStartTime = 0;
    CachePerfStats stats = partitionedRegion.getRegionCachePerfStats();
    if (stats != null) {
      clearStartTime = System.nanoTime();
    }
    try {
      // Force all primary buckets to be created before clear.
      assignAllPrimaryBuckets();

      requireAsyncEventQueuesSupportPartitionedRegionClear();

      if (cacheWrite) {
        invokeCacheWriter(regionEvent);
      }

      doClearUnderLock(regionEvent);
    } finally {
      releaseDistributedClearLock(lockName);
      if (stats != null) {
        partitionedRegion.getRegionCachePerfStats()
            .incPartitionedRegionClearTotalDuration(System.nanoTime() - clearStartTime);
      }
    }
  }

  private void requireAsyncEventQueuesSupportPartitionedRegionClear() {
    for (AsyncEventQueue asyncEventQueue : partitionedRegion.getCache()
        .getAsyncEventQueues(false)) {
      InternalAsyncEventQueue internalAsyncEventQueue = (InternalAsyncEventQueue) asyncEventQueue;
      if (internalAsyncEventQueue.isPartitionedRegionClearUnsupported()) {
        throw new UnsupportedOperationException(String.format(
            "Clear is not supported on region %s because it has a lucene index",
            partitionedRegion.getFullPath()));
      }
    }
  }

  private void doClearUnderLock(InternalCacheEvent event) {
    // clear write locks need to be taken on all local and remote primary buckets
    // whether or not the partitioned region has any listeners clients interested
    lockLocalPrimaryBucketsUnderLock(partitionedRegion.getDistributionManager().getId());
    try {
      sendPartitionedRegionClearMessage(event, OperationType.OP_LOCK_FOR_PR_CLEAR);
      Set<Integer> bucketsCleared = clearRegion(event);
      requireAllBucketsWereCleared(bucketsCleared);
    } finally {
      unlockLocalPrimaryBucketsUnderLock();
      sendPartitionedRegionClearMessage(event, OperationType.OP_UNLOCK_FOR_PR_CLEAR);
    }
  }

  private void requireAllBucketsWereCleared(Set<Integer> bucketsCleared) {
    if (partitionedRegion.getTotalNumberOfBuckets() != bucketsCleared.size()) {
      String message = "Unable to clear all the buckets from the partitioned region "
          + partitionedRegion.getName()
          + ", either data (buckets) moved or member departed.";

      logger.warn(message + " expected to clear number of buckets: "
          + partitionedRegion.getTotalNumberOfBuckets() +
          " actual cleared: " + bucketsCleared.size());

      throw new PartitionedRegionPartialClearException(message);
    }
  }

  @VisibleForTesting
  void invokeCacheWriter(RegionEventImpl regionEvent) {
    try {
      partitionedRegion.cacheWriteBeforeRegionClear(regionEvent);
    } catch (OperationAbortedException operationAbortedException) {
      throw new CacheWriterException(operationAbortedException);
    }
  }

  @VisibleForTesting
  void assignAllPrimaryBuckets() {
    PartitionedRegion leader = colocationLeaderRegionProvider.getLeaderRegion(partitionedRegion);
    assignBucketsToPartitions.assignBucketsToPartitions(leader);
  }

  @VisibleForTesting
  void handleClearFromDepartedMember(InternalDistributedMember departedMember) {
    synchronized (lockRequester) {
      if (departedMember.equals(lockRequester.get())) {
        unlockLocalPrimaryBucketsUnderLock();
      }
    }
  }

  /**
   * Registering listener publishes 'this' so it must be done AFTER and OUTSIDE of construction.
   */
  private void registerListener() {
    partitionedRegion.getDistributionManager()
        .addMembershipListener(partitionedRegionClearListener);
  }

  private void notifyClients(InternalCacheEvent event) {
    // Set client routing information into the event
    // The clear operation in case of PR is distributed differently
    // hence the FilterRoutingInfo is set here instead of
    // DistributedCacheOperation.distribute().
    event.setEventType(EnumListenerEvent.AFTER_REGION_CLEAR);
    if (!isUsedForInternal(partitionedRegion)) {
      FilterRoutingInfo filterRoutingInfoPart1 =
          partitionedRegion.getFilterProfile().getFilterRoutingInfoPart1(event,
              FilterProfile.NO_PROFILES, emptySet());

      FilterRoutingInfo filterRoutingInfoPart2 =
          partitionedRegion.getFilterProfile().getFilterRoutingInfoPart2(filterRoutingInfoPart1,
              event);

      if (filterRoutingInfoPart2 != null) {
        event.setLocalFilterInfo(filterRoutingInfoPart2.getLocalFilterInfo());
      }
    }
    partitionedRegion.notifyBridgeClients(event);
  }

  /**
   * Throws UnsupportedOperationException if any server versions do not support partitioned region
   * clear.
   */
  private void requireMemberVersionsSupportPartitionRegionClear() {
    Collection<String> memberNames = checkForOlderMemberVersions();
    if (!memberNames.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format("Server(s) %s version was too old (< %s) for partitioned region clear",
              memberNames, KnownVersion.GEODE_1_14_0));
    }
  }

  private Collection<String> checkForOlderMemberVersions() {
    Collection<String> memberNames = new ArrayList<>();
    for (int i = 0; i < partitionedRegion.getTotalNumberOfBuckets(); i++) {
      InternalDistributedMember internalDistributedMember = partitionedRegion.getBucketPrimary(i);
      if (internalDistributedMember == null) {
        continue;
      }

      if (internalDistributedMember.getVersion().isOlderThan(KnownVersion.GEODE_1_14_0)) {
        if (!memberNames.contains(internalDistributedMember.getName())) {
          memberNames.add(internalDistributedMember.getName());
        }
      }
    }
    return memberNames;
  }

  @VisibleForTesting
  PartitionedRegionClearListener getPartitionedRegionClearListenerForTesting() {
    return partitionedRegionClearListener;
  }

  @VisibleForTesting
  boolean isLockedForListenerAndClientNotificationForTesting() {
    return lockRequester.get() != null;
  }

  @VisibleForTesting
  void setLockedForTesting(InternalDistributedMember member) {
    lockRequester.set(member);
  }

  @VisibleForTesting
  InternalDistributedMember getLockRequesterForTesting() {
    return lockRequester.get();
  }

  private static RegionEventImpl cast(InternalCacheEvent event) {
    return (RegionEventImpl) event;
  }

  private static boolean isUsedForInternal(PartitionedRegion partitionedRegion) {
    return partitionedRegion.isUsedForMetaRegion() ||
        partitionedRegion.isUsedForPartitionedRegionAdmin() ||
        partitionedRegion.isUsedForPartitionedRegionBucket() ||
        partitionedRegion.isUsedForParallelGatewaySenderQueue();
  }

  @SuppressWarnings({"SizeReplaceableByIsEmpty", "TypeMayBeWeakened"})
  private static boolean isNotEmpty(BucketRegion region) {
    return region.size() > 0;
  }

  @VisibleForTesting
  class PartitionedRegionClearListener implements MembershipListener {

    private final AtomicBoolean membershipChange = new AtomicBoolean();

    @Override
    public synchronized void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      setMembershipChange(true);
      handleClearFromDepartedMember(id);
    }

    private void setMembershipChange(boolean newValue) {
      membershipChange.set(newValue);
    }

    private boolean getMembershipChange() {
      return membershipChange.get();
    }
  }

  @FunctionalInterface
  @VisibleForTesting
  interface ColocationLeaderRegionProvider {
    PartitionedRegion getLeaderRegion(PartitionedRegion partitionedRegion);
  }

  @FunctionalInterface
  @VisibleForTesting
  interface AssignBucketsToPartitions {
    void assignBucketsToPartitions(PartitionedRegion partitionedRegion);
  }

  @FunctionalInterface
  @VisibleForTesting
  interface UpdateAttributesProcessorFactory {
    UpdateAttributesProcessor create(PartitionedRegion partitionedRegion);
  }
}
