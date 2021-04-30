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

  protected static final String CLEAR_OPERATION = "_clearOperation";

  private final Duration retryTime = Duration.ofMinutes(2);

  private final PartitionedRegion partitionedRegion;

  private final DistributedLockService distributedLockService;

  private final LockForListenerAndClientNotification lockForListenerAndClientNotification =
      new LockForListenerAndClientNotification();

  private final PartitionedRegionClearListener partitionedRegionClearListener =
      new PartitionedRegionClearListener();

  private final ColocationLeaderRegionProvider colocationLeaderRegionProvider;
  private final AssignBucketsToPartitions assignBucketsToPartitions;
  private final UpdateAttributesProcessorFactory updateAttributesProcessorFactory;

  private final AtomicBoolean membershipChange = new AtomicBoolean();

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
            colocationLeaderRegionProvider, assignBucketsToPartitions,
            updateAttributesProcessorFactory);
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
    } catch (Exception ex) {
      logger.warn("Caught exception while unlocking clear distributed lock. " + ex.getMessage());
    }
  }

  /**
   * only called if there are any listeners or clients interested.
   */
  @VisibleForTesting
  void obtainLockForClear(RegionEventImpl event) {
    obtainClearLockLocal(partitionedRegion.getDistributionManager().getId());
    sendPartitionedRegionClearMessage(event, OperationType.OP_LOCK_FOR_PR_CLEAR);
  }

  /**
   * only called if there are any listeners or clients interested.
   */
  @VisibleForTesting
  void releaseLockForClear(RegionEventImpl event) {
    releaseClearLockLocal();
    sendPartitionedRegionClearMessage(event, OperationType.OP_UNLOCK_FOR_PR_CLEAR);
  }

  /**
   * clears local primaries and send message to remote primaries to clear
   */
  @VisibleForTesting
  Set<Integer> clearRegion(RegionEventImpl regionEvent) {
    // this includes all local primary buckets and their remote secondaries
    Set<Integer> localPrimaryBuckets = clearRegionLocal(regionEvent);
    // this includes all remote primary buckets and their secondaries
    Set<Integer> remotePrimaryBuckets = sendPartitionedRegionClearMessage(regionEvent,
        OperationType.OP_PR_CLEAR);

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
            throw new PartitionedRegionPartialClearException(
                "Unable to find primary bucket region during clear operation on "
                    + partitionedRegion.getName() + " region.");
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
  Set<Integer> clearRegionLocal(InternalCacheEvent regionEvent) {
    Set<Integer> clearedBuckets = new HashSet<>();
    long clearStartTime = System.nanoTime();
    setMembershipChange(false);
    // Synchronized to handle the requester departure.
    synchronized (lockForListenerAndClientNotification) {
      if (partitionedRegion.getDataStore() != null) {
        partitionedRegion.getDataStore().lockBucketCreationForRegionClear();
        try {
          boolean retry;
          do {
            waitForPrimary(new RetryTimeKeeper(retryTime.toMillis()));
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

            if (getMembershipChange()) {
              // Retry and reset the membership change status.
              setMembershipChange(false);
              retry = true;
            } else {
              retry = false;
            }

          } while (retry);
          doAfterClear(regionEvent);
        } finally {
          partitionedRegion.getDataStore().unlockBucketCreationForRegionClear();
          if (!clearedBuckets.isEmpty() && partitionedRegion.getCachePerfStats() != null) {
            partitionedRegion.getRegionCachePerfStats().incRegionClearCount();
            partitionedRegion.getRegionCachePerfStats()
                .incPartitionedRegionClearLocalDuration(System.nanoTime() - clearStartTime);
          }
        }
      } else {
        // Non data-store with client queue and listener
        doAfterClear(regionEvent);
      }
    }
    return clearedBuckets;
  }

  private boolean getAndSetMembershipChange(boolean value) {

  }

  @VisibleForTesting
  void doAfterClear(InternalCacheEvent regionEvent) {
    if (partitionedRegion.hasAnyClientsInterested()) {
      notifyClients(regionEvent);
    }

    if (partitionedRegion.hasListener()) {
      partitionedRegion.dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_CLEAR, regionEvent);
    }
  }

  /**
   * obtain locks for all local buckets
   */
  @VisibleForTesting
  void obtainClearLockLocal(InternalDistributedMember requester) {
    synchronized (lockForListenerAndClientNotification) {
      // Check if the member is still part of the distributed system
      if (!partitionedRegion.getDistributionManager().isCurrentMember(requester)) {
        return;
      }

      lockForListenerAndClientNotification.setLocked(requester);
      if (partitionedRegion.getDataStore() != null) {
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
    }
  }

  @VisibleForTesting
  void releaseClearLockLocal() {
    synchronized (lockForListenerAndClientNotification) {
      if (lockForListenerAndClientNotification.getLockRequester() == null) {
        // The member has left.
        return;
      }
      try {
        if (partitionedRegion.getDataStore() != null) {

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
      } finally {
        lockForListenerAndClientNotification.setUnLocked();
      }
    }
  }

  @VisibleForTesting
  Set<Integer> sendPartitionedRegionClearMessage(RegionEventImpl event, OperationType op) {
    RegionEventImpl eventForLocalClear = (RegionEventImpl) event.clone();
    eventForLocalClear.setOperation(Operation.REGION_LOCAL_CLEAR);

    do {
      try {
        return attemptToSendPartitionedRegionClearMessage(event, op);
      } catch (ForceReattemptException reattemptException) {
        // retry
      }
    } while (true);
  }

  /**
   * @return buckets that are cleared. empty set if any exception happened
   */
  @VisibleForTesting
  Set<Integer> attemptToSendPartitionedRegionClearMessage(RegionEventImpl event, OperationType op)
      throws ForceReattemptException {
    Set<Integer> clearedBuckets = new HashSet<>();

    if (partitionedRegion.getPRRoot() == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Partition region {} failed to initialize. Remove its profile from remote members.",
            partitionedRegion);
      }
      updateAttributesProcessorFactory
          .create(partitionedRegion)
          .distribute(false);
      return clearedBuckets;
    }

    final Set<InternalDistributedMember> configRecipients =
        new HashSet<>(partitionedRegion.getRegionAdvisor()
            .adviseAllPRNodes());

    try {
      final PartitionRegionConfig prConfig =
          partitionedRegion.getPRRoot().get(partitionedRegion.getRegionIdentifier());

      if (prConfig != null) {
        for (Node node : prConfig.getNodes()) {
          InternalDistributedMember memberId = node.getMemberId();
          if (!memberId.equals(partitionedRegion.getMyId())) {
            configRecipients.add(memberId);
          }
        }
      }
    } catch (CancelException ignore) {
      // ignore
    }

    try {
      PartitionedRegionClearResponse clearResponse =
          new PartitionedRegionClearResponse(partitionedRegion.getSystem(), configRecipients);
      PartitionedRegionClearMessage clearMessage =
          new PartitionedRegionClearMessage(configRecipients, partitionedRegion, clearResponse, op,
              event);
      clearMessage.send();

      clearResponse.waitForRepliesUninterruptibly();
      clearedBuckets = clearResponse.getBucketsCleared();

    } catch (ReplyException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ForceReattemptException) {
        throw (ForceReattemptException) cause;
      }
      if (cause instanceof PartitionedRegionPartialClearException) {
        throw (PartitionedRegionPartialClearException) cause;
      }
      logger.warn(
          "PartitionedRegionClear#sendPartitionedRegionClearMessage: Caught exception during ClearRegionMessage send and waiting for response",
          e);
    }
    return clearedBuckets;
  }

  @VisibleForTesting
  void doClear(RegionEventImpl regionEvent, boolean cacheWrite) {
    requireServerVersionsSupportPartitionRegionClear();

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

      for (AsyncEventQueue asyncEventQueue : partitionedRegion.getCache()
          .getAsyncEventQueues(false)) {
        InternalAsyncEventQueue internalAsyncEventQueue = (InternalAsyncEventQueue) asyncEventQueue;
        if (internalAsyncEventQueue.isPartitionedRegionClearUnsupported()) {
          throw new UnsupportedOperationException(String.format(
              "Clear is not supported on region %s because it has a lucene index",
              partitionedRegion.getFullPath()));
        }
      }

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

  private void doClearUnderLock(RegionEventImpl regionEvent) {
    // clear write locks need to be taken on all local and remote primary buckets
    // whether or not the partitioned region has any listeners clients interested
    obtainLockForClear(regionEvent);
    try {
      Set<Integer> bucketsCleared = clearRegion(regionEvent);

      if (partitionedRegion.getTotalNumberOfBuckets() != bucketsCleared.size()) {
        String message = "Unable to clear all the buckets from the partitioned region "
            + partitionedRegion.getName()
            + ", either data (buckets) moved or member departed.";

        logger.warn(message + " expected to clear number of buckets: "
            + partitionedRegion.getTotalNumberOfBuckets() +
            " actual cleared: " + bucketsCleared.size());

        throw new PartitionedRegionPartialClearException(message);
      }
    } finally {
      releaseLockForClear(regionEvent);
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
    // TODO: fix synchronization
    if (departedMember.equals(lockForListenerAndClientNotification.getLockRequester())) {
      synchronized (lockForListenerAndClientNotification) {
        if (lockForListenerAndClientNotification.getLockRequester() != null) {
          releaseClearLockLocal();
        }
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
  private void requireServerVersionsSupportPartitionRegionClear() {
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

    if (!memberNames.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format("Server(s) %s version was too old (< %s) for partitioned region clear",
              memberNames, KnownVersion.GEODE_1_14_0));
    }
  }

  private void setMembershipChange(boolean membershipChange) {
    this.membershipChange.set(membershipChange);
  }

  @VisibleForTesting
  PartitionedRegionClearListener getPartitionedRegionClearListenerForTesting() {
    return partitionedRegionClearListener;
  }

  @VisibleForTesting
  boolean isLockedForListenerAndClientNotificationForTesting() {
    return lockForListenerAndClientNotification.isLocked();
  }

  @VisibleForTesting
  boolean getMembershipChange() {
    return membershipChange.get();
  }

  @VisibleForTesting
  void setLockedForTesting(InternalDistributedMember member) {
    lockForListenerAndClientNotification.setLocked(member);
  }

  @VisibleForTesting
  InternalDistributedMember getLockRequesterForTesting() {
    return lockForListenerAndClientNotification.getLockRequester();
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

  private static class LockForListenerAndClientNotification {

    private boolean locked;

    private InternalDistributedMember lockRequester;

    private synchronized void setLocked(InternalDistributedMember member) {
      locked = true;
      lockRequester = member;
    }

    private synchronized void setUnLocked() {
      locked = false;
      lockRequester = null;
    }

    private synchronized boolean isLocked() {
      return locked;
    }

    private synchronized InternalDistributedMember getLockRequester() {
      return lockRequester;
    }
  }

  @VisibleForTesting
  class PartitionedRegionClearListener implements MembershipListener {

    @Override
    public synchronized void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      setMembershipChange(true);
      handleClearFromDepartedMember(id);
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
