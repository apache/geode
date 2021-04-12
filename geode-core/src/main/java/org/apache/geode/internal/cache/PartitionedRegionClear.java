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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.OperationAbortedException;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class PartitionedRegionClear {

  private static final Logger logger = LogService.getLogger();

  protected static final String CLEAR_OPERATION = "_clearOperation";

  private final int retryTime = 2 * 60 * 1000;

  private final PartitionedRegion partitionedRegion;

  protected final LockForListenerAndClientNotification lockForListenerAndClientNotification =
      new LockForListenerAndClientNotification();

  private volatile boolean membershipChange = false;

  protected final PartitionedRegionClearListener partitionedRegionClearListener =
      new PartitionedRegionClearListener();

  public PartitionedRegionClear(PartitionedRegion partitionedRegion) {
    this.partitionedRegion = partitionedRegion;
    partitionedRegion.getDistributionManager()
        .addMembershipListener(partitionedRegionClearListener);
  }

  public boolean isLockedForListenerAndClientNotification() {
    return lockForListenerAndClientNotification.isLocked();
  }

  void acquireDistributedClearLock(String clearLock) {
    try {
      partitionedRegion.getPartitionedRegionLockService().lock(clearLock, -1, -1);
    } catch (IllegalStateException e) {
      partitionedRegion.lockCheckReadiness();
      throw e;
    }
  }

  void releaseDistributedClearLock(String clearLock) {
    try {
      partitionedRegion.getPartitionedRegionLockService().unlock(clearLock);
    } catch (IllegalStateException e) {
      partitionedRegion.lockCheckReadiness();
    } catch (Exception ex) {
      logger.warn("Caught exception while unlocking clear distributed lock. " + ex.getMessage());
    }
  }

  protected PartitionedRegionClearListener getPartitionedRegionClearListener() {
    return partitionedRegionClearListener;
  }

  /**
   * only called if there are any listeners or clients interested.
   */
  void obtainLockForClear(RegionEventImpl event) {
    obtainClearLockLocal(partitionedRegion.getDistributionManager().getId());
    sendPartitionedRegionClearMessage(event,
        PartitionedRegionClearMessage.OperationType.OP_LOCK_FOR_PR_CLEAR);
  }

  /**
   * only called if there are any listeners or clients interested.
   */
  void releaseLockForClear(RegionEventImpl event) {
    releaseClearLockLocal();
    sendPartitionedRegionClearMessage(event,
        PartitionedRegionClearMessage.OperationType.OP_UNLOCK_FOR_PR_CLEAR);
  }

  /**
   * clears local primaries and send message to remote primaries to clear
   */
  Set<Integer> clearRegion(RegionEventImpl regionEvent) {
    // this includes all local primary buckets and their remote secondaries
    Set<Integer> localPrimaryBuckets = clearRegionLocal(regionEvent);
    // this includes all remote primary buckets and their secondaries
    Set<Integer> remotePrimaryBuckets = sendPartitionedRegionClearMessage(regionEvent,
        PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR);

    Set<Integer> allBucketsCleared = new HashSet<>();
    allBucketsCleared.addAll(localPrimaryBuckets);
    allBucketsCleared.addAll(remotePrimaryBuckets);
    return allBucketsCleared;
  }

  protected void waitForPrimary(PartitionedRegion.RetryTimeKeeper retryTimer) {
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
  public Set<Integer> clearRegionLocal(RegionEventImpl regionEvent) {
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
            waitForPrimary(new PartitionedRegion.RetryTimeKeeper(retryTime));
            RegionEventImpl bucketRegionEvent;
            for (BucketRegion localPrimaryBucketRegion : partitionedRegion.getDataStore()
                .getAllLocalPrimaryBucketRegions()) {
              if (localPrimaryBucketRegion.size() > 0) {
                bucketRegionEvent =
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
          if (clearedBuckets.size() != 0 && partitionedRegion.getCachePerfStats() != null) {
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

  protected void doAfterClear(RegionEventImpl regionEvent) {
    if (partitionedRegion.hasAnyClientsInterested()) {
      notifyClients(regionEvent);
    }

    if (partitionedRegion.hasListener()) {
      partitionedRegion.dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_CLEAR, regionEvent);
    }
  }

  void notifyClients(RegionEventImpl event) {
    // Set client routing information into the event
    // The clear operation in case of PR is distributed differently
    // hence the FilterRoutingInfo is set here instead of
    // DistributedCacheOperation.distribute().
    event.setEventType(EnumListenerEvent.AFTER_REGION_CLEAR);
    if (!partitionedRegion.isUsedForMetaRegion() && !partitionedRegion
        .isUsedForPartitionedRegionAdmin()
        && !partitionedRegion.isUsedForPartitionedRegionBucket() && !partitionedRegion
            .isUsedForParallelGatewaySenderQueue()) {

      FilterRoutingInfo localCqFrInfo =
          partitionedRegion.getFilterProfile().getFilterRoutingInfoPart1(event,
              FilterProfile.NO_PROFILES, Collections.emptySet());

      FilterRoutingInfo localCqInterestFrInfo =
          partitionedRegion.getFilterProfile().getFilterRoutingInfoPart2(localCqFrInfo, event);

      if (localCqInterestFrInfo != null) {
        event.setLocalFilterInfo(localCqInterestFrInfo.getLocalFilterInfo());
      }
    }
    partitionedRegion.notifyBridgeClients(event);
  }

  /**
   * obtain locks for all local buckets
   */
  protected void obtainClearLockLocal(InternalDistributedMember requester) {
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

  protected void releaseClearLockLocal() {
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

  protected Set<Integer> sendPartitionedRegionClearMessage(RegionEventImpl event,
      PartitionedRegionClearMessage.OperationType op) {
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
  protected Set<Integer> attemptToSendPartitionedRegionClearMessage(RegionEventImpl event,
      PartitionedRegionClearMessage.OperationType op)
      throws ForceReattemptException {
    Set<Integer> bucketsOperated = new HashSet<>();

    if (partitionedRegion.getPRRoot() == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Partition region {} failed to initialize. Remove its profile from remote members.",
            this.partitionedRegion);
      }
      new UpdateAttributesProcessor(partitionedRegion, true).distribute(false);
      return bucketsOperated;
    }

    final Set<InternalDistributedMember> configRecipients =
        new HashSet<>(partitionedRegion.getRegionAdvisor()
            .adviseAllPRNodes());

    try {
      final PartitionRegionConfig prConfig =
          partitionedRegion.getPRRoot().get(partitionedRegion.getRegionIdentifier());

      if (prConfig != null) {
        for (Node node : prConfig.getNodes()) {
          InternalDistributedMember idm = node.getMemberId();
          if (!idm.equals(partitionedRegion.getMyId())) {
            configRecipients.add(idm);
          }
        }
      }
    } catch (CancelException ignore) {
      // ignore
    }

    try {
      PartitionedRegionClearMessage.PartitionedRegionClearResponse resp =
          new PartitionedRegionClearMessage.PartitionedRegionClearResponse(
              partitionedRegion.getSystem(), configRecipients);
      PartitionedRegionClearMessage partitionedRegionClearMessage =
          new PartitionedRegionClearMessage(configRecipients, partitionedRegion, resp, op, event);
      partitionedRegionClearMessage.send();

      resp.waitForRepliesUninterruptibly();
      bucketsOperated = resp.bucketsCleared;

    } catch (ReplyException e) {
      Throwable t = e.getCause();
      if (t instanceof ForceReattemptException) {
        throw (ForceReattemptException) t;
      }
      if (t instanceof PartitionedRegionPartialClearException) {
        throw new PartitionedRegionPartialClearException(t.getMessage(), t);
      }
      logger.warn(
          "PartitionedRegionClear#sendPartitionedRegionClearMessage: Caught exception during ClearRegionMessage send and waiting for response",
          e);
    }
    return bucketsOperated;
  }

  /**
   * This method returns a boolean to indicate if all server versions support Partition Region clear
   */
  public void allServerVersionsSupportPartitionRegionClear() {
    List<String> memberNames = new ArrayList<>();
    for (int i = 0; i < partitionedRegion.getTotalNumberOfBuckets(); i++) {
      InternalDistributedMember internalDistributedMember = partitionedRegion.getBucketPrimary(i);
      if ((internalDistributedMember != null)
          && (internalDistributedMember.getVersion().isOlderThan(KnownVersion.GEODE_1_14_0))) {
        if (!memberNames.contains(internalDistributedMember.getName())) {
          memberNames.add(internalDistributedMember.getName());
        }
      }
    }
    if (!memberNames.isEmpty()) {
      throw new UnsupportedOperationException(
          "A server's " + memberNames + " version was too old (< "
              + KnownVersion.GEODE_1_14_0 + ") for : Partitioned Region Clear");

    }
  }


  void doClear(RegionEventImpl regionEvent, boolean cacheWrite) {
    String lockName = CLEAR_OPERATION + partitionedRegion.getName();
    long clearStartTime = 0;

    allServerVersionsSupportPartitionRegionClear();

    try {
      // distributed lock to make sure only one clear op is in progress in the cluster.
      acquireDistributedClearLock(lockName);
      clearStartTime = System.nanoTime();

      // Force all primary buckets to be created before clear.
      assignAllPrimaryBuckets();

      for (AsyncEventQueue asyncEventQueue : partitionedRegion.getCache()
          .getAsyncEventQueues(false)) {
        if (((AsyncEventQueueImpl) asyncEventQueue).isPartitionedRegionClearUnsupported()) {
          throw new UnsupportedOperationException(
              "PartitionedRegion clear is not supported on region "
                  + partitionedRegion.getFullPath() + " because it has a lucene index");
        }
      }

      // do cacheWrite
      if (cacheWrite) {
        invokeCacheWriter(regionEvent);
      }

      // Check if there are any listeners or clients interested. If so, then clear write
      // locks needs to be taken on all local and remote primary buckets in order to
      // preserve the ordering of client events (for concurrent operations on the region).
      boolean acquireClearLockForNotification =
          (partitionedRegion.hasAnyClientsInterested() || partitionedRegion.hasListener());
      if (acquireClearLockForNotification) {
        obtainLockForClear(regionEvent);
      }
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
        if (acquireClearLockForNotification) {
          releaseLockForClear(regionEvent);
        }
      }
    } finally {
      releaseDistributedClearLock(lockName);
      CachePerfStats stats = partitionedRegion.getRegionCachePerfStats();
      if (stats != null) {
        partitionedRegion.getRegionCachePerfStats()
            .incPartitionedRegionClearTotalDuration(System.nanoTime() - clearStartTime);
      }
    }
  }

  protected void invokeCacheWriter(RegionEventImpl regionEvent) {
    try {
      partitionedRegion.cacheWriteBeforeRegionClear(regionEvent);
    } catch (OperationAbortedException operationAbortedException) {
      throw new CacheWriterException(operationAbortedException);
    }
  }

  protected void assignAllPrimaryBuckets() {
    PartitionedRegion leader = ColocationHelper.getLeaderRegion(partitionedRegion);
    PartitionRegionHelper.assignBucketsToPartitions(leader);
  }

  protected void handleClearFromDepartedMember(InternalDistributedMember departedMember) {
    if (departedMember.equals(lockForListenerAndClientNotification.getLockRequester())) {
      synchronized (lockForListenerAndClientNotification) {
        if (lockForListenerAndClientNotification.getLockRequester() != null) {
          releaseClearLockLocal();
        }
      }
    }
  }

  class LockForListenerAndClientNotification {

    private boolean locked = false;

    private InternalDistributedMember lockRequester;

    synchronized void setLocked(InternalDistributedMember member) {
      locked = true;
      lockRequester = member;
    }

    synchronized void setUnLocked() {
      locked = false;
      lockRequester = null;
    }

    synchronized boolean isLocked() {
      return locked;
    }

    synchronized InternalDistributedMember getLockRequester() {
      return lockRequester;
    }
  }

  protected void setMembershipChange(boolean membershipChange) {
    this.membershipChange = membershipChange;
  }

  protected boolean getMembershipChange() {
    return membershipChange;
  }

  protected class PartitionedRegionClearListener implements MembershipListener {

    @Override
    public synchronized void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      setMembershipChange(true);
      handleClearFromDepartedMember(id);
    }
  }
}
