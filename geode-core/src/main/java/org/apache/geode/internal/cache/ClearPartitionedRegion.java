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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ClearPartitionedRegion {

  private static final Logger logger = LogService.getLogger();

  private final PartitionedRegion partitionedRegion;

  private LockForListenerAndClientNotification lockForListenerAndClientNotification =
      new LockForListenerAndClientNotification();

  public ClearPartitionedRegion(PartitionedRegion partitionedRegion) {
    this.partitionedRegion = partitionedRegion;
    partitionedRegion.getDistributionManager()
        .addMembershipListener(new ClearPartitionedRegionListener());
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
      logger.warn("Caught exception while unlocking clear distributed lock", ex.getMessage());
    }
  }

  void obtainWriteLockForClear(RegionEventImpl event) {
    sendLocalClearRegionMessage(event,
        ClearPartitionedRegionMessage.OperationType.OP_LOCK_FOR_CLEAR);
    obtainClearLockLocal(partitionedRegion.getDistributionManager().getId());
  }

  void releaseWriteLockForClear(RegionEventImpl event) {
    sendLocalClearRegionMessage(event,
        ClearPartitionedRegionMessage.OperationType.OP_UNLOCK_FOR_CLEAR);
    releaseClearLockLocal();
  }

  void clearRegion(RegionEventImpl regionEvent, boolean cacheWrite,
      RegionVersionVector vector) {
    clearRegionLocal(regionEvent, cacheWrite, null);
    sendLocalClearRegionMessage(regionEvent,
        ClearPartitionedRegionMessage.OperationType.OP_CLEAR);
  }

  public void clearRegionLocal(RegionEventImpl regionEvent, boolean cacheWrite,
      RegionVersionVector vector) {
    // Synchronized to handle the requester departure.
    synchronized (lockForListenerAndClientNotification) {
      if (partitionedRegion.getDataStore() != null) {
        partitionedRegion.getDataStore().lockBucketCreationForRegionClear();
        try {
          for (BucketRegion localPrimaryBucketRegion : partitionedRegion.getDataStore()
              .getAllLocalPrimaryBucketRegions()) {
            localPrimaryBucketRegion.clear();
          }
          doAfterClear(regionEvent);
        } finally {
          partitionedRegion.getDataStore().unLockBucketCreationForRegionClear();
        }
      } else {
        // Non data-store with client queue and listener
        doAfterClear(regionEvent);
      }
    }
  }

  private void doAfterClear(RegionEventImpl regionEvent) {
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

  protected void obtainClearLockLocal(InternalDistributedMember requester) {
    synchronized (lockForListenerAndClientNotification) {
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
        // The member has been left.
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

  private void sendLocalClearRegionMessage(RegionEventImpl event,
      ClearPartitionedRegionMessage.OperationType op) {
    RegionEventImpl eventForLocalClear = (RegionEventImpl) event.clone();
    eventForLocalClear.setOperation(Operation.REGION_LOCAL_CLEAR);
    sendClearRegionMessage(event, op);
  }

  private void sendClearRegionMessage(RegionEventImpl event,
      ClearPartitionedRegionMessage.OperationType op) {
    boolean retry = true;
    while (retry) {
      retry = attemptToSendClearRegionMessage(event, op);
    }
  }

  private boolean attemptToSendClearRegionMessage(RegionEventImpl event,
      ClearPartitionedRegionMessage.OperationType op) {
    if (partitionedRegion.getPRRoot() == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Partition region {} failed to initialize. Remove its profile from remote members.",
            this);
      }
      new UpdateAttributesProcessor(partitionedRegion, true).distribute(false);
      return false;
    }
    final HashSet configRecipients =
        new HashSet(partitionedRegion.getRegionAdvisor().adviseAllPRNodes());

    // It's possible this instance has not been initialized
    // or hasn't gotten through initialize() far enough to have
    // sent a CreateRegionProcessor message, bug 36048
    try {
      final PartitionRegionConfig prConfig =
          partitionedRegion.getPRRoot().get(partitionedRegion.getRegionIdentifier());

      if (prConfig != null) {
        // Fix for bug#34621 by Tushar
        Iterator itr = prConfig.getNodes().iterator();
        while (itr.hasNext()) {
          InternalDistributedMember idm = ((Node) itr.next()).getMemberId();
          if (!idm.equals(partitionedRegion.getMyId())) {
            configRecipients.add(idm);
          }
        }
      }
    } catch (CancelException ignore) {
      // ignore
    }

    try {
      ClearPartitionedRegionMessage.ClearPartitionedRegionResponse resp =
          new ClearPartitionedRegionMessage.ClearPartitionedRegionResponse(
              partitionedRegion.getSystem(),
              configRecipients);
      ClearPartitionedRegionMessage clearPartitionedRegionMessage =
          new ClearPartitionedRegionMessage(configRecipients, partitionedRegion, resp, op, event);
      clearPartitionedRegionMessage.send();
      resp.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      if (e.getRootCause() instanceof ForceReattemptException) {
        return true;
      }
      logger.warn(
          "PartitionedRegion#sendClearRegionMessage: Caught exception during ClearRegionMessage send and waiting for response",
          e);
    }
    return false;
  }

  void doClear(RegionEventImpl regionEvent, boolean cacheWrite,
      PartitionedRegion partitionedRegion) {
    String lockName = "_clearOperation" + partitionedRegion.getDisplayName();

    // Force all primary buckets to be created before clear.
    PartitionRegionHelper.assignBucketsToPartitions(partitionedRegion);

    try {
      // distributed lock to make sure only one clear op is in progress in the cluster.
      acquireDistributedClearLock(lockName);

      // do cacheWrite
      partitionedRegion.cacheWriteBeforeRegionClear(regionEvent);

      // Check if there are any listeners or clients interested. If so, then clear write
      // locks needs to be taken on all local and remote primary buckets in order to
      // preserve the ordering of client events (for concurrent operations on the region).
      boolean acquireClearLockForClientNotification =
          (partitionedRegion.hasAnyClientsInterested() && partitionedRegion.hasListener());
      if (acquireClearLockForClientNotification) {
        obtainWriteLockForClear(regionEvent);
      }
      try {
        clearRegion(regionEvent, cacheWrite, null);
      } finally {
        if (acquireClearLockForClientNotification) {
          releaseWriteLockForClear(regionEvent);
        }
      }

    } finally {
      releaseDistributedClearLock(lockName);
    }
  }

  void handleClearFromDepartedMember(InternalDistributedMember departedMember) {
    if (departedMember.equals(lockForListenerAndClientNotification.getLockRequester())) {
      synchronized (lockForListenerAndClientNotification) {
        if (lockForListenerAndClientNotification.getLockRequester() != null) {
          releaseClearLockLocal();
        }
      }
    }
  }

  class LockForListenerAndClientNotification {

    private volatile boolean locked = false;

    private volatile InternalDistributedMember lockRequester;

    private void setLocked(InternalDistributedMember member) {
      locked = true;
      lockRequester = member;
    }

    private void setUnLocked() {
      locked = false;
      lockRequester = null;
    }

    boolean isLocked() {
      return locked;
    }

    InternalDistributedMember getLockRequester() {
      return lockRequester;
    }
  }

  protected class ClearPartitionedRegionListener implements MembershipListener {

    @Override
    public synchronized void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      handleClearFromDepartedMember(id);
    }
  }
}
