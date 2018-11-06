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
package org.apache.geode.internal.cache.persistence;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

public class PersistenceInitialImageAdvisor {

  private static final Logger logger = LogService.getLogger();

  private final InternalPersistenceAdvisor persistenceAdvisor;
  private final String shortDiskStoreID;
  private final String regionPath;
  private final CacheDistributionAdvisor cacheDistributionAdvisor;
  private final boolean hasDiskImageToRecoverFrom;

  public PersistenceInitialImageAdvisor(InternalPersistenceAdvisor persistenceAdvisor,
      String shortDiskStoreID, String regionPath, CacheDistributionAdvisor cacheDistributionAdvisor,
      boolean hasDiskImageToRecoverFrom) {
    this.persistenceAdvisor = persistenceAdvisor;
    this.shortDiskStoreID = shortDiskStoreID;
    this.regionPath = regionPath;
    this.cacheDistributionAdvisor = cacheDistributionAdvisor;
    this.hasDiskImageToRecoverFrom = hasDiskImageToRecoverFrom;
  }

  public InitialImageAdvice getAdvice(InitialImageAdvice previousAdvice) {
    final boolean isPersistAdvisorDebugEnabled =
        logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE);

    MembershipChangeListener listener = new MembershipChangeListener(persistenceAdvisor);
    cacheDistributionAdvisor.addMembershipAndProxyListener(listener);
    persistenceAdvisor.addListener(listener);
    try {
      while (true) {
        cacheDistributionAdvisor.getAdvisee().getCancelCriterion().checkCancelInProgress(null);
        try {
          // On first pass, previous advice is null. On subsequent passes, it's the advice
          // from the previous iteration.
          InitialImageAdvice advice =
              cacheDistributionAdvisor.adviseInitialImage(previousAdvice, true);

          if (hasReplicates(advice)) {
            if (hasDiskImageToRecoverFrom) {
              removeReplicatesIfWeAreEqualToAnyOrElseClearEqualMembers(advice.getReplicates());
            }
            return advice;
          } else if (hasNonPersistentMember(advice)) {
            updateMembershipViewFromAnyPeer(advice.getNonPersistent(), hasDiskImageToRecoverFrom);
          }

          // Fix for 51698 - If there are online members that we previously failed to get a GII
          // from, retry those members rather than wait for new persistent members to recover.
          if (hasReplicates(previousAdvice)) {
            logger.info(
                "GII failed from all sources, but members are still online. Retrying the GII.");
            previousAdvice = null;
            continue;
          }

          Set<PersistentMemberID> previouslyOnlineMembers =
              persistenceAdvisor.getPersistedOnlineOrEqualMembers();

          // If there are no currently online members, and no previously online members, this member
          // should just go with what's on its own disk
          if (previouslyOnlineMembers.isEmpty()) {
            if (isPersistAdvisorDebugEnabled()) {
              logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
                  "{}-{}: No previously online members. Recovering with the data from the local disk",
                  shortDiskStoreID, regionPath);
            }
            return advice;
          }

          Set<PersistentMemberID> offlineMembers = new HashSet<>();
          Set<PersistentMemberID> membersToWaitFor =
              persistenceAdvisor.getMembersToWaitFor(previouslyOnlineMembers, offlineMembers);

          if (membersToWaitFor.isEmpty()) {
            if (isPersistAdvisorDebugEnabled()) {
              logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
                  "{}-{}: All of the previously online members are now online and waiting for us. Acquiring tie lock. Previously online members {}",
                  shortDiskStoreID, regionPath, advice.getReplicates());
            }
            if (persistenceAdvisor.acquireTieLock()) {
              return refreshInitialImageAdviceAndThenCheckMyStateWithReplicates(previousAdvice);
            }
          } else {
            if (isPersistAdvisorDebugEnabled) {
              logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
                  "{}-{}: Going to wait for these member ids: {}", shortDiskStoreID, regionPath,
                  membersToWaitFor);
            }
          }

          waitForMembershipChangeForMissingDiskStores(listener, offlineMembers, membersToWaitFor);
        } catch (InterruptedException e) {
          logger.debug("Interrupted while trying to determine latest persisted copy", e);
        }
      }
    } finally {
      persistenceAdvisor.setWaitingOnMembers(null, null);
      cacheDistributionAdvisor.removeMembershipAndProxyListener(listener);
      persistenceAdvisor.removeListener(listener);
    }
  }

  private void updateMembershipViewFromAnyPeer(Set<InternalDistributedMember> peers,
      boolean recoverFromDisk) {
    for (InternalDistributedMember peer : peers) {
      try {
        persistenceAdvisor.updateMembershipView(peer, recoverFromDisk);
        return;
      } catch (ReplyException e) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE, "Failed to update membership view", e);
        }
      }
    }
  }

  private InitialImageAdvice refreshInitialImageAdviceAndThenCheckMyStateWithReplicates(
      InitialImageAdvice previousAdvice) {
    if (isPersistAdvisorDebugEnabled()) {
      logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
          "{}-{}: Acquired the lock. This member will initialize", shortDiskStoreID, regionPath);
    }
    InitialImageAdvice advice = cacheDistributionAdvisor.adviseInitialImage(previousAdvice, true);
    if (hasReplicates(advice)) {
      if (isPersistAdvisorDebugEnabled()) {
        logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
            "{}-{}: Another member has initialized while we were getting the lock. We will initialize from that member",
            shortDiskStoreID, regionPath);
      }
      persistenceAdvisor.checkMyStateOnMembers(advice.getReplicates());
    }
    return advice;
  }

  private boolean hasNonPersistentMember(InitialImageAdvice advice) {
    return !advice.getNonPersistent().isEmpty();
  }

  /**
   * if one or more replicates are equal to this member: remove replicates from advice, return
   * advice for GII loop
   */
  private void removeReplicatesIfWeAreEqualToAnyOrElseClearEqualMembers(
      Set<InternalDistributedMember> replicates) {
    if (isPersistAdvisorDebugEnabled()) {
      logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
          "{}-{}: There are members currently online. Checking for our state on those members and then initializing",
          shortDiskStoreID, regionPath);
    }
    // Check with these members to make sure that they have heard of us. If any of them
    // say we have the same data on disk, we don't need to do a GII.
    boolean weAreEqualToAReplicate = persistenceAdvisor.checkMyStateOnMembers(replicates);
    if (weAreEqualToAReplicate) {
      // prevent GII by removing all replicates
      removeReplicates(replicates);
    } else {
      persistenceAdvisor.clearEqualMembers();
    }
    // Either a replicate has said we're equal and we've cleared replicates,
    // or none of them said we're equal and we've cleared our equal members.
    // We had replicates. Now one of these things is true:
    // - We've cleared replicates (meaning we're equal to one, so can load from disk)
    // - No replicates report we're equal (so must GII from one, which we indicate by clearing equal
    // members).
  }

  private boolean hasReplicates(InitialImageAdvice advice) {
    return advice != null && !advice.getReplicates().isEmpty();
  }

  private void removeReplicates(Set<InternalDistributedMember> replicates) {
    if (isPersistAdvisorDebugEnabled()) {
      logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
          "{}-{}: We have the same data on disk as one of {} recovering gracefully",
          shortDiskStoreID, regionPath, replicates);
    }
    replicates.clear();
  }

  private void waitForMembershipChangeForMissingDiskStores(MembershipChangeListener listener,
      Set<PersistentMemberID> offlineMembers, Set<PersistentMemberID> membersToWaitFor)
      throws InterruptedException {
    persistenceAdvisor.beginWaitingForMembershipChange(membersToWaitFor);
    try {
      // The persistence advisor needs to know which members are really not available because the
      // user uses this information to decide which members they haven't started yet.
      // membersToWaitFor includes members that are still waiting to start up, but are waiting for
      // members other than the current member. So we pass the set of offline members here.

      persistenceAdvisor.setWaitingOnMembers(membersToWaitFor, offlineMembers);
      listener.waitForChange();
    } finally {
      persistenceAdvisor.endWaitingForMembershipChange();
    }
  }

  private boolean isPersistAdvisorDebugEnabled() {
    return logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE);
  }
}
