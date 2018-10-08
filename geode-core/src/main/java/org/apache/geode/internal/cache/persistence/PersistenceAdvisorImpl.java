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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.TestingOnly;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.cache.persistence.RevokedPersistentDataException;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ProfileListener;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager.MemberRevocationListener;
import org.apache.geode.internal.cache.persistence.PersistentStateQueryMessage.PersistentStateQueryReplyProcessor;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.process.StartupStatus;
import org.apache.geode.internal.util.TransformUtils;

public class PersistenceAdvisorImpl implements InternalPersistenceAdvisor {

  private static final Logger logger = LogService.getLogger();
  private static final PersistenceAdvisorObserver DEFAULT_PERSISTENCE_ADVISOR_OBSERVER = s -> {
  };
  private static PersistenceAdvisorObserver persistenceAdvisorObserver =
      DEFAULT_PERSISTENCE_ADVISOR_OBSERVER;

  protected final Object lock;

  protected final CacheDistributionAdvisor cacheDistributionAdvisor;
  protected final String regionPath;
  protected final PersistentMemberView persistentMemberView;
  private final DiskRegionStats diskRegionStats;
  private final PersistentMemberManager persistentMemberManager;
  private final ProfileChangeListener profileChangeListener;

  private final Set<PersistentMemberID> recoveredMembers;
  private final Set<PersistentMemberID> removedMembers = new HashSet<>();
  private final Set<PersistentMemberID> equalMembers;
  private final DistributedLockService distributedLockService;

  private volatile boolean holdingTieLock;

  protected volatile boolean online;
  private volatile Set<PersistentStateListener> persistentStateListeners = Collections.emptySet();
  private volatile boolean initialized;
  private volatile boolean shouldUpdatePersistentView;
  protected volatile boolean isClosed;

  protected volatile Set<PersistentMemberID> allMembersWaitingFor;
  protected volatile Set<PersistentMemberID> offlineMembersWaitingFor;

  private final PersistentStateQueryMessageSenderFactory persistentStateQueryMessageSenderFactory;

  public PersistenceAdvisorImpl(CacheDistributionAdvisor cacheDistributionAdvisor,
      DistributedLockService distributedLockService, PersistentMemberView persistentMemberView,
      String regionPath, DiskRegionStats diskRegionStats,
      PersistentMemberManager persistentMemberManager) {
    this(cacheDistributionAdvisor, distributedLockService, persistentMemberView, regionPath,
        diskRegionStats, persistentMemberManager, new PersistentStateQueryMessageSenderFactory());
  }

  @TestingOnly
  PersistenceAdvisorImpl(CacheDistributionAdvisor cacheDistributionAdvisor,
      DistributedLockService distributedLockService, PersistentMemberView persistentMemberView,
      String regionPath, DiskRegionStats diskRegionStats,
      PersistentMemberManager persistentMemberManager,
      PersistentStateQueryMessageSenderFactory persistentStateQueryMessageSenderFactory) {
    this.cacheDistributionAdvisor = cacheDistributionAdvisor;
    this.distributedLockService = distributedLockService;
    this.regionPath = regionPath;
    this.persistentMemberView = persistentMemberView;
    this.diskRegionStats = diskRegionStats;
    profileChangeListener = new ProfileChangeListener();
    this.persistentMemberManager = persistentMemberManager;
    this.persistentStateQueryMessageSenderFactory = persistentStateQueryMessageSenderFactory;

    // Prevent membership changes while we are persisting the membership view online. If we
    // synchronize on something else, we need to be careful about lock ordering because the
    // membership notifications are called with the advisor lock held.
    lock = cacheDistributionAdvisor;

    // Remember which members we know about because of what we have persisted. We will later use
    // this to handle updates from peers.
    recoveredMembers = getPersistedMembers();

    // To prevent races if we crash during initialization, mark equal members as online before we
    // initialize. We will still report these members as equal, but if we crash and recover they
    // will no longer be considered equal.
    equalMembers = new CopyOnWriteHashSet<>(persistentMemberView.getOfflineAndEqualMembers());
    for (PersistentMemberID id : equalMembers) {
      persistentMemberView.memberOnline(id);
    }
  }

  @Override
  public void initialize() {
    if (initialized) {
      return;
    }

    if (wasAboutToDestroy()) {
      logger.info("Region {} crashed during a region destroy. Finishing the destroy.",
          regionPath);
      finishPendingDestroy();
    }

    cacheDistributionAdvisor.addProfileChangeListener(profileChangeListener);

    Set<PersistentMemberPattern> revokedMembers = persistentMemberManager
        .addRevocationListener(profileChangeListener, persistentMemberView.getRevokedMembers());

    for (PersistentMemberPattern pattern : revokedMembers) {
      memberRevoked(pattern);
    }

    // Start logging changes to the persistent view
    startMemberLogging();

    initialized = true;
  }

  /**
   * Adds a PersistentStateListener whose job is to log changes in the persistent view.
   */
  private void startMemberLogging() {
    addListener(new PersistentStateListener.PersistentStateAdapter() {
      /**
       * A persistent member has gone offline. Log the offline member and log which persistent
       * members are still online (the current persistent view).
       */
      @Override
      public void memberOffline(InternalDistributedMember member, PersistentMemberID persistentID) {
        if (logger.isDebugEnabled()) {

          Set<PersistentMemberID> members =
              new HashSet<>(cacheDistributionAdvisor.adviseInitializedPersistentMembers().values());
          members.remove(persistentID);

          Set<String> onlineMembers = new HashSet<>();
          TransformUtils.transform(members, onlineMembers,
              TransformUtils.persistentMemberIdToLogEntryTransformer);

          logger.info(
              "The following persistent member has gone offline for region {}: {}  Remaining participating members for the region include: {}",
              new Object[] {regionPath,
                  TransformUtils.persistentMemberIdToLogEntryTransformer
                      .transform(persistentID),
                  onlineMembers});
        }
      }
    });
  }

  @Override
  public PersistentStateQueryResults getMyStateOnMembers(Set<InternalDistributedMember> members)
      throws ReplyException {
    return fetchPersistentStateQueryResults(members,
        cacheDistributionAdvisor.getDistributionManager(), persistentMemberView.getMyPersistentID(),
        persistentMemberView.getMyInitializingID());
  }

  private PersistentStateQueryResults fetchPersistentStateQueryResults(
      Set<InternalDistributedMember> members, DistributionManager dm,
      PersistentMemberID persistentMemberID, PersistentMemberID initializingMemberId) {
    PersistentStateQueryReplyProcessor replyProcessor = persistentStateQueryMessageSenderFactory
        .createPersistentStateQueryReplyProcessor(dm, members);
    PersistentStateQueryMessage message =
        persistentStateQueryMessageSenderFactory.createPersistentStateQueryMessage(regionPath,
            persistentMemberID, initializingMemberId, replyProcessor.getProcessorId());
    return message.send(members, dm, replyProcessor);
  }

  /**
   * Return what state we have persisted for a given peer's id.
   */
  @Override
  public PersistentMemberState getPersistedStateOfMember(PersistentMemberID id) {
    if (isRevoked(id)) {
      return PersistentMemberState.REVOKED;
    }

    // If the peer is marked as equal, indicate they are equal
    if (equalMembers != null && equalMembers.contains(id)) {
      return PersistentMemberState.EQUAL;
    }

    // If we have a member that is marked as online that is an older version of the peers id, tell
    // them they are online
    for (PersistentMemberID onlineMember : persistentMemberView.getOnlineMembers()) {
      if (onlineMember.isOlderOrEqualVersionOf(id)) {
        return PersistentMemberState.ONLINE;
      }
    }

    // If we have a member that is marked as offline that is a newer version of the peers id, tell
    // them they are online
    for (PersistentMemberID offline : persistentMemberView.getOfflineMembers()) {
      if (id.isOlderOrEqualVersionOf(offline)) {
        return PersistentMemberState.OFFLINE;
      }
    }
    return null;
  }

  @Override
  public void updateMembershipView(InternalDistributedMember peer, boolean targetReinitializing) {
    beginUpdatingPersistentView();
    DistributionManager dm = cacheDistributionAdvisor.getDistributionManager();
    PersistentMembershipView peersPersistentMembershipView =
        MembershipViewRequest.send(peer, dm, regionPath, targetReinitializing);
    if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
      logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE, "{}-{}: Updating persistent view from {}",
          shortDiskStoreId(), regionPath, peer);
    }

    synchronized (lock) {
      PersistentMemberID myId = getPersistentID();
      Map<InternalDistributedMember, PersistentMemberID> peersOnlineMembers =
          peersPersistentMembershipView.getOnlineMembers();
      Set<PersistentMemberID> peersOfflineMembers =
          peersPersistentMembershipView.getOfflineMembers();

      for (PersistentMemberID id : peersOnlineMembers.values()) {
        if (!isRevoked(id) && !removedMembers.contains(id)) {
          if (!id.equals(myId) && !recoveredMembers.remove(id)
              && !id.getDiskStoreId().equals(getDiskStoreID())) {
            if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
              logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
                  "{}-{}: Processing membership view from peer. Marking {} as online because {} says its online",
                  shortDiskStoreId(), regionPath, id, peer);
            }
            persistentMemberView.memberOnline(id);
          }
        }
      }

      for (PersistentMemberID id : peersOfflineMembers) {
        if (!isRevoked(id) && !removedMembers.contains(id)) {
          // This method is called before the current member is online. if the peer knows about a
          // member that the current member doesn't know about, that means that member must have
          // been added to the DS after the current member went offline. Therefore, that member is
          // *newer* than the current member. So mark that member as online (meaning, online later
          // than the current member).
          if (!id.equals(myId) && !recoveredMembers.remove(id)
              && !id.getDiskStoreId().equals(getDiskStoreID())) {
            if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
              logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
                  "{}-{}: Processing membership view from peer. Marking {} as online because {} says its offline, but we have never seen it",
                  shortDiskStoreId(), regionPath, id, peer);
            }
            persistentMemberView.memberOnline(id);
          }
        }
      }


      for (PersistentMemberID id : recoveredMembers) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
              "{}-{}: Processing membership view from peer. Removing {} because {} doesn't have it",
              shortDiskStoreId(), regionPath, id, peer);
        }
        persistentMemberView.memberRemoved(id);
      }
    }

    // Update the set of revoked members from the peer. This should be called without holding the
    // lock to avoid deadlocks
    Set<PersistentMemberPattern> revokedMembers = peersPersistentMembershipView.getRevokedMembers();
    for (PersistentMemberPattern revoked : revokedMembers) {
      persistentMemberManager.revokeMember(revoked);
    }
  }

  private boolean isRevoked(PersistentMemberID id) {
    return persistentMemberManager.isRevoked(regionPath, id);
  }

  @Override
  public void setOnline(boolean didGII, boolean atomicCreation, PersistentMemberID newId)
      throws ReplyException {
    if (online) {
      return;
    }

    if (!didGII) {
      setInitializing(newId);
    }

    synchronized (lock) {

      // Transition any members that are marked as online, but not actually currently running, to
      // offline.
      Set<PersistentMemberID> membersToMarkOffline =
          new HashSet<>(persistentMemberView.getOnlineMembers());
      Map<InternalDistributedMember, PersistentMemberID> onlineMembers;
      if (!atomicCreation) {
        onlineMembers = cacheDistributionAdvisor.adviseInitializedPersistentMembers();
      } else {
        // Fix for 41100 - If this is an atomic bucket creation, don't mark our peers, which are
        // concurrently initializing, as offline they have the exact same data as we do (none), so
        // we are not technically "newer," and this avoids a race where both members can think the
        // other is offline ("older").
        onlineMembers = cacheDistributionAdvisor.advisePersistentMembers();
      }
      membersToMarkOffline.removeAll(onlineMembers.values());

      // Another fix for 41100 - Don't mark equal members as offline if that are currently running.
      // We don't have newer data than these members so this is safe, and it it avoids a race where
      // we mark them offline at this point, and then later they mark us as offline.
      if (equalMembers != null && !equalMembers.isEmpty()) {

        // This is slightly hacky. We're looking for a running member that has the same disk store
        // as our equal members, because all have is a persistent id of the equal members. The
        // persistent id of the running member may be different than what we have marked as equal,
        // because the id in the profile is the new id for the member.
        Collection<PersistentMemberID> allMembers =
            cacheDistributionAdvisor.advisePersistentMembers().values();
        Set<DiskStoreID> runningDiskStores = new HashSet<>();
        for (PersistentMemberID mem : allMembers) {
          runningDiskStores.add(mem.getDiskStoreId());
        }
        // Remove any equal members which are not actually running right now.
        for (PersistentMemberID id : equalMembers) {
          if (!runningDiskStores.contains(id.getDiskStoreId())) {
            equalMembers.remove(id);
          }
        }
        membersToMarkOffline.removeAll(equalMembers);
      }
      for (PersistentMemberID id : membersToMarkOffline) {
        persistentMemberView.memberOffline(id);
      }
      if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
        logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
            "{}-{}: Persisting the new membership view and ID as online. Online members {}. Offline members {}. Equal memebers {}.",
            shortDiskStoreId(), regionPath, persistentMemberView.getOnlineMembers(),
            persistentMemberView.getOfflineMembers(), equalMembers);
      }

      persistentMemberView.setInitialized();
      online = true;
      removedMembers.clear();
    }
    if (diskRegionStats != null) {
      diskRegionStats.incInitializations(!didGII);
    }
  }

  /**
   * Start listening for persistent view updates and apply any updates that have already happened.
   *
   * This method should be called after we have decided that there is no conflicting persistent
   * exception.
   *
   * Fix for bug 44045.
   */
  protected void beginUpdatingPersistentView() {
    synchronized (lock) {
      // Only update the view if it is has not already happened.
      if (!shouldUpdatePersistentView) {
        shouldUpdatePersistentView = true;
        Map<InternalDistributedMember, PersistentMemberID> onlineMembers =
            cacheDistributionAdvisor.adviseInitializedPersistentMembers();
        for (Map.Entry<InternalDistributedMember, PersistentMemberID> entry : onlineMembers
            .entrySet()) {
          memberOnline(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  @Override
  public void setInitializing(PersistentMemberID newId) {
    beginUpdatingPersistentView();

    DistributionManager dm = cacheDistributionAdvisor.getDistributionManager();

    PersistentMemberID oldId = getPersistentID();
    PersistentMemberID initializingId = getInitializingID();

    Set<InternalDistributedMember> profileUpdateRecipients =
        cacheDistributionAdvisor.adviseProfileUpdate();
    if (newId == null || !newId.equals(oldId) && !newId.equals(initializingId)) {
      // If we have not yet prepared the old id, prepare it now.


      // This will only be the case if we crashed while initializing previously. In the case, we are
      // essentially finishing what we started by preparing that ID first. This will remove that ID
      // from the peers.
      if (initializingId != null) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
              "{}-{}: We still have an initializing id: {}. Telling peers to remove the old id {} and transitioning this initializing id to old id. recipients {}",
              shortDiskStoreId(), regionPath, initializingId, oldId, profileUpdateRecipients);
        }
        long viewVersion = cacheDistributionAdvisor.startOperation();
        try {
          PrepareNewPersistentMemberMessage.send(profileUpdateRecipients, dm, regionPath, oldId,
              initializingId);
        } finally {
          if (viewVersion != -1) {
            cacheDistributionAdvisor.endOperation(viewVersion);
          }
        }
        oldId = initializingId;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Persisting my new persistent ID {}", newId);
      }
      persistentMemberView.setInitializing(newId);
    }

    profileUpdateRecipients = cacheDistributionAdvisor.adviseProfileUpdate();
    if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
      logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
          "{}-{}: Sending the new ID to peers. They should remove the old id {}. Recipients: {}",
          shortDiskStoreId(), regionPath, oldId, profileUpdateRecipients);
    }
    if (newId != null) {
      PrepareNewPersistentMemberMessage.send(profileUpdateRecipients, dm, regionPath, oldId, newId);
    }
  }

  @Override
  public PersistentMemberID generatePersistentID() {
    return persistentMemberView.generatePersistentID();
  }

  @Override
  public PersistentMembershipView getMembershipView() {
    if (!initialized) {
      return null;
    }
    Set<PersistentMemberID> offlineMembers = getPersistedMembers();
    Map<InternalDistributedMember, PersistentMemberID> onlineMembers =
        cacheDistributionAdvisor.adviseInitializedPersistentMembers();
    offlineMembers.removeAll(onlineMembers.values());

    PersistentMemberID myId = getPersistentID();
    if (myId != null) {
      onlineMembers
          .put(cacheDistributionAdvisor.getDistributionManager().getDistributionManagerId(), myId);
    }

    return new PersistentMembershipView(offlineMembers, onlineMembers,
        persistentMemberManager.getRevokedMembers());
  }

  @Override
  public Set<PersistentMemberID> getPersistedMembers() {
    Set<PersistentMemberID> persistentMembers = new HashSet<>();
    persistentMembers.addAll(persistentMemberView.getOfflineMembers());
    persistentMembers.addAll(persistentMemberView.getOfflineAndEqualMembers());
    persistentMembers.addAll(persistentMemberView.getOnlineMembers());
    return persistentMembers;
  }

  @Override
  public boolean checkMyStateOnMembers(Set<InternalDistributedMember> replicates)
      throws ReplyException {
    PersistentStateQueryResults remoteStates = getMyStateOnMembers(replicates);

    persistenceAdvisorObserver.observe(regionPath);

    boolean equal = false;
    for (Map.Entry<InternalDistributedMember, PersistentMemberState> entry : remoteStates
        .getStateOnPeers()
        .entrySet()) {
      InternalDistributedMember member = entry.getKey();
      PersistentMemberID remoteId = remoteStates.getPersistentIds().get(member);

      final PersistentMemberID myId = getPersistentID();
      PersistentMemberState stateOnPeer = entry.getValue();

      if (PersistentMemberState.REVOKED.equals(stateOnPeer)) {
        throw new RevokedPersistentDataException(
            String.format(
                "The persistent member id %s has been revoked in this distributed system. You cannot recover from disk files which have been revoked.",
                myId));
      }

      if (myId != null && stateOnPeer == null) {
        String message = String.format(
            "Region %s remote member %s with persistent data %s was not part of the same distributed system as the local data from %s",
            regionPath, member, remoteId, myId);
        throw new ConflictingPersistentDataException(message);
      }

      if (myId != null && stateOnPeer == PersistentMemberState.EQUAL) {
        equal = true;
      }

      // The other member changes its ID when it comes back online.
      if (remoteId != null) {
        PersistentMemberState remoteState = getPersistedStateOfMember(remoteId);
        if (remoteState == PersistentMemberState.OFFLINE) {
          String message =
              String.format(
                  "Region %s refusing to initialize from member %s with persistent data %s which was offline when the local data from %s was last online",
                  regionPath, member, remoteId, myId);
          throw new ConflictingPersistentDataException(message);
        }
      }
    }
    return equal;
  }

  public static void setPersistenceAdvisorObserver(PersistenceAdvisorObserver o) {
    persistenceAdvisorObserver = o == null ? DEFAULT_PERSISTENCE_ADVISOR_OBSERVER : o;
  }

  @Override
  public PersistentMemberID getPersistentIDIfOnline() {
    if (online) {
      return persistentMemberView.getMyPersistentID();
    } else {
      return null;
    }
  }

  private void memberOffline(InternalDistributedMember distributedMember,
      PersistentMemberID persistentID) {
    if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
      logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
          "{}-{}: Member offine. id={}, persistentID={}", shortDiskStoreId(), regionPath,
          distributedMember, persistentID);
    }
    synchronized (lock) {
      boolean foundMember = recoveredMembers.remove(persistentID);
      foundMember |= equalMembers.remove(persistentID);
      foundMember |= getPersistedMembers().contains(persistentID);
      // Don't persist members as offline until we are online. Otherwise, we may think we have later
      // data than them during recovery.
      if (shouldUpdatePersistentView && online) {
        try {
          // Don't persistent members as offline if we have already persisted them as equal.
          if (persistentMemberView.getOfflineAndEqualMembers().contains(persistentID)) {
            return;
          }
          // Don't mark the member as offline if we have never seen it. If we haven't seen it that
          // means it's not done initializing yet.
          if (foundMember) {
            if (PersistenceObserverHolder.getInstance().memberOffline(regionPath, persistentID)) {
              persistentMemberView.memberOffline(persistentID);
            }
            PersistenceObserverHolder.getInstance().afterPersistedOffline(regionPath, persistentID);
          }
        } catch (DiskAccessException e) {
          logger.warn("Unable to persist membership change", e);
        }
      }
      notifyListenersMemberOffline(distributedMember, persistentID);
    }

  }

  private void memberOnline(InternalDistributedMember distributedMember,
      PersistentMemberID persistentID) {
    if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
      logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
          "{}-{}: Sending the new ID to peers.  Member online. id={}, persistentID={}",
          shortDiskStoreId(), regionPath, distributedMember, persistentID);
    }
    synchronized (lock) {
      if (shouldUpdatePersistentView) {
        recoveredMembers.remove(persistentID);
        try {
          if (PersistenceObserverHolder.getInstance().memberOnline(regionPath, persistentID)) {
            persistentMemberView.memberOnline(persistentID);
          }
          PersistenceObserverHolder.getInstance().afterPersistedOnline(regionPath, persistentID);
        } catch (DiskAccessException e) {
          logger.warn("Unable to persist membership change", e);
        }
      } else {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
              "{}-{}: Not marking member online in persistent view because we're still in initialization",
              shortDiskStoreId(), regionPath);
        }
      }

      notifyListenersMemberOnline(distributedMember, persistentID);
    }
  }

  private void memberRevoked(PersistentMemberPattern pattern) {
    // Persist the revoked member, so if we recover later we will remember that they were revoked.
    persistentMemberView.memberRevoked(pattern);

    // Remove the revoked member from our view.
    for (PersistentMemberID id : persistentMemberView.getOfflineMembers()) {
      if (pattern.matches(id)) {
        memberRemoved(id, true);
      }
    }
    for (PersistentMemberID id : persistentMemberView.getOnlineMembers()) {
      if (pattern.matches(id)) {
        memberRemoved(id, true);
      }
    }
    for (PersistentMemberID id : persistentMemberView.getOfflineAndEqualMembers()) {
      if (pattern.matches(id)) {
        memberRemoved(id, true);
      }
    }
  }

  private void memberRemoved(PersistentMemberID id, boolean revoked) {
    if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
      logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE, "{}-{}: Member removed. persistentID={}",
          shortDiskStoreId(), regionPath, id);
    }

    synchronized (lock) {
      recoveredMembers.remove(id);
      equalMembers.remove(id);
      if (!online) {
        removedMembers.add(id);
      }
      try {
        if (PersistenceObserverHolder.getInstance().memberRemoved(regionPath, id)) {
          persistentMemberView.memberRemoved(id);
        }

        // Purge any IDs that are old versions of the the id that we just removed
        for (PersistentMemberID persistedId : getPersistedMembers()) {
          if (persistedId.isOlderOrEqualVersionOf(id)) {
            persistentMemberView.memberRemoved(persistedId);
          }
        }
        PersistenceObserverHolder.getInstance().afterRemovePersisted(regionPath, id);
      } catch (DiskAccessException e) {
        logger.warn("Unable to persist membership change", e);
      }
      notifyListenersMemberRemoved(id, revoked);
    }
  }

  @Override
  public PersistentMemberID getPersistentID() {
    return persistentMemberView.getMyPersistentID();
  }

  @Override
  public PersistentMemberID getInitializingID() {
    return persistentMemberView.getMyInitializingID();
  }

  @Override
  public void addListener(PersistentStateListener listener) {
    synchronized (this) {
      Set<PersistentStateListener> tmpListeners = new HashSet<>(persistentStateListeners);
      tmpListeners.add(listener);
      persistentStateListeners = Collections.unmodifiableSet(tmpListeners);
    }

  }

  @Override
  public void removeListener(PersistentStateListener listener) {
    synchronized (this) {
      Set<PersistentStateListener> tmpListeners = new HashSet<>(persistentStateListeners);
      tmpListeners.remove(listener);
      persistentStateListeners = Collections.unmodifiableSet(tmpListeners);
    }
  }

  private void notifyListenersMemberOnline(InternalDistributedMember member,
      PersistentMemberID persistentID) {
    for (PersistentStateListener listener : persistentStateListeners) {
      listener.memberOnline(member, persistentID);
    }
  }

  private void notifyListenersMemberOffline(InternalDistributedMember member,
      PersistentMemberID persistentID) {
    for (PersistentStateListener listener : persistentStateListeners) {
      listener.memberOffline(member, persistentID);
    }
  }

  private void notifyListenersMemberRemoved(PersistentMemberID persistentID, boolean revoked) {
    for (PersistentStateListener listener : persistentStateListeners) {
      listener.memberRemoved(persistentID, revoked);
    }
  }

  @Override
  public HashSet<PersistentMemberID> getPersistedOnlineOrEqualMembers() {
    HashSet<PersistentMemberID> members = new HashSet<>(persistentMemberView.getOnlineMembers());
    members.addAll(equalMembers);
    return members;
  }

  @Override
  public void prepareNewMember(InternalDistributedMember sender, PersistentMemberID oldId,
      PersistentMemberID newId) {
    if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
      logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
          "{}-{}: Preparing new persistent id {}. Old id is {}", shortDiskStoreId(), regionPath,
          newId, oldId);
    }
    synchronized (lock) {
      // Don't prepare the ID if the advisor doesn't have a profile. This prevents a race with the
      // advisor remove
      if (!cacheDistributionAdvisor.containsId(sender)) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
              "{}-{}: Refusing to prepare id because {} is not in our advisor", shortDiskStoreId(),
              regionPath, sender);
        }
        return;
      }
      // Persist new members even if we are not online yet. Two members can become online at once.
      // This way, they will know about each other.
      persistentMemberView.memberOnline(newId);

      // The oldId and newId could be the same if the member is retrying a GII. See bug #42051
      if (oldId != null && !oldId.equals(newId)) {
        if (initialized) {
          memberRemoved(oldId, false);
        }
      }
    }
  }

  protected String shortDiskStoreId() {
    DiskStoreID diskStoreID = getDiskStoreID();
    return diskStoreID == null ? "mem" : diskStoreID.abbrev();
  }

  @Override
  public void removeMember(PersistentMemberID id) {
    memberRemoved(id, false);
  }

  @Override
  public void markMemberOffline(InternalDistributedMember member, PersistentMemberID id) {
    memberOffline(member, id);
  }

  @Override
  public CacheDistributionAdvisor getCacheDistributionAdvisor() {
    return cacheDistributionAdvisor;
  }

  @Override
  public void setWaitingOnMembers(Set<PersistentMemberID> allMembersToWaitFor,
      Set<PersistentMemberID> offlineMembersToWaitFor) {
    allMembersWaitingFor = allMembersToWaitFor;
    offlineMembersWaitingFor = offlineMembersToWaitFor;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }


  public void finishPendingDestroy() {
    // send a message to peers indicating that they should remove this profile
    long viewVersion = cacheDistributionAdvisor.startOperation();
    try {
      RemovePersistentMemberMessage.send(cacheDistributionAdvisor.adviseProfileUpdate(),
          cacheDistributionAdvisor.getDistributionManager(), regionPath, getPersistentID(),
          getInitializingID());

      persistentMemberView.finishPendingDestroy();
    } finally {
      if (viewVersion != -1) {
        cacheDistributionAdvisor.endOperation(viewVersion);
      }
    }
    synchronized (lock) {
      recoveredMembers.clear();
    }
  }

  /**
   * Returns the member id of the member who has the latest copy of the persistent region. This may
   * be the local member ID if this member has the latest known copy.
   *
   * This method will block until the latest member is online.
   *
   * @throws ConflictingPersistentDataException if there are active members which are not based on
   *         the state that is persisted in this member.
   */
  @Override
  public InitialImageAdvice getInitialImageAdvice(InitialImageAdvice previousAdvice,
      boolean hasDiskImageToRecoverFrom) {
    PersistenceInitialImageAdvisor piia = new PersistenceInitialImageAdvisor(this,
        shortDiskStoreId(), regionPath, cacheDistributionAdvisor, hasDiskImageToRecoverFrom);
    return piia.getAdvice(previousAdvice);
  }

  /**
   * @param previouslyOnlineMembers the members we have persisted online in our persistence files
   * @param offlineMembers This method will populate this set with any members that we are waiting
   *        for an are actually not running right now. This is different that the set of members we
   *        need to wait for - this member may end up waiting on member that is actually running.
   * @return the list of members that this member needs to wait for before it can initialize.
   */
  @Override
  public Set<PersistentMemberID> getMembersToWaitFor(
      Set<PersistentMemberID> previouslyOnlineMembers, Set<PersistentMemberID> offlineMembers)
      throws ReplyException {
    PersistentMemberID myPersistentID = getPersistentID();
    PersistentMemberID myInitializingId = getInitializingID();

    // This is the set of members that are currently waiting for this member
    // to come online.
    Set<PersistentMemberID> membersToWaitFor = new HashSet<>(previouslyOnlineMembers);
    offlineMembers.addAll(previouslyOnlineMembers);

    // If our persistent ID is null, we need to wait for all of the previously online members.
    if (myPersistentID != null || myInitializingId != null) {
      Set<InternalDistributedMember> members = cacheDistributionAdvisor.adviseProfileUpdate();
      Set<InternalDistributedMember> membersHostingThisRegion =
          cacheDistributionAdvisor.adviseGeneric();

      // Fetch the persistent view from all of our peers.
      PersistentStateQueryResults results = fetchPersistentStateQueryResults(members,
          cacheDistributionAdvisor.getDistributionManager(), myPersistentID, myInitializingId);

      // iterate through all of the peers. For each peer: if the member was previously online
      // according
      // to us, grab its online members and add them to the members to wait for set. We may need to
      // do this several times until we discover all of the members that may have newer data than
      // us.
      boolean addedMembers = true;
      while (addedMembers) {
        addedMembers = false;
        for (Entry<InternalDistributedMember, Set<PersistentMemberID>> entry : results
            .getOnlineMemberMap()
            .entrySet()) {
          InternalDistributedMember memberId = entry.getKey();
          Set<PersistentMemberID> peersOnlineMembers = entry.getValue();
          PersistentMemberID persistentID = results.getPersistentIds().get(memberId);
          PersistentMemberID initializingID = results.getInitializingIds().get(memberId);
          if (membersToWaitFor.contains(persistentID)
              || membersToWaitFor.contains(initializingID)) {
            for (PersistentMemberID peerOnlineMember : peersOnlineMembers) {
              if (!isRevoked(peerOnlineMember)
                  && !peerOnlineMember.getDiskStoreId().equals(getDiskStoreID())
                  && !persistentMemberView.getOfflineMembers().contains(peerOnlineMember)) {
                if (membersToWaitFor.add(peerOnlineMember)) {
                  addedMembers = true;
                  // Make sure we also persist that this member is online.
                  persistentMemberView.memberOnline(peerOnlineMember);
                  if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
                    logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
                        "{}-{}: Adding {} to the list of members we're wait for, because {} has newer or equal data than is and is waiting for that member",
                        shortDiskStoreId(), regionPath, peerOnlineMember, memberId);
                  }
                }
              }
            }
          }
        }
      }
      removeOlderMembers(membersToWaitFor);
      if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
        logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
            "{}-{}: Initial state of membersToWaitFor, before pruning {}", shortDiskStoreId(),
            regionPath, membersToWaitFor);
      }

      // For each of our peers, see what our state is according to their view.
      for (Map.Entry<InternalDistributedMember, PersistentMemberState> entry : results
          .getStateOnPeers().entrySet()) {
        InternalDistributedMember memberId = entry.getKey();
        PersistentMemberID persistentID = results.getPersistentIds().get(memberId);
        PersistentMemberID initializingID = results.getInitializingIds().get(memberId);
        DiskStoreID diskStoreID = results.getDiskStoreIds().get(memberId);
        PersistentMemberState state = entry.getValue();

        if (PersistentMemberState.REVOKED.equals(state)) {
          throw new RevokedPersistentDataException(
              String.format(
                  "The persistent member id %s has been revoked in this distributed system. You cannot recover from disk files which have been revoked.",
                  myPersistentID));
        }

        // If the peer thinks we are newer or equal to them, we don't need to wait for this peer.
        if (membersHostingThisRegion.contains(memberId) && persistentID != null && state != null
            && myInitializingId == null && (state.equals(PersistentMemberState.ONLINE)
                || state.equals(PersistentMemberState.EQUAL))) {
          if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
            logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
                "{}-{}: Not waiting for {} because it thinks our state was {}", shortDiskStoreId(),
                regionPath, persistentID, state);
          }
          removeNewerPersistentID(membersToWaitFor, persistentID);
        }

        // If the peer has an initialized ID, they are no longer offline.
        if (persistentID != null) {
          removeNewerPersistentID(offlineMembers, persistentID);
        }

        // If the peer thinks we are newer or equal to them, we don't need to wait for this peer.
        if (membersHostingThisRegion.contains(memberId) && initializingID != null && state != null
            && (state.equals(PersistentMemberState.ONLINE)
                || state.equals(PersistentMemberState.EQUAL))) {
          if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
            logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
                "{}-{}: Not waiting for {} because it thinks our state was {}", shortDiskStoreId(),
                regionPath, initializingID, state);
          }
          removeByDiskStoreID(membersToWaitFor, diskStoreID, false);
        }

        // If the peer has an initializing id, they are also not online.
        if (initializingID != null) {
          removeNewerPersistentID(offlineMembers, initializingID);
        }

        // If we were able to determine what disk store this member is in, and it doesn't have a
        // persistent ID, but we think we should be waiting for it, stop waiting for it.
        if (initializingID == null && persistentID == null & diskStoreID != null) {
          removeByDiskStoreID(membersToWaitFor, diskStoreID, true);
          removeByDiskStoreID(offlineMembers, diskStoreID, true);
        }
      }
    }
    return membersToWaitFor;
  }

  /**
   * Given a set of persistent members, if the same member occurs more than once in the set but
   * with different timestamps, remove the older ones leaving only the most recent.
   *
   * @param persistentMemberSet The set of persistent members, possibly modified by this method.
   */
  protected void removeOlderMembers(Set<PersistentMemberID> persistentMemberSet) {
    Map<DiskStoreID, PersistentMemberID> mostRecentMap = new HashMap<>();
    List<PersistentMemberID> idsToRemove = new ArrayList<>();
    for (PersistentMemberID persistentMember : persistentMemberSet) {
      DiskStoreID diskStoreId = persistentMember.getDiskStoreId();
      PersistentMemberID mostRecent = mostRecentMap.get(diskStoreId);
      if (mostRecent == null) {
        mostRecentMap.put(diskStoreId, persistentMember);
      } else {
        PersistentMemberID older = persistentMember;
        boolean persistentMemberIsNewer =
            !persistentMember.isOlderOrEqualVersionOf(mostRecent);
        if (persistentMemberIsNewer) {
          older = mostRecent;
          mostRecentMap.put(diskStoreId, persistentMember);
        }
        idsToRemove.add(older);
      }
    }
    persistentMemberSet.removeAll(idsToRemove);
  }

  /**
   * Remove all members with a given disk store id from the set of members to wait for, who is newer
   * than the real one. The reason is: A is waiting for B2, but B sends B1<=A to A. That means A
   * knows more than B in both B1 and B2. B itself knows nothing about B2. So we don't need to wait
   * for B2 (since we don't need to wait for B1).
   */
  private void removeNewerPersistentID(Set<PersistentMemberID> membersToWaitFor,
      PersistentMemberID persistentID) {
    for (Iterator<PersistentMemberID> itr = membersToWaitFor.iterator(); itr.hasNext();) {
      PersistentMemberID id = itr.next();
      if (persistentID.isOlderOrEqualVersionOf(id)) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
              "{}-{}: Not waiting for {} because local member knows more about it",
              shortDiskStoreId(), regionPath, id);
        }
        itr.remove();
      }
    }
  }

  /**
   * Remove all members with a given disk store id from the set of members to wait for.
   */
  private void removeByDiskStoreID(Set<PersistentMemberID> membersToWaitFor,
      DiskStoreID diskStoreID, boolean updateAdvisor) {
    for (Iterator<PersistentMemberID> itr = membersToWaitFor.iterator(); itr.hasNext();) {
      PersistentMemberID id = itr.next();
      if (id.getDiskStoreId().equals(diskStoreID)) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
              "{}-{}: Not waiting for {} because it no longer has this region in its disk store",
              shortDiskStoreId(), regionPath, id);
        }
        itr.remove();
        if (updateAdvisor) {
          memberRemoved(id, false);
        }
      }
    }
  }

  @Override
  public boolean wasHosting() {
    return getPersistentID() != null || getInitializingID() != null;
  }

  protected String getRegionPathForOfflineMembers() {
    return regionPath;
  }

  /**
   * Returns the set of missing members that we report back to the any admin DS looking for missing
   * members.
   */
  protected Set<PersistentMemberID> getMissingMembers() {
    return offlineMembersWaitingFor;
  }

  /**
   * Returns the set of missing members that we report back to the any admin DS looking for missing
   * members.
   */
  public Set<PersistentMemberID> getAllMembersToWaitFor() {
    return allMembersWaitingFor;
  }

  @Override
  public void logWaitingForMembers() {
    Set<String> membersToWaitForLogEntries = new HashSet<>();

    if (offlineMembersWaitingFor != null && !offlineMembersWaitingFor.isEmpty()) {
      TransformUtils.transform(offlineMembersWaitingFor, membersToWaitForLogEntries,
          TransformUtils.persistentMemberIdToLogEntryTransformer);

      StartupStatus.startup(
          String.format(
              "Region %s has potentially stale data. It is waiting for another member to recover the latest data.My persistent id:%sMembers with potentially new data:%sUse the gfsh show missing-disk-stores command to see all disk stores that are being waited on by other members.",
              regionPath,
              TransformUtils.persistentMemberIdToLogEntryTransformer.transform(getPersistentID()),
              membersToWaitForLogEntries));
    } else {
      TransformUtils.transform(allMembersWaitingFor, membersToWaitForLogEntries,
          TransformUtils.persistentMemberIdToLogEntryTransformer);

      StartupStatus.startup(
          String.format(
              "Region %s has potentially stale data. It is waiting for another online member to recover the latest data.My persistent id:%sMembers with potentially new data:%sUse the gfsh show missing-disk-stores command to see all disk stores that are being waited on by other members.",
              regionPath,
              TransformUtils.persistentMemberIdToLogEntryTransformer.transform(getPersistentID()),
              membersToWaitForLogEntries));
    }
  }

  @Override
  public void clearEqualMembers() {
    synchronized (lock) {
      equalMembers.clear();
    }
  }

  @Override
  public void checkInterruptedByShutdownAll() {}

  @Override
  public void close() {
    isClosed = true;
    persistentMemberManager.removeRevocationListener(profileChangeListener);
    cacheDistributionAdvisor.removeProfileChangeListener(profileChangeListener);
    releaseTieLock();
  }


  /**
   * Try to acquire the distributed lock which members must grab for in the case of a tie. Whoever
   * gets the lock initializes first.
   */
  @Override
  public boolean acquireTieLock() {
    // We're tied for the latest copy of the data. try to get the distributed lock.
    holdingTieLock = distributedLockService.lock("PERSISTENCE_" + regionPath, 0, -1);
    if (!holdingTieLock) {
      if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
        logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE, "{}-{}: Failed to acquire the lock.",
            shortDiskStoreId(), regionPath);
      }
    }
    return holdingTieLock;
  }

  @Override
  public void releaseTieLock() {
    if (holdingTieLock) {
      distributedLockService.unlock("PERSISTENCE_" + regionPath);
      holdingTieLock = false;
    }
  }

  private boolean wasAboutToDestroy() {
    return persistentMemberView.wasAboutToDestroy()
        || persistentMemberView.wasAboutToDestroyDataStorage();
  }

  protected synchronized void resetState() {
    online = false;
    removedMembers.clear();
  }

  public void flushMembershipChanges() {
    try {
      cacheDistributionAdvisor.waitForCurrentOperations();
    } catch (RegionDestroyedException ignored) {
    }
  }

  @Override
  public void persistMembersOfflineAndEqual(
      Map<InternalDistributedMember, PersistentMemberID> map) {
    for (PersistentMemberID persistentID : map.values()) {
      persistentMemberView.memberOfflineAndEqual(persistentID);
    }
  }

  @Override
  public DiskStoreID getDiskStoreID() {
    return persistentMemberView.getDiskStoreID();
  }

  @Override
  public boolean isOnline() {
    return online;
  }

  public interface PersistenceAdvisorObserver {
    void observe(String regionPath);
  }

  private class ProfileChangeListener implements ProfileListener, MemberRevocationListener {

    @Override
    public void profileCreated(Profile profile) {
      profileUpdated(profile);
    }

    @Override
    public void profileRemoved(Profile profile, boolean destroyed) {
      CacheProfile cp = (CacheProfile) profile;
      if (cp.persistentID != null) {
        if (destroyed) {
          memberRemoved(cp.persistentID, false);
        } else {
          memberOffline(profile.getDistributedMember(), cp.persistentID);
        }
      }
    }

    @Override
    public void profileUpdated(Profile profile) {
      CacheProfile cp = (CacheProfile) profile;
      if (cp.persistentID != null && cp.persistenceInitialized) {
        memberOnline(profile.getDistributedMember(), cp.persistentID);
      }
    }

    @Override
    public void revoked(PersistentMemberPattern pattern) {
      memberRevoked(pattern);
    }

    @Override
    public Set<PersistentMemberID> getMissingMemberIds() {
      return getMissingMembers();
    }

    @Override
    public String getRegionPath() {
      return getRegionPathForOfflineMembers();
    }

    @Override
    public boolean matches(PersistentMemberPattern pattern) {
      return pattern.matches(getPersistentID()) || pattern.matches(getInitializingID());
    }

    @Override
    public void addPersistentIDs(Set<PersistentMemberID> localData) {
      PersistentMemberID id = getPersistentID();
      if (id != null) {
        localData.add(id);
      }
      id = getInitializingID();
      if (id != null) {
        localData.add(id);
      }
    }
  }
}
