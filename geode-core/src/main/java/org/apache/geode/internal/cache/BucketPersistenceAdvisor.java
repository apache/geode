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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion.BucketLock;
import org.apache.geode.internal.cache.partitioned.RedundancyAlreadyMetException;
import org.apache.geode.internal.cache.persistence.MembershipChangeListener;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisorImpl;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberView;
import org.apache.geode.internal.cache.persistence.PersistentStateListener.PersistentStateAdapter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.util.TransformUtils;

public class BucketPersistenceAdvisor extends PersistenceAdvisorImpl {

  private static final Logger logger = LogService.getLogger();

  public CountDownLatch someMemberRecoveredLatch = new CountDownLatch(1);
  public boolean recovering = true;
  private boolean atomicCreation;
  private final BucketLock bucketLock;
  /**
   * A listener to watch for removes during the recovery process. After recovery is done, we know
   * longer need to worry about removes.
   */
  private final RecoveryListener recoveryListener;
  private final ProxyBucketRegion proxyBucket;
  private short version;
  private RuntimeException recoveryException;

  public BucketPersistenceAdvisor(CacheDistributionAdvisor advisor, DistributedLockService dl,
      PersistentMemberView storage, String regionPath, DiskRegionStats diskStats,
      PersistentMemberManager memberManager, BucketLock bucketLock,
      ProxyBucketRegion proxyBucketRegion) {
    super(advisor, dl, storage, regionPath, diskStats, memberManager);
    this.bucketLock = bucketLock;

    recoveryListener = new RecoveryListener();
    this.proxyBucket = proxyBucketRegion;
    addListener(recoveryListener);
  }

  public void recoveryDone(RuntimeException e) {
    this.recovering = false;
    if (!getPersistedMembers().isEmpty()) {
      ((BucketAdvisor) cacheDistributionAdvisor).setHadPrimary();
    }
    // Make sure any removes that we saw during recovery are
    // applied.
    removeListener(recoveryListener);
    for (PersistentMemberID id : recoveryListener.getRemovedMembers()) {
      removeMember(id);
    }
    if (someMemberRecoveredLatch.getCount() > 0) {
      this.recoveryException = e;
      this.someMemberRecoveredLatch.countDown();
    } else if (recoveryException != null) {
      logger.fatal(
          String.format("Unable to recover secondary bucket from disk for region %s bucket %s",
              new Object[] {proxyBucket.getPartitionedRegion().getFullPath(),
                  proxyBucket.getBucketId()}),
          e);
    }
  }

  @Override
  public void checkInterruptedByShutdownAll() {
    // when ShutdownAll is on-going, break all the GII for BR
    if (proxyBucket.getCache().isCacheAtShutdownAll()) {
      throw proxyBucket.getCache().getCacheClosedException("Cache is being closed by ShutdownAll");
    }
    proxyBucket.getPartitionedRegion().checkReadiness();
  }

  public boolean isRecovering() {
    return this.recovering;
  }

  @Override
  public void beginWaitingForMembershipChange(Set<PersistentMemberID> membersToWaitFor) {
    if (recovering) {
      bucketLock.unlock();
    } else {
      if (membersToWaitFor != null && !membersToWaitFor.isEmpty()) {
        String message = String.format(
            "Region %s bucket %s has persistent data that is no longer online stored at these locations: %s",
            proxyBucket.getPartitionedRegion().getFullPath(),
            proxyBucket.getBucketId(), membersToWaitFor);
        throw new PartitionOfflineException((Set) membersToWaitFor, message);
      }
    }
  }

  @Override
  public void logWaitingForMembers() {
    // We only log the bucket level information at fine level.
    if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
      Set<String> membersToWaitForPrettyFormat = new HashSet<String>();

      if (offlineMembersWaitingFor != null && !offlineMembersWaitingFor.isEmpty()) {
        TransformUtils.transform(offlineMembersWaitingFor, membersToWaitForPrettyFormat,
            TransformUtils.persistentMemberIdToLogEntryTransformer);
        logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
            "Region {}, bucket {} has potentially stale data.  It is waiting for another member to recover the latest data.My persistent id: {} Members with potentially new data:{}  Use the gfsh show missing-disk-stores command to see all disk stores that are being waited on by other members.",
            new Object[] {proxyBucket.getPartitionedRegion().getFullPath(),
                proxyBucket.getBucketId(),
                TransformUtils.persistentMemberIdToLogEntryTransformer
                    .transform(getPersistentID()),
                membersToWaitForPrettyFormat});
      } else {
        TransformUtils.transform(allMembersWaitingFor, membersToWaitForPrettyFormat,
            TransformUtils.persistentMemberIdToLogEntryTransformer);
        if (logger.isDebugEnabled()) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
              "All persistent members being waited on are online, but they have not yet initialized");
        }
        logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
            "Region {}, bucket {} has potentially stale data.  It is waiting for another member to recover the latest data. My persistent id: {} Members with potentially new data:{}  Use the gfsh show missing-disk-stores command to see all disk stores that are being waited on by other members.",
            new Object[] {proxyBucket.getPartitionedRegion().getFullPath(),
                proxyBucket.getBucketId(),
                TransformUtils.persistentMemberIdToLogEntryTransformer
                    .transform(getPersistentID()),
                membersToWaitForPrettyFormat});
      }
    }
  }

  @Override
  public void endWaitingForMembershipChange() {
    if (recovering) {
      bucketLock.lock();
      // We allow regions with persistent colocated children to exceed redundancy
      // so that we can create the child bucket. Otherwise, we need to check
      // that redundancy has not already been met now that we've got the dlock
      // the delock.
      if (!proxyBucket.hasPersistentChildRegion()
          && !proxyBucket.checkBucketRedundancyBeforeGrab(null, false)) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
              "{}-{}: After reacquiring dlock, we detected that redundancy is already satisfied",
              shortDiskStoreId(), regionPath);
        }
        // Remove the persistent data for this bucket, since
        // redundancy is already satisfied.
        proxyBucket.destroyOfflineData();
        throw new RedundancyAlreadyMetException();
      }
    }
  }

  @Override
  public void updateMembershipView(InternalDistributedMember replicate,
      boolean targetReinitializing) {
    if (recovering) {
      super.updateMembershipView(replicate, targetReinitializing);
      someMemberRecoveredLatch.countDown();
    } else {
      // don't update the membership view, we already updated it during recovery.
    }
  }

  /**
   * Wait for there to be initialized copies of this bucket. Get the latest membership view from
   * those copies.
   *
   */
  public void initializeMembershipView() {
    MembershipChangeListener listener = new MembershipChangeListener(this);
    addListener(listener);
    boolean interrupted = false;
    try {
      while (!isClosed) {
        cacheDistributionAdvisor.getAdvisee().getCancelCriterion().checkCancelInProgress(null);

        // Look for any online copies of the bucket.
        // If there are any, get a membership view from them.
        Map<InternalDistributedMember, PersistentMemberID> onlineMembers =
            cacheDistributionAdvisor.adviseInitializedPersistentMembers();
        if (onlineMembers != null) {
          if (updateMembershipView(onlineMembers.keySet())) {
            break;
          }
        }

        Set<InternalDistributedMember> postRecoveryMembers =
            ((BucketAdvisor) cacheDistributionAdvisor).adviseRecoveredFromDisk();
        if (postRecoveryMembers != null) {
          if (updateMembershipView(postRecoveryMembers)) {
            break;
          }
        }

        Set<PersistentMemberID> membersToWaitFor = getPersistedMembers();

        if (!membersToWaitFor.isEmpty()) {
          setWaitingOnMembers(membersToWaitFor, membersToWaitFor);
          try {
            listener.waitForChange();
          } catch (InterruptedException e) {
            interrupted = true;
          }
        } else {
          beginUpdatingPersistentView();
          break;
        }
      }
    } finally {
      setWaitingOnMembers(null, null);
      removeListener(listener);
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private boolean updateMembershipView(Collection<InternalDistributedMember> targets) {
    for (InternalDistributedMember target : targets) {
      try {
        updateMembershipView(target, false);
        return true;
      } catch (ReplyException e) {
        // The member left?
        if (logger.isDebugEnabled()) {
          logger.debug("Received a reply exception trying to update membership view", e);
        }
      }
    }
    return false;
  }

  public void bucketRemoved() {
    this.resetState();
  }

  @Override
  public boolean acquireTieLock() {
    // We don't actually need to get a dlock here for PRs, we're already
    // holding the bucket lock when we create a bucket region
    return true;
  }

  @Override
  public void releaseTieLock() {
    // We don't actually need to get a dlock here for PRs, we're already
    // holding the bucket lock when we create a bucket region
  }


  @Override
  protected String getRegionPathForOfflineMembers() {
    return proxyBucket.getPartitionedRegion().getFullPath();
  }

  @Override
  public Set<PersistentMemberID> getMissingMembers() {
    if (recovering) {
      return super.getMissingMembers();
    } else {
      Set<PersistentMemberID> offlineMembers = getPersistedMembers();
      offlineMembers.removeAll(cacheDistributionAdvisor.advisePersistentMembers().values());
      return offlineMembers;
    }
  }

  @Override
  public PersistentMemberID generatePersistentID() {
    PersistentMemberID id = persistentMemberView.generatePersistentID();
    if (id == null) {
      return id;
    } else {
      id = new PersistentMemberID(id.getDiskStoreId(), id.getHost(), id.getDirectory(),
          this.proxyBucket.getPartitionedRegion().getBirthTime(), version++);
      return id;
    }
  }



  private static class RecoveryListener extends PersistentStateAdapter {
    private Set<PersistentMemberID> removedMembers =
        Collections.synchronizedSet(new HashSet<PersistentMemberID>());

    @Override
    public void memberRemoved(PersistentMemberID persistentID, boolean revoked) {
      this.removedMembers.add(persistentID);
    }

    public HashSet<PersistentMemberID> getRemovedMembers() {
      synchronized (removedMembers) {
        return new HashSet<PersistentMemberID>(removedMembers);
      }
    }
  }

  /**
   * Callers should have already verified that debug output is enabled.
   *
   */
  public void dump(String infoMsg) {
    persistentMemberView.getOnlineMembers();
    persistentMemberView.getOfflineMembers();
    persistentMemberView.getOfflineAndEqualMembers();
    persistentMemberView.getMyInitializingID();
    persistentMemberView.getMyPersistentID();
    final StringBuilder buf = new StringBuilder(2000);
    if (infoMsg != null) {
      buf.append(infoMsg);
      buf.append(": ");
    }
    buf.append("\nMY PERSISTENT ID:\n");
    buf.append(persistentMemberView.getMyPersistentID());
    buf.append("\nMY INITIALIZING ID:\n");
    buf.append(persistentMemberView.getMyInitializingID());

    buf.append("\nONLINE MEMBERS:\n");
    for (PersistentMemberID id : persistentMemberView.getOnlineMembers()) {
      buf.append("\t");
      buf.append(id);
      buf.append("\n");
    }

    buf.append("\nOFFLINE MEMBERS:\n");
    for (PersistentMemberID id : persistentMemberView.getOfflineMembers()) {
      buf.append("\t");
      buf.append(id);
      buf.append("\n");
    }

    buf.append("\nOFFLINE AND EQUAL MEMBERS:\n");
    for (PersistentMemberID id : persistentMemberView.getOfflineAndEqualMembers()) {
      buf.append("\t");
      buf.append(id);
      buf.append("\n");
    }
    logger.debug(buf.toString());
  }

  /**
   * Wait for this bucket to be recovered from disk, at least to the point where it starts doing a
   * GII.
   *
   * This method will throw an exception if the recovery thread encountered an exception.
   */
  public void waitForPrimaryPersistentRecovery() {
    boolean interupted = false;
    while (true) {
      try {
        someMemberRecoveredLatch.await();
        break;
      } catch (InterruptedException e) {
        interupted = true;
      }
    }

    if (interupted) {
      Thread.currentThread().interrupt();
    }

    if (recoveryException != null) {
      StackTraceElement[] oldStack = recoveryException.getStackTrace();
      recoveryException.fillInStackTrace();
      ArrayList<StackTraceElement> newStack = new ArrayList<StackTraceElement>();
      newStack.addAll(Arrays.asList(oldStack));
      newStack.addAll(Arrays.asList(recoveryException.getStackTrace()));

      recoveryException.setStackTrace(newStack.toArray(new StackTraceElement[0]));
      throw recoveryException;
    }
  }



  /**
   * Overridden to fix bug 41336. We defer initialization of this member until after the atomic
   * bucket creation phase is over.
   */
  @Override
  public void setInitializing(PersistentMemberID newId) {
    if (atomicCreation) {
      if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
        logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
            "{}-{}: {} Deferring setInitializing until the EndBucketCreation phase for {}",
            shortDiskStoreId(), regionPath, regionPath, newId);
      }
    } else {
      super.setInitializing(newId);
    }
  }

  /**
   * Overridden to fix bug 41336. We defer initialization of this member until after the atomic
   * bucket creation phase is over.
   */
  @Override
  public void setOnline(boolean didGII, boolean wasAtomicCreation, PersistentMemberID newId)
      throws ReplyException {
    // This is slightly confusing. If we're currently in the middle of an atomic
    // creation, we will do nothing right now. Later, when endBucketCreation
    // is called, we will pass the "wasAtomicCreation" flag down to the super
    // class to ensure that it knows its coming online as part of an atomic creation.
    if (this.atomicCreation) {
      if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
        logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
            "{}-{}: {} Deferring setOnline until the EndBucketCreation phase for {}",
            shortDiskStoreId(), regionPath, regionPath, newId);
      }
    } else {
      super.setOnline(didGII, wasAtomicCreation, newId);
    }
  }

  /**
   * Finish the atomic creation of this bucket on multiple members This method is called with the
   * proxy bucket synchronized.
   */
  public void endBucketCreation(PersistentMemberID newId) {
    synchronized (lock) {
      if (!atomicCreation) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
          logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
              "{}-{}: {} In endBucketCreation - already online, skipping (possible concurrent endBucketCreation)",
              shortDiskStoreId(), regionPath, regionPath);
        }
        return;
      }

      if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR_VERBOSE)) {
        logger.debug(LogMarker.PERSIST_ADVISOR_VERBOSE,
            "{}-{}: {} In endBucketCreation - now persisting the id {}", shortDiskStoreId(),
            regionPath, regionPath, newId);
      }
      atomicCreation = false;
    }
    super.setOnline(false, true, newId);
  }

  public boolean isAtomicCreation() {
    return this.atomicCreation;
  }

  public void setAtomicCreation(boolean atomicCreation) {
    if (getPersistentID() != null) {
      return;
    }
    synchronized (lock) {
      this.atomicCreation = atomicCreation;
    }
  }

  private BucketPersistenceAdvisor getColocatedPersistenceAdvisor() {
    PartitionedRegion colocatedRegion =
        ColocationHelper.getColocatedRegion(proxyBucket.getPartitionedRegion());
    if (colocatedRegion == null) {
      return null;
    }
    ProxyBucketRegion colocatedProxyBucket =
        colocatedRegion.getRegionAdvisor().getProxyBucketArray()[proxyBucket.getBucketId()];
    return colocatedProxyBucket.getPersistenceAdvisor();
  }
}
