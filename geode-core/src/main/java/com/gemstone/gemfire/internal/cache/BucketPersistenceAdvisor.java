/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.BucketLock;
import com.gemstone.gemfire.internal.cache.partitioned.RedundancyAlreadyMetException;
import com.gemstone.gemfire.internal.cache.persistence.PersistenceAdvisorImpl;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberManager;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberView;
import com.gemstone.gemfire.internal.cache.persistence.PersistentStateListener.PersistentStateAdapter;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.util.TransformUtils;

/**
 *
 */
public class BucketPersistenceAdvisor extends PersistenceAdvisorImpl {

  private static final Logger logger = LogService.getLogger();
  
  public CountDownLatch someMemberRecoveredLatch = new CountDownLatch(1); 
  public boolean recovering = true;
  private boolean atomicCreation;
  private final BucketLock bucketLock;
  /** A listener to watch for removes during the recovery process.
   *  After recovery is done, we know longer need to worry about removes.
   */
  private final RecoveryListener recoveryListener;
  private final ProxyBucketRegion proxyBucket;
  private short version;
  private RuntimeException recoveryException;
  
  public BucketPersistenceAdvisor(CacheDistributionAdvisor advisor,
      DistributedLockService dl, PersistentMemberView storage, String regionPath,
      DiskRegionStats diskStats, PersistentMemberManager memberManager,
      BucketLock bucketLock, ProxyBucketRegion proxyBucketRegion) {
    super(advisor, dl, storage, regionPath, diskStats, memberManager);
    this.bucketLock = bucketLock;
    
    recoveryListener = new RecoveryListener();
    this.proxyBucket = proxyBucketRegion;
    addListener(recoveryListener);
  }

  public void recoveryDone(RuntimeException e) {
    this.recovering = false;
    if(!getPersistedMembers().isEmpty()) {
      ((BucketAdvisor) advisor).setHadPrimary();
    }
    //Make sure any removes that we saw during recovery are
    //applied.
    removeListener(recoveryListener);
    for(PersistentMemberID id : recoveryListener.getRemovedMembers()) {
      removeMember(id);
    }
    if(someMemberRecoveredLatch.getCount() > 0) {
      this.recoveryException =e;
      this.someMemberRecoveredLatch.countDown();
    } else if(recoveryException != null){
      logger.fatal(LocalizedMessage.create(LocalizedStrings.BucketPersistenceAdvisor_ERROR_RECOVERYING_SECONDARY_BUCKET_0, 
          new Object[] {proxyBucket.getPartitionedRegion().getFullPath(), proxyBucket.getBucketId()}), 
          e);
    }
  }

  protected void checkInterruptedByShutdownAll() {
    // when ShutdownAll is on-going, break all the GII for BR
    if (proxyBucket.getCache().isCacheAtShutdownAll()) {
      throw new CacheClosedException("Cache is being closed by ShutdownAll");
    }
    proxyBucket.getPartitionedRegion().checkReadiness();
  }

  public boolean isRecovering() {
    return this.recovering;
  }
  
  @Override
  protected void beginWaitingForMembershipChange(
      Set<PersistentMemberID> membersToWaitFor) {
    if(recovering) {
      bucketLock.unlock();
    } else {
      if(membersToWaitFor != null && !membersToWaitFor.isEmpty()) {
        String message = LocalizedStrings.PartitionedRegionDataStore_DATA_OFFLINE_MESSAGE.toLocalizedString(proxyBucket.getPartitionedRegion().getFullPath(), proxyBucket.getBucketId(), membersToWaitFor);
        throw new PartitionOfflineException((Set)membersToWaitFor, message);
      }
    }
  }
  
  @Override
  protected void logWaitingForMember(Set<PersistentMemberID> allMembersToWaitFor, Set<PersistentMemberID> offlineMembersToWaitFor) {
    //We only log the bucket level information at fine level.
    if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR)) {
      Set<String> membersToWaitForPrettyFormat = new HashSet<String>(); 

      if(offlineMembersToWaitFor != null && !offlineMembersToWaitFor.isEmpty()) {
        TransformUtils.transform(offlineMembersToWaitFor, membersToWaitForPrettyFormat, TransformUtils.persistentMemberIdToLogEntryTransformer);
        logger.info(LogMarker.PERSIST_ADVISOR, LocalizedMessage.create(LocalizedStrings.BucketPersistenceAdvisor_WAITING_FOR_LATEST_MEMBER,
            new Object[] {proxyBucket.getPartitionedRegion().getFullPath(), proxyBucket.getBucketId(), TransformUtils.persistentMemberIdToLogEntryTransformer.transform(getPersistentID()),
                membersToWaitForPrettyFormat}));
      } else {
        TransformUtils.transform(allMembersToWaitFor, membersToWaitForPrettyFormat, TransformUtils.persistentMemberIdToLogEntryTransformer);
        if (logger.isDebugEnabled()) {
          logger.debug(LogMarker.PERSIST_ADVISOR, "All persistent members being waited on are online, but they have not yet initialized");
        }
        logger.info(LogMarker.PERSIST_ADVISOR, LocalizedMessage.create(LocalizedStrings.BucketPersistenceAdvisor_WAITING_FOR_LATEST_MEMBER, 
            new Object[] {proxyBucket.getPartitionedRegion().getFullPath(), proxyBucket.getBucketId(), TransformUtils.persistentMemberIdToLogEntryTransformer.transform(getPersistentID()),
                membersToWaitForPrettyFormat}));
      }
    }
  }

  @Override
  protected void endWaitingForMembershipChange() {
    if(recovering) {
      bucketLock.lock();
      //We allow regions with persistent colocated children to exceed redundancy
      //so that we can create the child bucket. Otherwise, we need to check
      //that redundancy has not already been met now that we've got the dlock
      //the delock.
      if(!proxyBucket.hasPersistentChildRegion() 
          && !proxyBucket.checkBucketRedundancyBeforeGrab(null, false)) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR)) {
          logger.debug(LogMarker.PERSIST_ADVISOR, "{}-{}: After reacquiring dlock, we detected that redundancy is already satisfied",
              shortDiskStoreId(), regionPath);
        }
        //Remove the persistent data for this bucket, since
        //redundancy is already satisfied.
        proxyBucket.destroyOfflineData();
        throw new RedundancyAlreadyMetException();
      }
    }
  }
  
  @Override
  public void updateMembershipView(InternalDistributedMember replicate,
      boolean targetReinitializing) {
    if(recovering) {
      super.updateMembershipView(replicate, targetReinitializing);
      someMemberRecoveredLatch.countDown();
    } else {
      //don't update the membership view, we already updated it during recovery.
    }
  }

  /**
   * Wait for there to be initialized copies of this bucket. Get
   * the latest membership view from those copies.
   * 
   */
  public void initializeMembershipView() {
    MembershipChangeListener listener = new MembershipChangeListener();
    addListener(listener);
    boolean interrupted = false;
    try {
      while(!isClosed) {
        advisor.getAdvisee().getCancelCriterion().checkCancelInProgress(null);

        //Look for any online copies of the bucket.
        //If there are any, get a membership view from them.
        Map<InternalDistributedMember, PersistentMemberID> onlineMembers = advisor.adviseInitializedPersistentMembers();
        if(onlineMembers != null) {
          if(updateMembershipView(onlineMembers.keySet())) {
            break;
          }
        }
        
        Set<InternalDistributedMember> postRecoveryMembers = ((BucketAdvisor)advisor).adviseRecoveredFromDisk();
        if(postRecoveryMembers != null) {
          if(updateMembershipView(postRecoveryMembers)) {
            break;
          }
        }
        
        Set<PersistentMemberID> membersToWaitFor = getPersistedMembers();
        
        if(!membersToWaitFor.isEmpty()) {
          try {
            listener.waitForChange(membersToWaitFor, membersToWaitFor);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        } else {
          beginUpdatingPersistentView();
          break;
        }
      }
    } finally {
      removeListener(listener);
      if(interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
  
  private boolean updateMembershipView(Collection<InternalDistributedMember> targets) {
    for(InternalDistributedMember target: targets) {
      try {
        updateMembershipView(target, false);
        return true;
      } catch (ReplyException e) {
        //The member left?
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
  
  public boolean acquireTieLock() {
    //We don't actually need to get a dlock here for PRs, we're already
    //holding the bucket lock when we create a bucket region
    return true;
  }
  
  public void releaseTieLock() {
    //We don't actually need to get a dlock here for PRs, we're already
    //holding the bucket lock when we create a bucket region
  }
  
  
  @Override
  protected String getRegionPathForOfflineMembers() {
    return proxyBucket.getPartitionedRegion().getFullPath();
  }

  @Override
  public Set<PersistentMemberID> getMissingMembers() {
    if(recovering) {
      return super.getMissingMembers();
    } else {
      Set<PersistentMemberID> offlineMembers = getPersistedMembers();
      offlineMembers.removeAll(advisor.advisePersistentMembers().values());
      return offlineMembers;
    }
  }
  
  @Override
  public PersistentMemberID generatePersistentID() {
    PersistentMemberID id = storage.generatePersistentID();
    if(id == null) {
      return id;
    } else {
      id = new PersistentMemberID(id.diskStoreId, id.host, id.directory, this.proxyBucket
          .getPartitionedRegion().getBirthTime(), version++);
      return id;
    }
  }



  private static class RecoveryListener extends PersistentStateAdapter {
    private Set<PersistentMemberID> removedMembers = Collections.synchronizedSet(new HashSet<PersistentMemberID>());

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
   * @param infoMsg
   */
  public void dump(String infoMsg) {
    storage.getOnlineMembers();
    storage.getOfflineMembers();
    storage.getOfflineAndEqualMembers();
    storage.getMyInitializingID();
    storage.getMyPersistentID();
    final StringBuilder buf = new StringBuilder(2000);
    if (infoMsg != null) {
      buf.append(infoMsg);
      buf.append(": ");
    }
    buf.append("\nMY PERSISTENT ID:\n");
    buf.append(storage.getMyPersistentID());
    buf.append("\nMY INITIALIZING ID:\n");
    buf.append(storage.getMyInitializingID());
    
    buf.append("\nONLINE MEMBERS:\n");
    for (PersistentMemberID id : storage.getOnlineMembers()) {
      buf.append("\t");
      buf.append(id);
      buf.append("\n");
    }
    
    buf.append("\nOFFLINE MEMBERS:\n");
    for (PersistentMemberID id : storage.getOfflineMembers()) {
      buf.append("\t");
      buf.append(id);
      buf.append("\n");
    }
    
    buf.append("\nOFFLINE AND EQUAL MEMBERS:\n");
    for (PersistentMemberID id : storage.getOfflineAndEqualMembers()) {
      buf.append("\t");
      buf.append(id);
      buf.append("\n");
    }
    logger.debug(buf.toString());
  }

  /**
   * Wait for this bucket to be recovered from disk, at least to the point
   * where it starts doing a GII.
   * 
   * This method will throw an exception if the recovery thread encountered
   * an exception.
   */
  public void waitForPrimaryPersistentRecovery() {
    boolean interupted = false;
    while(true) {
      try {
        someMemberRecoveredLatch.await();
        break;
      } catch (InterruptedException e) {
        interupted = true;
      }
    }
    
    if(interupted) {
      Thread.currentThread().interrupt();
    }
    
    if(recoveryException != null) {
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
   * Overridden to fix bug 41336. We defer initialization of
   * this member until after the atomic bucket creation phase is over.
   */
  @Override
  public void setInitializing(PersistentMemberID newId) {
    if(atomicCreation) {
      if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR)) {
        logger.debug(LogMarker.PERSIST_ADVISOR, "{}-{}: {} Deferring setInitializing until the EndBucketCreation phase for {}",
            shortDiskStoreId(), regionPath, regionPath, newId);
      }
    } else {
      super.setInitializing(newId);
    }
  }

  /**
   * Overridden to fix bug 41336. We defer initialization of
   * this member until after the atomic bucket creation phase is over.
   */
  @Override
  public void setOnline(boolean didGII, boolean wasAtomicCreation, PersistentMemberID newId)
      throws ReplyException {
    //This is slightly confusing. If we're currently in the middle of an atomic
    //creation, we will do nothing right now. Later, when endBucketCreation
    //is called, we will pass the "wasAtomicCreation" flag down to the super
    //class to ensure that it knows its coming online as part of an atomic creation.
    if(this.atomicCreation) {
      if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR)) {
        logger.debug(LogMarker.PERSIST_ADVISOR, "{}-{}: {} Deferring setOnline until the EndBucketCreation phase for {}",
            shortDiskStoreId(), regionPath, regionPath, newId);
      }
    } else {
      super.setOnline(didGII, wasAtomicCreation, newId);
    }
  }

  /** Finish the atomic creation of this bucket on multiple members
   * This method is called with the proxy bucket synchronized. 
   */
  public void endBucketCreation(PersistentMemberID newId) {
    synchronized(lock) {
      if(!atomicCreation) {
        if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR)) {
          logger.debug(LogMarker.PERSIST_ADVISOR, "{}-{}: {} In endBucketCreation - already online, skipping (possible concurrent endBucketCreation)",
              shortDiskStoreId(), regionPath, regionPath);
        }
        return;
      }

      if (logger.isDebugEnabled(LogMarker.PERSIST_ADVISOR)) {
        logger.debug(LogMarker.PERSIST_ADVISOR, "{}-{}: {} In endBucketCreation - now persisting the id {}",
            shortDiskStoreId(), regionPath, regionPath, newId);
      }
      atomicCreation = false;
    }
    super.setOnline(false, true, newId);
  }

  public void setAtomicCreation(boolean atomicCreation) {
    synchronized(lock) {
      this.atomicCreation = atomicCreation;
    }
  }
  
  private BucketPersistenceAdvisor getColocatedPersistenceAdvisor() {
    PartitionedRegion colocatedRegion = ColocationHelper
        .getColocatedRegion(proxyBucket.getPartitionedRegion());
    if(colocatedRegion == null) {
      return null;
    }
    ProxyBucketRegion colocatedProxyBucket = colocatedRegion.getRegionAdvisor()
        .getProxyBucketArray()[proxyBucket.getBucketId()];
    return colocatedProxyBucket.getPersistenceAdvisor();
  }
}
