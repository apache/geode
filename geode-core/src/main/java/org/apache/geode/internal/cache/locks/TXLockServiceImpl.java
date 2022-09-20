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

package org.apache.geode.internal.cache.locks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.LockGrantorId;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;
import org.apache.geode.logging.internal.log4j.api.LogService;

/** Provides clean separation of implementation from public facade */
public class TXLockServiceImpl extends TXLockService {
  private static final Logger logger = LogService.getLogger();

  /** Transaction lock requests never timeout */
  private static final long TIMEOUT_MILLIS = -1;

  /** Transaction lock leases never expire */
  private static final long LEASE_MILLIS = -1;

  // -------------------------------------------------------------------------
  // Constructor and instance variables
  // -------------------------------------------------------------------------

  /** Instance of dlock service to use */
  private final DLockService dlock;

  /**
   * List of active txLockIds
   */
  protected final List<TXLockId> txLockIdList = new ArrayList<>();

  /**
   * True if grantor recovery is in progress; used to keep <code>release</code> from waiting for
   * grantor.
   */
  private volatile boolean recovering = false;

  /**
   * Read locks are held while any <code>txLock</code> is held; write lock is held during grantor
   * recovery.
   */
  private final StoppableReentrantReadWriteLock recoveryLock;

  /** The distributed system for cancellation checks. */
  private final InternalDistributedSystem system;

  @VisibleForTesting
  TXLockServiceImpl(InternalDistributedSystem sys, StoppableReentrantReadWriteLock recoveryLock,
      DLockService dlock) {
    system = sys;
    this.recoveryLock = recoveryLock;
    this.dlock = dlock;
  }

  TXLockServiceImpl(String name, InternalDistributedSystem sys) {
    if (sys == null) {
      throw new IllegalStateException(
          "TXLockService cannot be created until connected to distributed system.");
    }
    sys.getCancelCriterion().checkCancelInProgress(null);
    system = sys;

    recoveryLock = new StoppableReentrantReadWriteLock(sys.getCancelCriterion());

    dlock = (DLockService) DLockService.create(name, sys, true, true);

    dlock.setDLockRecoverGrantorMessageProcessor(new TXRecoverGrantorMessageProcessor());

    dlock.setDLockLessorDepartureHandler(new TXLessorDepartureHandler());
  }

  // -------------------------------------------------------------------------
  // Instance methods
  // -------------------------------------------------------------------------

  @Override
  public boolean isLockGrantor() {
    return dlock.isLockGrantor();
  }

  @Override
  public void becomeLockGrantor() {
    dlock.becomeLockGrantor();
  }

  @Override
  public TXLockId txLock(List regionLockReqs, Set txParticipants) throws CommitConflictException {
    if (regionLockReqs == null) {
      throw new IllegalArgumentException(
          "regionLockReqs must not be null");
    }
    Set participants = txParticipants;
    if (participants == null) {
      participants = Collections.EMPTY_SET;
    }

    boolean gotLocks = false;
    TXLockId txLockId = null;
    try {
      synchronized (txLockIdList) {
        txLockId = new TXLockIdImpl(dlock.getDistributionManager().getId());
        txLockIdList.add(txLockId);
      }
      TXLockBatch batch = new TXLockBatch(txLockId, regionLockReqs, participants);

      logger.debug("[TXLockServiceImpl.txLock] acquire try-locks for {}", batch);

      // TODO: get a readWriteLock to make the following block atomic...
      Object[] keyIfFail = new Object[1];
      gotLocks = dlock.acquireTryLocks(batch, TIMEOUT_MILLIS, LEASE_MILLIS, keyIfFail);
      if (gotLocks) { // ...otherwise race can occur between tryLocks and readLock
        acquireRecoveryReadLock();
      } else if (keyIfFail[0] != null) {
        synchronized (txLockIdList) {
          txLockIdList.remove(txLockId);
        }
        throw new CommitConflictException(
            String.format("Concurrent transaction commit detected %s",
                keyIfFail[0]));
      } else {
        synchronized (txLockIdList) {
          txLockIdList.remove(txLockId);
        }
        throw new CommitConflictException(
            String.format("Failed to request try locks from grantor: %s",
                dlock.getLockGrantorId()));
      }

      logger.debug("[TXLockServiceImpl.txLock] gotLocks is {}, returning txLockId:{}", gotLocks,
          txLockId);
      return txLockId;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.debug("[TXLockServiceImpl.txLock] was interrupted", e);
      if (gotLocks) {
        synchronized (txLockIdList) {
          txLockIdList.remove(txLockId);
        }
      }
      // TODO: change to be TransactionFailedException (after creating TFE)...
      throw new CommitConflictException(
          "Concurrent transaction commit detected because request was interrupted.",
          e);
    }
  }

  @Override
  public void updateParticipants(TXLockId txLockId, final Set updatedParticipants) {
    synchronized (txLockIdList) {
      if (!txLockIdList.contains(txLockId)) {
        IllegalArgumentException e = new IllegalArgumentException(
            String.format("Invalid txLockId not found: %s",
                txLockId));
        system.getDistributionManager().getCancelCriterion().checkCancelInProgress(e);
        Cache cache = system.getCache();
        if (cache != null) {
          cache.getCancelCriterion().checkCancelInProgress(e);
        }
        throw e;
      }
    }
    if (updatedParticipants == null) {
      throw new IllegalArgumentException(
          "Invalid updatedParticipants, null");
    }
    if (updatedParticipants.isEmpty()) {
      return;
    }
    if (!recovering) { // not recovering
      // Serializable grantorId = this.dlock.getLockGrantorId();
      if (dlock.isLockGrantor()) {
        // Don't need to send a message (or wait for a reply)
        // logger.info("DEBUG: [TXLockServiceImpl.updateParticipants] this VM is the Grantor");
        TXLockUpdateParticipantsMessage.updateParticipants(dlock, txLockId,
            updatedParticipants);
      } else { // not lock grantor
        LockGrantorId lockGrantorId = txLockId.getLockGrantorId();
        if (lockGrantorId == null || !dlock.isLockGrantorId(lockGrantorId)) {
          return; // grantor is gone so we cannot update it
        }
        InternalDistributedMember grantorId = lockGrantorId.getLockGrantorMember();

        // logger.info("DEBUG: [TXLockServiceImpl.updateParticipants] sending update message to
        // Grantor " + grantorId);
        ReplyProcessor21 processor =
            new ReplyProcessor21(dlock.getDistributionManager(), grantorId);
        TXLockUpdateParticipantsMessage dlup = new TXLockUpdateParticipantsMessage(txLockId,
            dlock.getName(), updatedParticipants, processor.getProcessorId());
        dlup.setRecipient(grantorId);
        dlock.getDistributionManager().putOutgoing(dlup);
        // for() loop removed for bug 36983 - you can't loop on waitForReplies()
        dlock.getDistributionManager().getCancelCriterion().checkCancelInProgress(null);
        try {
          processor.waitForRepliesUninterruptibly();
        } catch (ReplyException e) {
          e.handleCause();
        }
      } // not lock grantor
    } // not recovering
  }

  @Override
  public void release(TXLockId txLockId) {
    synchronized (txLockIdList) {
      if (!txLockIdList.contains(txLockId)) {
        // TXLockService.destroyServices can be invoked in cache.close().
        // Other P2P threads could process message such as TXCommitMessage afterwards,
        // and invoke TXLockService.createDTLS(). It could create a new TXLockService
        // which will have a new empty list (txLockIdList) and it will not
        // contain the originally added txLockId
        throw new IllegalArgumentException(
            String.format("Invalid txLockId not found: %s",
                txLockId));
      }

      dlock.releaseTryLocks(txLockId, () -> recovering);

      txLockIdList.remove(txLockId);
      releaseRecoveryReadLock();
    }
  }

  @Override
  public boolean isDestroyed() {
    return dlock.isDestroyed() || system.getCancelCriterion().isCancelInProgress();
  }

  // -------------------------------------------------------------------------
  // Internal implementation methods
  // -------------------------------------------------------------------------

  boolean isRecovering() {
    return recovering;
  }

  /** Delays grantor recovery replies until finished with locks */
  void acquireRecoveryWriteLock() throws InterruptedException {
    recovering = true;
    recoveryLock.writeLock().lockInterruptibly();
  }

  void releaseRecoveryWriteLock() {
    recoveryLock.writeLock().unlock();
    recovering = false;
  }

  private void acquireRecoveryReadLock() throws InterruptedException {
    recoveryLock.readLock().lockInterruptibly();
  }

  private void releaseRecoveryReadLock() {
    recoveryLock.readLock().unlock();
  }

  /** Exposes the internal dlock service - Public for testing purposes */
  public DLockService getInternalDistributedLockService() {
    return dlock;
  }

  /** Destroys the underlying lock service in this member */
  @Override
  void basicDestroy() {
    dlock.destroyAndRemove();
  }

  @VisibleForTesting
  public int getTxLockIdList() {
    return this.txLockIdList.size();
  }

}
