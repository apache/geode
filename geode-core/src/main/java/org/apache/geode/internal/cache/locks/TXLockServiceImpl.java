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

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.LockGrantorId;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;

/** Provides clean separation of implementation from public facade */
public class TXLockServiceImpl extends TXLockService {
  private static final Logger logger = LogService.getLogger();

  /** Transaction lock requests never timeout */
  private static long TIMEOUT_MILLIS = -1;

  /** Transaction lock leases never expire */
  private static long LEASE_MILLIS = -1;

  // -------------------------------------------------------------------------
  // Constructor and instance variables
  // -------------------------------------------------------------------------

  /** Instance of dlock service to use */
  private DLockService dlock;

  /** List of active txLockIds */
  protected List txLockIdList = new ArrayList();

  /**
   * True if grantor recovery is in progress; used to keep <code>release</code> from waiting for
   * grantor. TODO: this boolean can probably be removed... it was insufficient and new fixes for
   * bug 38763 have the side effect of making this boolean obsolete (verify before removal!)
   */
  private volatile boolean recovering = false;

  /**
   * Read locks are held while any <code>txLock</code> is held; write lock is held during grantor
   * recovery.
   */
  private final StoppableReentrantReadWriteLock recoveryLock;

  /** The distributed system for cancellation checks. */
  private final InternalDistributedSystem system;

  /** Constructs new instance of transaction lock service */
  TXLockServiceImpl(String name) {
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys == null) {
      throw new IllegalStateException(
          LocalizedStrings.TXLockServiceImpl_TXLOCKSERVICE_CANNOT_BE_CREATED_UNTIL_CONNECTED_TO_DISTRIBUTED_SYSTEM
              .toLocalizedString());
    }
    sys.getCancelCriterion().checkCancelInProgress(null);
    this.system = sys;

    this.recoveryLock = new StoppableReentrantReadWriteLock(sys.getCancelCriterion());

    this.dlock = (DLockService) DLockService.create(name, sys, true /* distributed */,
        true /* destroyOnDisconnect */, true /* automateFreeResources */);

    this.dlock.setDLockRecoverGrantorMessageProcessor(new TXRecoverGrantorMessageProcessor());

    this.dlock.setDLockLessorDepartureHandler(new TXLessorDepartureHandler());
  }

  // -------------------------------------------------------------------------
  // Instance methods
  // -------------------------------------------------------------------------

  @Override
  public boolean isLockGrantor() {
    return this.dlock.isLockGrantor();
  }

  @Override
  public void becomeLockGrantor() {
    this.dlock.becomeLockGrantor();
  }

  @Override
  public TXLockId txLock(List regionLockReqs, Set txParticipants) throws CommitConflictException {
    if (regionLockReqs == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.TXLockServiceImpl_REGIONLOCKREQS_MUST_NOT_BE_NULL.toLocalizedString());
    }
    Set participants = txParticipants;
    if (participants == null) {
      participants = Collections.EMPTY_SET;
    }

    boolean gotLocks = false;
    TXLockId txLockId = null;
    try {
      synchronized (this.txLockIdList) {
        txLockId = new TXLockIdImpl(this.dlock.getDistributionManager().getId());
        this.txLockIdList.add(txLockId);
      }
      TXLockBatch batch = new TXLockBatch(txLockId, regionLockReqs, participants);

      logger.debug("[TXLockServiceImpl.txLock] acquire try-locks for {}", batch);

      // TODO: get a readWriteLock to make the following block atomic...
      Object[] keyIfFail = new Object[1];
      gotLocks = this.dlock.acquireTryLocks(batch, TIMEOUT_MILLIS, LEASE_MILLIS, keyIfFail);
      if (gotLocks) { // ...otherwise race can occur between tryLocks and readLock
        acquireRecoveryReadLock();
      } else if (keyIfFail[0] != null) {
        throw new CommitConflictException(
            LocalizedStrings.TXLockServiceImpl_CONCURRENT_TRANSACTION_COMMIT_DETECTED_0
                .toLocalizedString(keyIfFail[0]));
      } else {
        throw new CommitConflictException(
            LocalizedStrings.TXLockServiceImpl_FAILED_TO_REQUEST_TRY_LOCKS_FROM_GRANTOR_0
                .toLocalizedString(this.dlock.getLockGrantorId()));
      }

      logger.debug("[TXLockServiceImpl.txLock] gotLocks is {}, returning txLockId:{}", gotLocks,
          txLockId);
      return txLockId;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.debug("[TXLockServiceImpl.txLock] was interrupted", e);
      if (gotLocks) {
        if (txLockId != null) {
          synchronized (this.txLockIdList) {
            this.txLockIdList.remove(txLockId);
          }
        }
        gotLocks = false;
      }
      // TODO: change to be TransactionFailedException (after creating TFE)...
      throw new CommitConflictException(
          LocalizedStrings.TXLockServiceImpl_CONCURRENT_TRANSACTION_COMMIT_DETECTED_BECAUSE_REQUEST_WAS_INTERRUPTED
              .toLocalizedString(),
          e);
    }
  }

  @Override
  public void updateParticipants(TXLockId txLockId, final Set updatedParticipants) {
    synchronized (this.txLockIdList) {
      if (!this.txLockIdList.contains(txLockId)) {
        throw new IllegalArgumentException(
            LocalizedStrings.TXLockServiceImpl_INVALID_TXLOCKID_NOT_FOUND_0
                .toLocalizedString(txLockId));
      }
    }
    if (updatedParticipants == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.TXLockServiceImpl_INVALID_UPDATEDPARTICIPANTS_NULL.toLocalizedString());
    }
    if (updatedParticipants.isEmpty()) {
      return;
    }
    if (!this.recovering) { // not recovering
      // Serializable grantorId = this.dlock.getLockGrantorId();
      if (this.dlock.isLockGrantor()) {
        // Don't need to send a message (or wait for a reply)
        // logger.info("DEBUG: [TXLockServiceImpl.updateParticipants] this VM is the Grantor");
        TXLockUpdateParticipantsMessage.updateParticipants(this.dlock, txLockId,
            updatedParticipants);
      } else { // not lock grantor
        LockGrantorId lockGrantorId = txLockId.getLockGrantorId();
        if (lockGrantorId == null || !this.dlock.isLockGrantorId(lockGrantorId)) {
          return; // grantor is gone so we cannot update him
        }
        InternalDistributedMember grantorId = lockGrantorId.getLockGrantorMember();

        // logger.info("DEBUG: [TXLockServiceImpl.updateParticipants] sending update message to
        // Grantor " + grantorId);
        ReplyProcessor21 processor =
            new ReplyProcessor21(this.dlock.getDistributionManager(), grantorId);
        TXLockUpdateParticipantsMessage dlup = new TXLockUpdateParticipantsMessage(txLockId,
            this.dlock.getName(), updatedParticipants, processor.getProcessorId());
        dlup.setRecipient(grantorId);
        this.dlock.getDistributionManager().putOutgoing(dlup);
        // for() loop removed for bug 36983 - you can't loop on waitForReplies()
        this.dlock.getDistributionManager().getCancelCriterion().checkCancelInProgress(null);
        try {
          processor.waitForRepliesUninterruptibly();
        } catch (ReplyException e) {
          e.handleAsUnexpected();
        }
      } // not lock grantor
    } // not recovering
  }

  @Override
  public void release(TXLockId txLockId) {
    synchronized (this.txLockIdList) {
      if (!this.txLockIdList.contains(txLockId)) {
        // TXLockService.destroyServices can be invoked in cache.close().
        // Other P2P threads could process message such as TXCommitMessage afterwards,
        // and invoke TXLockService.createDTLS(). It could create a new TXLockService
        // which will have a new empty list (txLockIdList) and it will not
        // contain the originally added txLockId
        throw new IllegalArgumentException(
            LocalizedStrings.TXLockServiceImpl_INVALID_TXLOCKID_NOT_FOUND_0
                .toLocalizedString(txLockId));
      }
      // only release w/ dlock if not in middle of recovery...
      if (!this.recovering) {
        this.dlock.releaseTryLocks(txLockId, true);
      }
      this.txLockIdList.remove(txLockId);
      releaseRecoveryReadLock();
    }
  }

  @Override
  public boolean isDestroyed() {
    return this.dlock.isDestroyed() || this.system.getCancelCriterion().isCancelInProgress();
  }

  // -------------------------------------------------------------------------
  // Internal implementation methods
  // -------------------------------------------------------------------------

  /** Delays grantor recovery replies until finished with locks */
  void acquireRecoveryWriteLock() throws InterruptedException {
    this.recoveryLock.writeLock().lockInterruptibly();
    this.recovering = true;
  }

  void releaseRecoveryWriteLock() {
    this.recoveryLock.writeLock().unlock();
    this.recovering = false;
  }

  private void acquireRecoveryReadLock() throws InterruptedException {
    this.recoveryLock.readLock().lockInterruptibly();
  }

  private void releaseRecoveryReadLock() {
    this.recoveryLock.readLock().unlock();
  }

  /** Exposes the internal dlock service - Public for testing purposes */
  public DLockService getInternalDistributedLockService() {
    return this.dlock;
  }

  /** Destroys the underlying lock service in this member */
  @Override
  void basicDestroy() {
    this.dlock.destroyAndRemove();
  }

}

