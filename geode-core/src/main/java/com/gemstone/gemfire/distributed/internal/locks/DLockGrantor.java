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

package com.gemstone.gemfire.distributed.internal.locks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.LockServiceDestroyedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.locks.DLockQueryProcessor.DLockQueryMessage;
import com.gemstone.gemfire.distributed.internal.locks.DLockRequestProcessor.DLockRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.IdentityArrayList;
import com.gemstone.gemfire.internal.cache.TXReservationMgr;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.util.concurrent.StoppableCountDownLatch;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantReadWriteLock;

/**
 * Provides lock grantor authority to a distributed lock service. This is
 * responsible for granting, releasing, and timing out locks as well as
 * exposing hooks for recovery or transfer of lock grantor.
 * <p>
 * ReadWriteLocks are not currently handled by grantor recovery or transfer.
 *
 * @author Kirk Lund
 * @author Darrel Schneider
 */
@SuppressWarnings("unchecked")
public class DLockGrantor {
  private static final Logger logger = LogService.getLogger();
  
  public static final boolean DEBUG_SUSPEND_LOCK = // TODO:LOG:CONVERT: REMOVE THIS
      Boolean.getBoolean(
          "gemfire.DLockService.DLockGrantor.debugSuspendLock"); 
  
  /** 
   * Default wait before grantor thread will reawaken to check for expirations
   * and timeouts.
   */
  public static final long GRANTOR_THREAD_MAX_WAIT = DLockGrantorThread.MAX_WAIT;

  /**
   * Newly constructed grantor is INITIALIZING. All lock requests and related
   * messages will block.
   */
  private static final int INITIALIZING = 0;  // msgs will block until READY
  
  /**
   * Grantor is READY and handling lock requests.
   */
  private static final int READY = 1;         // msgs will be processed
  
  /**
   * Grantor is DESTROYED and will respond as NOT_GRANTOR to any requests.
   */
  private static final int DESTROYED = 5;     // NOT_GRANTOR
  
  /** 
   * DistributedLockService that this grantor is granting locks for.
   */
  protected final DLockService dlock;
  
  /**
   * Map of grant tokens for tracking grantor-side state of distributed locks.
   * Key: Object name, Value: DLockGrantToken grant
   * 
   * @guarded.By grantTokens
   */
  private final Map grantTokens = new HashMap();

  /** 
   * Dedicated thread responsible for handling expirations and timeouts. 
   */
  protected final DLockGrantorThread thread;
  
  /** 
   * The current state of this grantor. Volatile for read access. Mutation
   * must occur while synchronized on this grantor.
   * <p>
   * State starts out as INITIALIZING and moves to READY at which point the
   * grantor will start servicing requests.
   * <p>
   * If a newcomer requests transfer of grantorship, then the state will
   * change to HALTED and then finally become DESTROYED when transfer is
   * complete. During HALTED, the elder recognizes the newcomer as the current
   * grantor.
   * <p>
   * If this service is shutting down, it will seek out a successor to become 
   * the new grantor. During this phase, the state will be PENDING_SHUTDOWN which means
   * no requests or releases will be replied to until a successor is found,
   * registers transfer intentions with the elder and sends this process a
   * transfer grantorship request. At that point the state will be HALTED as
   * described above.
   * 
   * @guarded.By this
   */
  private volatile int state = INITIALIZING;

  /** 
   * ReadWriteLock to protect in-progress operations from destroying service. 
   */
  private final StoppableReentrantReadWriteLock destroyLock;
  
  /** 
   * Specialized lock information for Transaction lock batches.
   * <p>
   * Key: Object batchId, Value: DLockBatch batch
   * <p>
   * Handling of batch locks synchronizes on this to assure serial processing.
   * 
   * @guarded.By batchLocks
   */
  private final Map batchLocks = new HashMap();
  
  /** 
   * Handles special lock-reservation type for transactions.
   */
  private final TXReservationMgr resMgr = new TXReservationMgr(false);

  /** 
   * Enforces waiting until this grantor is initialized. Used to block all
   * lock requests until INITIALIZED. 
   */
  private final StoppableCountDownLatch whileInitializing;

  /** 
   * Enforces waiting until this grantor is destroyed. Used to block all
   * lock requests while destroying. Latch opens after state becomes DESTROYED
   * and grantor begins replying with NOT_GRANTOR. 
   */
  private final StoppableCountDownLatch untilDestroyed;
  
  /** 
   * If -1 then it has not yet been fetched from elder. Otherwise it is the 
   * versionId that the elder gave us. During explicit becomeGrantor, the
   * value is -1, and then the elder provides a real versionId. 
   */
  private final AtomicLong versionId = new AtomicLong(-1);
  
  /** 
   * Used to verify that requestor member is still in view when granting. 
   */
  protected final DM dm;
  
  // -------------------------------------------------------------------------
  //   SuspendLocking state (BEGIN)
  
  /** 
   * Synchronization for protecting all SuspendLocking state. Guards all
   * suspend locking state. 
   */
  protected final Object suspendLock = new Object();

  /** 
   * Identifies remote thread that has currently suspended locking or null.
   * 
   * Concurrency: protected by synchronization of {@link #suspendLock}
   */
  protected RemoteThread lockingSuspendedBy = null;
  
  /**
   * Value indicates nonexistent lock
   */
  protected static final int INVALID_LOCK_ID = -1;

  /** 
   * Identifies the lockId used by the remote thread to suspend locking. 
   * 
   * Concurrency: protected by synchronization of {@link #suspendLock}
   */
  protected int suspendedLockId = INVALID_LOCK_ID;
  
  /** 
   * FIFO queue for suspend read and write lock waiters.
   * 
   * @guarded.By {@link #suspendLock}
   */
  private final LinkedList suspendQueue = new LinkedList();
  
  /**
   * Map of read lock counts for each RemoteThread currently holding locks.
   * <p> 
   * Key=RemoteThread, Value=ReadLockCount 
   * 
   * @guarded.By {@link #suspendLock}
   */
  private final HashMap readLockCountMap = new HashMap();
  
  /** 
   * Number of suspend waiters waiting for write lock. 
   * 
   * @guarded.By {@link #suspendLock}
   */
  private int writeLockWaiters = 0;
  
  /** 
   * Total number of read locks held against suspend write lock. 
   * 
   * @guarded.By {@link #suspendLock}
   */
  private int totalReadLockCount = 0;
  
  /** 
   * List of next requests to process after handling an unlock or resume. 
   * 
   * @guarded.By {@link #suspendLock}
   */
  private ArrayList permittedRequests = new ArrayList();

  /** 
   * List of active drains of permittedRequests. TODO: does this need to be
   * a synchronizedList?
   * 
   * @guarded.By {@link #suspendLock}
   */
  private final List permittedRequestsDrain = 
      Collections.synchronizedList(new LinkedList());
  
  //   SuspendLocking state (END)
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  //   Static methods
  // -------------------------------------------------------------------------
  
  /**
   * Creates new instance of grantor for the lock service.
   *
   * @param dlock the lock service the grantor is authority for
   * @param versionId the version, from the elder, of this grantor
   * @return new instance of grantor for the lock service
   */
  static DLockGrantor createGrantor(DLockService dlock,
                                    long versionId) {
    return new DLockGrantor(dlock, versionId);
  }

  /**
   * Gets the local instance of grantor for the lock service if one exists.
   *
   * @param dlock the lock service the grantor is authority for
   * @return instance of grantor for the lock service or null if no local 
   * grantor has been created
   */
  static DLockGrantor getGrantorForService(DLockService dlock) {
    if (dlock == null) return null;
    return dlock.getGrantor();
  }

  /** 
   * Returns instance of DLockGrantor that will handle distributed lock
   * granting for specified service.
   * <p> 
   * Returns null if unable to get ready grantor or if this process is not the 
   * grantor for the service.
   *
   * @param svc the lock service to return the grantor instance for
   */
  public static DLockGrantor waitForGrantor(DLockService svc) throws InterruptedException {
    if (svc == null) return null;
    // now we need to get the grantor and make sure it's ready...
    DLockGrantor oldGrantor = null;
    DLockGrantor grantor = getGrantorForService(svc);
    do {
      if (grantor == null || grantor.isDestroyed()) return null;
      grantor.waitWhileInitializing();
      if (svc.isDestroyed()) return null;
      // make sure we are lock grantor
      if (!svc.isCurrentlyOrIsMakingLockGrantor()) return null;
      if (!grantor.isReady()) return null;
      // Now make sure the service still has this guy as its grantor
      oldGrantor = grantor;
      grantor = getGrantorForService(svc);
    } while (oldGrantor != grantor);
    return grantor;
  }
    
  // -------------------------------------------------------------------------
  //   Constructor
  // -------------------------------------------------------------------------
  
  /**
   * Creates instance of grantor for the lock service.
   *
   * @param dlock the lock service the grantor is authority for
   * @param vId unique id that the elder increments for each new grantor
   */
  private DLockGrantor(DLockService dlock, 
                       long vId) {
    this.dm = dlock.getDistributionManager();
    CancelCriterion stopper = this.dm.getCancelCriterion();
    this.whileInitializing = new StoppableCountDownLatch(stopper, 1);
    this.untilDestroyed = new StoppableCountDownLatch(stopper, 1);
    this.dlock = dlock;
    this.destroyLock = new StoppableReentrantReadWriteLock(stopper);
    this.versionId.set(vId);
    this.dm.addMembershipListener(this.membershipListener);
    this.thread = new DLockGrantorThread(this, stopper);
    this.dlock.getStats().incGrantors(1);
  }
  
  // -------------------------------------------------------------------------
  //   Public and package methods
  // -------------------------------------------------------------------------

  /**
   * Returns the grantor's version id which was assigned by an elder. Required
   * to help uniquely identify this grantor instance.
   * 
   * @return the grantor's version id which was assigned by an elder
   */
  public long getVersionId() {
    return this.versionId.get();
  }

  /**
   * Sets the version id after the elder tells us what it is. This is called
   * during the explicit become grantor process.
   * 
   * @param v the elder assigned version id for this grantor
   */
  public void setVersionId(long v) {
    this.versionId.set(v);
  }
  
  /** 
   * Waits uninterruptibly while this grantor is initializing. Returns when
   * grantor is ready to handle lock requests.
   * 
   * @throws DistributedSystemDisconnectedException if system shuts down before grantor is ready
   */
  public void waitWhileInitializing() throws InterruptedException {
    boolean interrupted = Thread.interrupted();
    try {
      if (interrupted && this.dlock.isInterruptibleLockRequest()) {
        throw new InterruptedException();
      }
      while (true) {
        try {
          this.whileInitializing.await();
          break;
        }
        catch (InterruptedException e) {
          interrupted = true;
          throwIfInterruptible(e);
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
  
  /** 
   * Waits uninterruptibly until this service is destroyed. Returns when
   * grantor has been completely destroyed.
   *  
   * @throws DistributedSystemDisconnectedException if system shuts down before grantor is destroyed
   */
  public void waitUntilDestroyed() throws InterruptedException {
    while (true) {
      boolean interrupted = Thread.interrupted();
      try {
        this.untilDestroyed.await();
        break;
      }
      catch (InterruptedException e) {
        interrupted = true;
        throwIfInterruptible(e);
      }
      finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }
  }
  
  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer(128);
    buffer.append('<')
      .append("DLockGrantor")
      .append("@")
      .append(Integer.toHexString(System.identityHashCode(this)))
      .append(" state=")
      .append(stateToString(this.state))
      .append(" name=")
      .append(this.dlock.getName())
      .append(" version=")
      .append(this.getVersionId())
      .append('>');
    return buffer.toString();
  }

  /**
   * Returns true if this grantor is ready to handle lock requests.
   * 
   * @return true if this grantor is ready to handle lock requests
   */
  boolean isReady() {
    return this.state == READY;
  }
  
  /**
   * Returns true if this grantor is still initializing and not yet ready
   * for lock requests.
   * 
   * @return true if this grantor is still initializing
   */
  boolean isInitializing() {
    return this.state == INITIALIZING;
  }
  
  /**
   * Returns true if this grantor has been destroyed.
   * 
   * @return true if this grantor has been destroyed
   */
  public boolean isDestroyed() {
    return this.state == DESTROYED;
  }
  
  /**
   * Throws LockGrantorDestroyedException if this grantor has been destroyed.
   *
   * @throws LockGrantorDestroyedException if grantor is destroyed
   */
  void checkDestroyed() {
    throwIfDestroyed(isDestroyed());
  }

  /**
   * Throws LockGrantorDestroyedException if destroyed is true.
   * 
   * @param destroyed if true then throw LockGrantorDestroyedException
   * @throws LockGrantorDestroyedException if destroyed is true
   */
  private void throwIfDestroyed(boolean destroyed) {
    if (destroyed) {
      throw new LockGrantorDestroyedException(LocalizedStrings.DLockGrantor_GRANTOR_IS_DESTROYED.toLocalizedString());
    }
  }
  
  /**
   * Handles request for a batch of locks using optimization for transactions.
   * <p>
   * Synchronizes on {@link #batchLocks}.
   * 
   * @throws LockGrantorDestroyedException if grantor is destroyed
   */
  void handleLockBatch(DLockRequestMessage request) throws InterruptedException {
    synchronized (this.batchLocks) { // assures serial processing
      waitWhileInitializing(); // calcWaitMillisFromNow
      if (request.checkForTimeout()) {
        cleanupSuspendState(request);
        return;
      }
      
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "[DLockGrantor.handleLockBatch]");
      }
      if (!acquireDestroyReadLock(0)) {
        waitUntilDestroyed();
        checkDestroyed();
      }
      try {
        checkDestroyed();
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.handleLockBatch] request: {}", request);
        }

        DLockBatch batch = (DLockBatch) request.getObjectName();
        this.resMgr.makeReservation((IdentityArrayList)batch.getReqs());
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.handleLockBatch] granting {}", batch.getBatchId());
        }
        this.batchLocks.put(batch.getBatchId(), batch);
        request.respondWithGrant(Long.MAX_VALUE);
//         // try-lock every lock in batch...
//         Object name = null;
//         Set lockNames = batch.getLockNames();
//         Set acquiredLocks = new HashSet();
//         long leaseExpireTime = -1;
        
//         for (Iterator iter = lockNames.iterator(); iter.hasNext();) {
//           name = iter.next();
//           DLockGrantToken grant = getOrCreateGrant(
//               this.dlock.getOrCreateToken(name));

//           // calc lease expire time just once...              
//           if (leaseExpireTime == -1) {
//             leaseExpireTime = grant.calcLeaseExpireTime(request.getLeaseTime());
//           }
          
//           // try to grant immediately else fail...
//           if (grant.grantBatchLock(request.getSender(), leaseExpireTime)) {
//             acquiredLocks.add(grant);
//           } else {
//             // fail out and release all..
//             break;
//           }
//         } // for-loop
        
//         if (acquiredLocks.size() == lockNames.size()) {
//           // got the locks!
//           logFine("[DLockGrantor.handleLockBatch] granting " + 
//               batch.getBatchId() + "; leaseExpireTime=" + leaseExpireTime);
          
//           // save the batch for later release...
//           this.batchLocks.put(batch.getBatchId(), batch);
//           request.respondWithGrant(leaseExpireTime);
//         }
//         else {
//           // failed... release them all...
//           for (Iterator iter = acquiredLocks.iterator(); iter.hasNext();) {
//             DLockGrantToken grant = (DLockGrantToken) iter.next();
//             grant.release();
//           }
//           request.respondWithTryLockFailed(name);
//         }
      }
      catch (CommitConflictException ex) {
        request.respondWithTryLockFailed(ex.getMessage());
      }
      finally {
        releaseDestroyReadLock();
      }
    }
  }
  
  /**
   * Returns transaction optimized lock batches that were created by the 
   * specified owner. 
   * <p>
   * Synchronizes on batchLocks.
   *  
   * @param owner member that owned the lock batches to return
   * @return lock batches that were created by owner
   */
  public DLockBatch[] getLockBatches(InternalDistributedMember owner) {
    // Key: Object batchId, Value: DLockBatch batch
    synchronized (this.batchLocks) {
      List batchList = new ArrayList();
      for (Iterator iter = this.batchLocks.values().iterator(); iter.hasNext();) {
        DLockBatch batch = (DLockBatch) iter.next();
        if (batch.getOwner().equals(owner)) {
          batchList.add(batch);
        }
      }
      return (DLockBatch[]) batchList.toArray(new DLockBatch[batchList.size()]);  
    }
  }

  /**  
   * Get the batch for the given batchId (for example use a txLockId from 
   * TXLockBatch in order to update its participants).  This operation was 
   * added as part of the solution to bug 32999. 
   * <p>
   * Acquires acquireDestroyReadLock. Synchronizes on batchLocks.
   * <p>
   * see com.gemstone.gemfire.internal.cache.TXCommitMessage#updateLockMembers()
   *  
   * @param batchId the identifier for the batch to retrieve
   * @return the transaction lock batch identified by the given batchId
   * @see com.gemstone.gemfire.internal.cache.locks.TXLockUpdateParticipantsMessage
   * @see com.gemstone.gemfire.internal.cache.locks.TXLockBatch#getBatchId()
   */
  public DLockBatch getLockBatch(Object batchId) throws InterruptedException {
    DLockBatch ret = null;
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS, "[DLockGrantor.getLockBatch] enter: {}", batchId);
    }
    synchronized(this.batchLocks) {
      waitWhileInitializing();      
      if (!acquireDestroyReadLock(0)) {
        waitUntilDestroyed();
        checkDestroyed();
      }
      try {
        checkDestroyed();
        ret = (DLockBatch) this.batchLocks.get(batchId);
      }
      finally {
        releaseDestroyReadLock();
      }
    }
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS, "[DLockGrantor.getLockBatch] exit: {}", batchId);
    }
    return ret;
  }

  /**  
   * Update the batch for the given batch. This operation was added as part of 
   * the solution to bug 32999. 
   * <p>
   * Acquires acquireDestroyReadLock. Synchronizes on batchLocks.
   * <p>
   * see com.gemstone.gemfire.internal.cache.locks.TXCommitMessage#updateLockMembers()
   * 
   * @param batchId the identify of the transaction lock batch
   * @param newBatch the new lock batch to be used
   * @see com.gemstone.gemfire.internal.cache.locks.TXLockUpdateParticipantsMessage
   * @see com.gemstone.gemfire.internal.cache.locks.TXLockBatch#getBatchId()
   */
  public void updateLockBatch(Object batchId, DLockBatch newBatch) throws InterruptedException {
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS, "[DLockGrantor.updateLockBatch] enter: {}", batchId);
    }
    synchronized(this.batchLocks) {
      waitWhileInitializing();      
      if (!acquireDestroyReadLock(0)) {
        waitUntilDestroyed();
        checkDestroyed();
      }
      try {
        checkDestroyed();
        final DLockBatch oldBatch = (DLockBatch) this.batchLocks.get(batchId);
        if (oldBatch!=null) {
          this.batchLocks.put(batchId, newBatch);
        }
      }
      finally {
        releaseDestroyReadLock();
      }
    }
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS, "[DLockGrantor.updateLockBatch] exit: {}", batchId);
    }
  }

  /**
   * Releases the transaction optimized lock batch.
   * <p>
   * Acquires acquireDestroyReadLock. Synchronizes on batchLocks.
   * 
   * @param batchId the identify of the transaction lock batch to release
   * @param owner the member that has created and locked the lock batch
   * @throws LockGrantorDestroyedException if grantor is destroyed or interrupted
   */
  public void releaseLockBatch(Object batchId, InternalDistributedMember owner) 
      throws InterruptedException {
    if (logger.isTraceEnabled(LogMarker.DLS)) {
      logger.trace(LogMarker.DLS, "[DLockGrantor.releaseLockBatch]");
    }
    synchronized (this.batchLocks) {
      waitWhileInitializing();
      if (!acquireDestroyReadLock(0)) {
        waitUntilDestroyed();
        checkDestroyed();
      }
      try {
        checkDestroyed();
        DLockBatch batch = (DLockBatch) this.batchLocks.remove(batchId);
        if (batch != null) {
          this.resMgr.releaseReservation((IdentityArrayList)batch.getReqs());
        }
//         Set lockNames = batch.getLockNames();
//         for (Iterator iter = lockNames.iterator(); iter.hasNext();) {
//           Object name = iter.next();
//           DLockGrantToken grant = getOrCreateGrant(
//               this.dlock.getOrCreateToken(name));
//           grant.releaseIfLockedBy(owner);
//         }
      } 
      finally {
        releaseDestroyReadLock();
      }
    }
  }
  
  /**
   * Returns true if the request comes from the local member.
   * 
   * @param request the lock request to check
   * @return true if the request comes from the local member
   */
  private boolean isLocalRequest(DLockRequestMessage request) {
    return request.getSender().equals(this.dlock.getDistributionManager().getId());
  }
  
  /** 
   * TEST HOOK: Allows testing to determine if there are waiting requests for 
   * a lock. 
   * <p>
   * Synchronizes on grantTokens and the grant token if one exists.
   *
   * @param name the lock to check for waiting requests for
   * @return true if the named lock has requests waiting to acquire it
   */
  boolean hasWaitingRequests(Object name) {
    DLockGrantToken grant = getGrantToken(name);
    if (grant == null) return false;
    synchronized(grant) {
      return grant.hasWaitingRequests();
    }
  }

  /**
   * Handles a DLockQueryMessage. Returns DLockGrantToken for the lock or
   * null. 
   * <p>
   * Acquires destroyReadLock. Synchronizes on grantTokens.
   *  
   * @param query the dlock query message to handle
   * @return DLockGrantToken for the lock or null
   */
  DLockGrantToken handleLockQuery(DLockQueryMessage query) throws InterruptedException {
    if (logger.isTraceEnabled(LogMarker.DLS)) {
      logger.trace(LogMarker.DLS, "[DLockGrantor.handleLockQuery] {}", query);
    }
    if (acquireDestroyReadLock(0)) {
      try {
        checkDestroyed();
        return getGrantToken(query.objectName);
      }
      finally {
        releaseDestroyReadLock();
      }
    }
    return null;
  }
  
  /**
   * Handles the provided lock request. The lock will either be granted,
   * refused if try-lock, or scheduled at end of waiting queue to eventually
   * be granted or timed out.
   * <p>
   * Acquires destroyReadLock. Synchronizes on grantTokens, suspendLock and
   * the grant token.
   * 
   * @param request the lock request to be processed by this grantor
   * @throws LockGrantorDestroyedException if grantor is destroyed
   */
  void handleLockRequest(DLockRequestMessage request) throws InterruptedException {
    Assert.assertTrue(request.getRemoteThread() != null);
    if (request.getObjectName() instanceof DLockBatch) {
      handleLockBatch(request);
      return;
    }
      
    waitWhileInitializing(); // calcWaitMillisFromNow

    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS, "[DLockGrantor.handleLockRequest] {}", request);
    }
    
    if (!acquireDestroyReadLock(0)) {
      if (isLocalRequest(request) && this.dlock.isDestroyed()) {
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.handleLockRequest] about to throwIfDestroyed");
        }
        // this special case is one fix for deadlock between waitUntilDestroyed
        // and dlock waitForGrantorCallsInProgress (when request is local)
        throwIfDestroyed(true);
      }
      else {
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.handleLockRequest] about to waitUntilDestroyed");
        }
        // is there still a deadlock when an explicit become is destroying
        // this grantor instead of destroying the dlock service?
        waitUntilDestroyed();
        checkDestroyed();
      }
    }
    try {
      checkDestroyed();
      if (acquireLockPermission(request)) {
        handlePermittedLockRequest(request);
      }
      else {
        // request has been added to suspendQueue for deferred handling
      }
    }
    finally {
      releaseDestroyReadLock();
    }
  }

  /** 
   * Internally handles a lock request which has permission to proceed.
   * <p>
   * Calling thread must hold destroyReadLock. Synchronizes on grantTokens, 
   * suspendLock and the grant token.
   * 
   * @param request the lock request to be processed by this grantor
   * @guarded.By {@link #acquireDestroyReadLock(long)}
   */
  private void handlePermittedLockRequest(final DLockRequestMessage request) {
    if (logger.isTraceEnabled(LogMarker.DLS)) {
      logger.trace(LogMarker.DLS, "[DLockGrantor.handlePermittedLockRequest] {}", request);
    }
    Assert.assertTrue(request.getRemoteThread() != null);
    DLockGrantToken grant = getOrCreateGrant(request.getObjectName());
    try {
      
      // try to grant immediately if not currently granted...
      if (grant.grantLockToRequest(request)) {
        // do nothing
      }
      
      // if request was local and then interrupted/released...
      else if (request.responded()) {
        // do nothing
      }
    
      // if request was a failed try-lock...
      else if (request.isTryLock()) {
        cleanupSuspendState(request);
        request.respondWithTryLockFailed(request.getObjectName());
      }
      
      // if request has timed out...
      else if (request.checkForTimeout()) {
        cleanupSuspendState(request);
      }
    
      // schedule into waiting queue for eventual granting...
      else {
        grant.schedule(request);
        this.thread.checkTimeToWait(calcWaitMillisFromNow(request), false);
      }
    }
    finally {
      grant.decAccess();
    }
  }
  
  /** 
   * Initializes this new grantor with previously held locks as provided during
   * grantor recovery. 
   * <p>
   * Acquires destroyReadLock. Synchronizes on this grantor, grantTokens, 
   * suspendLock, the grant token.
   *  
   * @param owner the member that owns the tokens to be scheduled
   * @param tokens set of DLockRemoteTokens to be scheduled for owner
   */
  void initializeHeldLocks(InternalDistributedMember owner, Set tokens) 
      throws InterruptedException {
    synchronized (this) {
      if (isDestroyed()) return;
      if (!acquireDestroyReadLock(0)) {
        return;
      }
    }
    
    try {
      synchronized (this.grantTokens) {
        Set members = 
            this.dlock.getDistributionManager().getDistributionManagerIds();
        
        final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
        for (Iterator iter = tokens.iterator(); iter.hasNext();) {
          DLockRemoteToken token = (DLockRemoteToken) iter.next();
          DLockGrantToken grantToken = getOrCreateGrant(token.getName());
          try {
            
            // make sure the token's owner is still in the system
            if (!members.contains(owner)) {
              // skipping because member is no longer in view
              if (isDebugEnabled_DLS) {
                logger.trace(LogMarker.DLS, "Initialization of held locks is skipping {} because owner {} is not in view: ", token, owner, members);
              }
              continue;
            }
              
            RemoteThread rThread = null;
            boolean isSuspendLock = false;
            int lockId = -1;
            
            synchronized(grantToken) {
              if (grantToken.isLeaseHeld()) {
                logger.error(LogMarker.DLS, LocalizedMessage.create(LocalizedStrings.DLockGrantor_INITIALIZATION_OF_HELD_LOCKS_IS_SKIPPING_0_BECAUSE_LOCK_IS_ALREADY_HELD_1, new Object[] { token, grantToken }));
                continue;
              }
              
              grantToken.grantLock(owner, 
                                   token.getLeaseExpireTime(), 
                                   token.getLeaseId(), 
                                   token.getLesseeThread());

              // grantToken may have already expired or is about to expire
              // complete initialization but make sure grantor thread will wake
              // up and expire it as soon as it's running
              if (grantToken.getLeaseExpireTime() > -1 
                  && grantToken.getLeaseExpireTime() < Long.MAX_VALUE) {
                long now = DLockService.getLockTimeStamp(this.dm);
                this.thread.checkTimeToWait(grantToken.getLeaseExpireTime() - now, true);
              }
              
              rThread = grantToken.getRemoteThread();
              isSuspendLock = grantToken.isSuspendLockingToken();
              lockId = grantToken.getLockId();
            }
            
            // update the readLock and suspendLocking states...
            synchronized(suspendLock) {
              if (isSuspendLock) {
                suspendLocking(rThread, lockId);
              }
              else {
                Assert.assertTrue(
                    !isLockingSuspended() || isLockingSuspendedBy(rThread),
                    "Locking is suspended by a different thread: " + token);
                Integer integer = (Integer) readLockCountMap.get(rThread);
                int readLockCount = integer == null ? 0 : integer.intValue();
                readLockCount++;
                readLockCountMap.put(rThread, Integer.valueOf(readLockCount));
                totalReadLockCount++;
                checkTotalReadLockCount();
              }
            } // suspendLock sync
            
          }
          finally {
            grantToken.decAccess();
          }
            
        } // tokens iter
      } // grantTokens sync
      return;
    }
    finally {
      releaseDestroyReadLock();
    }
  }
  
  /** 
   * Handles a request for extending the lease time of an already held lock.
   * <p>
   * Acquires destroyReadLock. Synchronizes on grantTokens and the grant token.
   * 
   * @param request the lock request to be reentered for lease extension
   * @return new extended leaseExpireTime or 0 if requestor no longer holds lock 
   */
  long reenterLock(DLockRequestMessage request) throws InterruptedException {
    waitWhileInitializing(); // calcWaitMillisFromNow
    if (!acquireDestroyReadLock(0)) {
      waitUntilDestroyed();
      checkDestroyed();
    }
    try {
      checkDestroyed();
      // to fix GEODE-678 no longer call request.checkForTimeout
      DLockGrantToken grant = getGrantToken(request.getObjectName());
      if (grant == null) {
        if (logger.isTraceEnabled(LogMarker.DLS)) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.reenterLock] no grantToken found for {}", request.getObjectName());
        }
        return 0;
      }
    
      synchronized (grant) { // synchronize against grant.expireAndGrantLock
        if (!this.dm.isCurrentMember(request.getSender())
            || grant.isDestroyed()) {
          return 0;
        }
        if (!grant.isLockedBy(request.getSender(), request.getLockId())) {
          if (logger.isTraceEnabled(LogMarker.DLS)) {
            logger.trace(LogMarker.DLS, "[DLockGrantor.reenterLock] grant is not locked by sender={} lockId={} grant={}", request.getSender(), request.getLockId(), grant);
          }
          return 0;
        }
      
        long leaseExpireTime = 
            Math.max(grant.getLeaseExpireTime(),
                     grant.calcLeaseExpireTime(request.getLeaseTime()));
        
        grant.grantLock(request.getSender(), 
                        leaseExpireTime, 
                        request.getLockId(), 
                        grant.getRemoteThread());
          
        return grant.getLeaseExpireTime();
      }
    } finally {
      releaseDestroyReadLock();
    }
  }
  
  /**
   * Release named lock if held by owner using lockId. Called from 
   * DLockReleaseMessage.basicProcess for remote unlock. 
   * <p>
   * Acquires destroyReadLock. Synchronizes on grantTokens and the grant token.
   * 
   * @param name the name of the lock to release
   * @param owner the member releasing the lock
   * @param lockId the identity of the lease used by the owner
   * @throws LockGrantorDestroyedException if grantor is destroyed
   */
  void releaseIfLocked(Object name, 
                       InternalDistributedMember owner, 
                       int lockId) throws InterruptedException {
    waitWhileInitializing();
    if (!acquireDestroyReadLock(0)) {
      waitUntilDestroyed();
      checkDestroyed();
    }
    try {
      checkDestroyed();
      getAndReleaseGrantIfLockedBy(name, owner, lockId);
    }
    finally {
      releaseDestroyReadLock();
    } 
  }
  
  /**
   * Fetches the actual grant token and releases it if leased by owner using 
   * lockId.
   * DLockReleaseMessage.basicProcess -> releaseIfLocked -> 
   *     getAndReleaseGrantIfLockedBy
   * <p>
   * Caller must hold destroyReadLock. Synchronizes on grantTokens and the 
   * grant token.
   *     
   * @param name the name of the lock to release
   * @param owner the member attempting to release the granted lock
   * @param lockId the id of the lease used by the owner
   * @guarded.By {@link #acquireDestroyReadLock(long)}
   */
  private void getAndReleaseGrantIfLockedBy(Object name, 
                                            InternalDistributedMember owner,
                                            int lockId) {
    synchronized (this.grantTokens) {
      DLockGrantToken grantToken = basicGetGrantToken(name);
      if (grantToken != null) { // checking isTokenDestroyed here will deadlock
        synchronized (grantToken) {
          //if (!grantToken.isTokenDestroyed())
          try {
            grantToken.releaseIfLockedBy(owner, lockId);
            removeGrantIfUnused(grantToken);
          }
          catch (IllegalStateException e) {
            this.dlock.checkDestroyed();
            checkDestroyed();
            // must have hit race... grantor doesn't have the token
            return;
          }
        }
      }
    }
  }
  
  /**
   * Fetches the grant token for named lock and attempts to grant it to the
   * next waiting requestor if one exists. Called from DLockReleaseProcessor 
   * when another process releases a lock and after the reply has been sent.
   * <p>
   * Acquires destroyReadLock. Synchronizes on grantTokens and the grant token.
   * 
   * @param name the name of the lock to grant
   * @throws LockGrantorDestroyedException if grantor is destroyed
   */
  void grantLock(Object name) throws InterruptedException {
    waitWhileInitializing();
    if (!acquireDestroyReadLock(0)) {
      waitUntilDestroyed();
      checkDestroyed();
    }
    try {
      checkDestroyed();
      DLockGrantToken grant = getGrantToken(name);
      if (grant != null) {
        removeGrantIfUnused(grant);
      }
    }
    finally {
      releaseDestroyReadLock();
    } 
  }

  /**
   * Handles the departure of a member by releasing every lock it owned.
   * <p>
   * Acquires destroyReadLock. Synchronizes on grantTokens, suspendLock, and
   * the grant token.
   * 
   * @param owner the member that departed
   */
  void handleDepartureOf(InternalDistributedMember owner) 
      throws InterruptedException {
    // bug 32657 has another cause in this method... interrupted thread from
    // connection/channel layer caused acquireDestroyReadLock to fail... 
    // fixed by Darrel in com.gemstone.gemfire.internal.tcp.Connection
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
    if (acquireDestroyReadLock(0)) {
      try {
        if (isDestroyed()) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS, "[DLockGrantor.handleDepartureOf] grantor is destroyed; ignoring {}", owner);
          }
          return;
        }
        try {
          DLockLessorDepartureHandler handler = 
              this.dlock.getDLockLessorDepartureHandler();
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS, "[DLockGrantor.handleDepartureOf] handler = {}", handler);
          }
          if (handler != null) {
            handler.handleDepartureOf(owner, this);
          }
        }
        catch (CancelException e) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS, "[DlockGrantor.handleDepartureOf] ignored cancellation (1)");
          }
        }
        finally {
          synchronized (this.suspendLock) {
            HashSet removals = new HashSet();
            for (Iterator it=readLockCountMap.entrySet().iterator(); it.hasNext(); ) {
              Map.Entry entry = (Map.Entry)it.next();
              RemoteThread rThread = (RemoteThread)entry.getKey();
              if (rThread.getDistributedMember().equals(owner)) {
                removals.add(rThread);
              }
            }
            for (Iterator it=removals.iterator(); it.hasNext(); ) {
              try {
                postReleaseLock((RemoteThread)it.next(), null);
              }
              catch (CancelException e) {
                if (isDebugEnabled_DLS) {
                  logger.trace(LogMarker.DLS, "[DlockGrantor.handleDepartureOf] ignored cancellation (2)");
                }
              }
            }
          } // synchronized
          synchronized (this.grantTokens) {
            // do not call handleDepartureOf while iterating grantTokens
            //   changes fix bug 39172 (ConcurrentModificationException)
            
            // 1) built up list of grants that reference departed member
            List grantsReferencingMember = new ArrayList();
            Collection grants = this.grantTokens.values();
            for (Iterator iter = grants.iterator(); iter.hasNext();) {
              DLockGrantToken grant = (DLockGrantToken) iter.next();
              try {
                grant.checkDepartureOf(owner, grantsReferencingMember);
              }
              catch (CancelException e) {
                if (isDebugEnabled_DLS) {
                  logger.trace(LogMarker.DLS, "[DlockGrantor.handleDepartureOf] ignored cancellation (3)");
                }
              }
            } // for
            
            // 2) call handleDepartureOf on list of grantsReferencingMember
            ArrayList grantsToRemoveIfUnused = new ArrayList();
            for (Iterator iter = grantsReferencingMember.iterator(); 
                 iter.hasNext();) {
              DLockGrantToken grant = (DLockGrantToken) iter.next();
              try {
                grant.handleDepartureOf(owner, grantsToRemoveIfUnused);
              }
              catch (CancelException e) {
                if (isDebugEnabled_DLS) {
                  logger.trace(LogMarker.DLS, "[DlockGrantor.handleDepartureOf] ignored cancellation (4)");
                }
              }
            } // for
            
            // 3) remove grants in grantsToRemoveIfUnused list
            // TODO: if grantsReferencingMember is always empty remove this
            for (Iterator iter = grantsToRemoveIfUnused.iterator(); 
                 iter.hasNext();) {
              DLockGrantToken grant = (DLockGrantToken) iter.next();
              try {
                removeGrantIfUnused(grant);
              }
              catch (CancelException e) {
                if (isDebugEnabled_DLS) {
                  logger.trace(LogMarker.DLS, "[DlockGrantor.handleDepartureOf] ignored cancellation (5)");
                }
              }
            } // for
          } // synchronized this.grantTokens
        } // finally
      }
      finally {
        releaseDestroyReadLock();
      }
    }
  }

  /**
   * Destroys this grantor without attempting to transfer grant tokens to
   * a successor.
   * <p>
   * Acquires destroyWriteLock. Synchronizes on this grantor, grantTokens, 
   * and each grant token.
   */
  void destroy() {
    synchronized (this) {
      if (isDestroyed()) return;
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "[simpleDestroy]");
      }
      // wait for the destroy write lock ignoring interrupts...
      boolean acquired = false;
      try {
        boolean locksHeld = false;
        try {
          acquireDestroyWriteLock(Long.MAX_VALUE);
          acquired = true;
          // check for any held locks...
          
          if (isInitializing()) {
            // assume the worst case and tell the elder that recovery will be required
            locksHeld = true;
          }
          else {
            synchronized (this.grantTokens) {
              InternalDistributedMember me = this.dlock.getDistributionManager().getId();
              for (Iterator iter = this.grantTokens.values().iterator(); iter.hasNext();) {
                DLockGrantToken grant = (DLockGrantToken) iter.next();
                InternalDistributedMember owner = grant.getOwner();
                if (owner != null && !owner.equals(me)) {
                  locksHeld = true;
                  break;
                }
              }
            }
          }
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS, "[simpleDestroy] {} locks held", (locksHeld ? "with" : "without"));
          }
        }
        finally {
          // make sure the following occurs even if checking locks above failed
          
          try {
            // release latches and change internal state
            destroyGrantor();
          }
          finally {
            // tell the elder we are not the grantor anymore
            this.dlock.clearGrantor(this.getVersionId(), locksHeld);
          }
        }
      }
      finally {
        if (acquired) {
          releaseDestroyWriteLock();
        }
      }
    }
  }
  
  /**
   * Send replies to all waiting requestors to notify them that this is no
   * longer the grantor.
   * <p>
   * Caller must acquire destroyWriteLock. Synchronizes on suspendLock,
   * grantTokens, and each grant token.
   * 
   * @guarded.By {@link #acquireDestroyWriteLock(long)}
   */
  private void destroyGrantor() {
    Assert.assertHoldsLock(this,true);
    makeDestroyed();
    // reply to all pending requests w/ NOT_GRANTOR
    synchronized (this.grantTokens) {
      Collection grants = this.grantTokens.values();
      for (Iterator iter = grants.iterator(); iter.hasNext();) {
        DLockGrantToken grant = (DLockGrantToken) iter.next();
        try {
          grant.handleGrantorDestruction();
        }
//        catch (VirtualMachineError err) {
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch (Throwable t) {
//          // Whenever you catch Error or Throwable, you must also
//          // catch VirtualMachineError (see above).  However, there is
//          // _still_ a possibility that you are dealing with a cascading
//          // error condition, so you also need to check to see if the JVM
//          // is still usable:
//          SystemFailure.checkFailure();
//        }
        finally {
          
        }
      }
    }
    
    synchronized(suspendLock) {
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "[DLockGrantor.destroyAndRemove] responding to {} permitted requests.", permittedRequests.size());
      }
      respondWithNotGrantor(permittedRequests.iterator());

      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "[DLockGrantor.destroyAndRemove] responding to {} requests awaiting permission.", suspendQueue.size());
      }
      respondWithNotGrantor(suspendQueue.iterator());

      for (Iterator iter = permittedRequestsDrain.iterator(); iter.hasNext();) {
        final List drain = (List) iter.next();
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.destroyAndRemove] responding to {} drained permitted requests.", drain.size());
        }
        respondWithNotGrantor(drain.iterator());
      }
    }
  }
  
  /**
   * Send responses to specified requests informing the senders that this
   * is no longer the grantor.
   * <p>
   * Caller must acquire destroyWriteLock.
   * 
   * @param requests the requests to respond to
   * @guarded.By {@link #acquireDestroyWriteLock(long)}
   */
  private void respondWithNotGrantor(Iterator requests) {
    while (requests.hasNext()) {
      final DLockRequestMessage request = (DLockRequestMessage) requests.next();
      try {
        request.respondWithNotGrantor();
      }
//      catch (VirtualMachineError err) {
//        SystemFailure.initiateFailure(err);
//        // If this ever returns, rethrow the error.  We're poisoned
//        // now, so don't let this thread continue.
//        throw err;
//      }
//      catch (Throwable t) {
//        // Whenever you catch Error or Throwable, you must also
//        // catch VirtualMachineError (see above).  However, there is
//        // _still_ a possibility that you are dealing with a cascading
//        // error condition, so you also need to check to see if the JVM
//        // is still usable:
//        SystemFailure.checkFailure();
//      }
      finally {
        
      }
    }
  }
  
  /**
   * TEST HOOK: Log additional debugging info about this grantor.
   */
  void debug() {
    logger.info(LogMarker.DLS, LocalizedMessage.create(LocalizedStrings.TESTING,
        "[DLockGrantor.debug] svc=" + this.dlock.getName() +
        "; state=" + this.state + 
        "; initLatch.ct=" + this.whileInitializing.getCount()));
  }
  
  /**
   * Make this grantor ready for handling lock requests.
   * <p>
   * Synchronizes on this grantor.
   * 
   * @param enforceInitializing true if this should assert isInitializing
   * @return true if grantor was successfully made ready
   */
  synchronized boolean makeReady(boolean enforceInitializing) {
    if (isDestroyed()) {
      this.dlock.checkDestroyed();
    }
    if (!enforceInitializing) {
      if (!isInitializing()) {
        return false;
      }
    }
    assertInitializing();
    if (logger.isTraceEnabled(LogMarker.DLS)) {
      StringBuffer sb = new StringBuffer(
          "DLockGrantor " + this.dlock.getName() + " initialized with:");
      for (Iterator tokens = grantTokens.values().iterator(); tokens.hasNext();) {
        sb.append("\n\t" + tokens.next());
      }
      logger.trace(LogMarker.DLS, sb.toString());
    }
    this.state = READY;
    this.whileInitializing.countDown();
    this.thread.start();
    return true;
  }

  // -------------------------------------------------------------------------
  //   Private methods
  // -------------------------------------------------------------------------
  
  /**
   * Drain currently permitted requests and grant lock to next requestor.
   * <p>
   * Acquires destroyReadLock. Synchronizes on suspendLock, grantTokens,
   * and the grant token.
   * 
   * @param objectName the lock to perform post release tasks for
   */
  void postRemoteReleaseLock(Object objectName) throws InterruptedException {
    if (!acquireDestroyReadLock(0)) {
      return;
    }
    try {
      checkDestroyed();
      drainPermittedRequests();
      grantLock(objectName);
    }
    catch (LockServiceDestroyedException e) {
      // ignore... service was destroyed and that's ok
    }
    catch (LockGrantorDestroyedException e) {
      // ignore... grantor was destroyed and that's ok
    }
    finally {
      releaseDestroyReadLock();
    } 
  }
  
  /**
   * Acquires a read lock on the destroy ReadWrite lock uninterruptibly using 
   * millis for try-lock attempt.
   * 
   * @param millis the milliseconds to try to acquire lock within
   * @return true if destroy read lock was acquired
   * @throws DistributedSystemDisconnectedException if system has been disconnected
   */
  private boolean acquireDestroyReadLock(long millis) throws InterruptedException {
    boolean interrupted = Thread.interrupted();
    try {
      if (interrupted && this.dlock.isInterruptibleLockRequest()) {
        throw new InterruptedException();
      }
      while (true) {
        try {
          this.dm.getCancelCriterion().checkCancelInProgress(null); // is this needed?
          boolean acquired = this.destroyLock.readLock().tryLock(millis);
          return acquired;
        }
        catch (InterruptedException e) {
          interrupted = true;
          throwIfInterruptible(e);
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Releases a read lock on the destroy ReadWrite lock.
   */
  private void releaseDestroyReadLock() {
    this.destroyLock.readLock().unlock();
  }
  
  /**
   * Acquires the write lock on the destroy ReadWrite lock within specified
   * millis.
   * 
   * @param millis the milliseconds to attempt to acquire the lock within
   * @throws DistributedSystemDisconnectedException if system has been disconnected
   */
  private void acquireDestroyWriteLock(long millis) {
    for (;;) {
      boolean interrupted = Thread.interrupted();
      try {
        this.dm.getCancelCriterion().checkCancelInProgress(null);
        boolean acquired = this.destroyLock.writeLock().tryLock(millis);
        if (acquired) {
          return;
        }
      }
      catch (InterruptedException e) {
        interrupted = true;
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Releases the write lock on the destroy ReadWrite lock.
   */
  private void releaseDestroyWriteLock() {
    this.destroyLock.writeLock().unlock();
  }
  
  /** 
   * Returns time to wait in millis from now based on start and wait in request.
   * 
   * @return the current wait time for the request before it times out
   */  
  private long calcWaitMillisFromNow(DLockRequestMessage request) {
    long result = request.getTimeoutTS();
    if (result != Long.MAX_VALUE) {
      long now = DLockService.getLockTimeStamp(
        this.dlock.getDistributionManager());
      result = result - now;
    }
    return result;
  }
  

  /**
   * Shuts down the grantor thread and changes internal state to destroyed.
   */
  private void makeDestroyed() {
    try {
      this.thread.shutdown();
      this.state = DESTROYED;
      if (logger.isTraceEnabled(LogMarker.DLS)) {
        logger.trace(LogMarker.DLS, "DLockGrantor {} state is DESTROYED", this.dlock.getName());
      }
      if (this.untilDestroyed.getCount() > 0) {
        this.untilDestroyed.countDown();
      }
      if (this.whileInitializing.getCount() > 0) {
        this.whileInitializing.countDown();
      }
      this.dlock.getDistributionManager()
        .removeMembershipListener(this.membershipListener);
    }
    finally {
      this.dlock.getStats().incGrantors(-1);
    }
  }
  
  /**
   * Returns a snapshot of the current grant tokens.
   * <p>
   * Synchronizes on grantTokens.
   * 
   * @return a snapshot of the current grant tokens
   */
  protected Collection snapshotGrantTokens() {
    Collection snapshot = null;
    synchronized (this.grantTokens) {
      snapshot = new ArrayList(this.grantTokens.values());
    }
    return snapshot;
  }
  
  /**
   * Fetches or creates a new grant token for the named lock.
   * <p>
   * Synchronizes on grantTokens and the grant token.
   * 
   * @param name the name of the lock
   * @return the grant token for the named lock
   */
  private DLockGrantToken getOrCreateGrant(Object name) {
    DLockGrantToken grantToken = null;
    synchronized (this.grantTokens) {
      grantToken = basicGetGrantToken(name);
      if (grantToken == null) { // checking isTokenDestroyed here will deadlock
        grantToken = new DLockGrantToken(this.dlock, this, name);
        grantToken.incAccess();
        basicPutGrantToken(grantToken);
      }
      else {
        synchronized (grantToken) {
          if (grantToken.isDestroyed()) {
            grantToken = new DLockGrantToken(this.dlock, this, name);
            grantToken.incAccess();
            basicPutGrantToken(grantToken);
          }
          else {
            grantToken.incAccess();
          }
        }
      }
    }
    return grantToken;
  }

  /**
   * TEST HOOK: Returns an unmodifible collection backed by the values of the 
   * DLockGrantToken map for testing purposes only.
   * <p>
   * Synchronizes on grantTokens.
   * 
   * @return unmodifible collection of the grant tokens
   */
  public Collection getGrantTokens() {
    synchronized (this.grantTokens) {
      return Collections.unmodifiableCollection(this.grantTokens.values());
    }
  }
  
  /**
   * Remove the grant token if it is unused. 
   * <p>
   * Synchronizes on grantTokens and the grant token.
   * 
   * @param grant the grant token to remove
   */
  protected void removeGrantIfUnused(DLockGrantToken grant) {
    synchronized (this.grantTokens) {
      synchronized (grant) {
        if (isDestroyed() || grant.isDestroyed()) {
          return;
        }
        else if (grant.grantLockToNextRequest()) {
          return;
        }
        else if (!grant.isBeingAccessed() &&
                 !grant.isGranted(false) && 
                 !grant.hasWaitingRequests()) {
          basicRemoveGrantToken(grant);
        }
      }
    }
  }
  
  /**
   * Iterates over grants and attempts to remove any that are no longer in use.
   * <p>
   * Synchronizes on grantTokens and the grant token.
   * 
   * @param grants the grants to be checked for removal
   */
  protected void removeUnusedGrants(Iterator grants) {
    while (grants.hasNext()) {
      DLockGrantToken grant = (DLockGrantToken) grants.next();
      removeGrantIfUnused(grant);
    }
  }
  
  /**
   * Returns the DLockGrantToken from grant tokens map stored under the key 
   * name.
   * <p>
   * Synchronizes on grantTokens.
   */
  public DLockGrantToken getGrantToken(Object name) {
    synchronized (this.grantTokens) {
      return basicGetGrantToken(name);
    }
  }
  
  /**
   * Fetches the grant token value stored in the map under key name.
   * <p>
   * Caller must synchronize on grantTokens
   * 
   * @param name the key to fetch the grant token value for
   * @return the grant token stored under key name
   * @guarded.By {@link #grantTokens}
   */
  private DLockGrantToken basicGetGrantToken(Object name) {
    return (DLockGrantToken) this.grantTokens.get(name);
  }
  
  /**
   * Stores the grant token as a value in the map under the key of its name.
   * <p>
   * Caller must synchronize on grantTokens
   * 
   * @param grantToken the grant token to store in the map
   * @guarded.By {@link #grantTokens}
   */
  private void basicPutGrantToken(DLockGrantToken grantToken) {
    this.grantTokens.put(grantToken.getName(), grantToken);
    dlock.getStats().incGrantTokens(1);
  }

  /**
   * Removes the grant token from the map. 
   * <p>
   * Caller must synchronize on grantTokens and then the grantToken.
   * 
   * @param grantToken the grant token to remove from the map.
   * @guarded.By {@link #grantTokens} and grantToken
   */
  private void basicRemoveGrantToken(DLockGrantToken grantToken) {
    Object removed = this.grantTokens.remove(grantToken.getName()); // changed to ref token
    if (removed != null) {
      Assert.assertTrue(removed == grantToken);
      grantToken.destroy();
      if (logger.isTraceEnabled(LogMarker.DLS)) {
        logger.trace(LogMarker.DLS, "[DLockGrantor.basicRemoveGrantToken] removed {}; removed={}",  grantToken, removed);
      }
    }
  }
  
  /**
   * Iterates over grants and handles any that have expired.
   * <p>
   * Synchronizes on each grant token.
   * 
   * @param grants the grants to iterate over
   * @return the next smallest expiration time
   */
  protected long expireAndGrantLocks(Iterator grants) {
    long smallestExpire = Long.MAX_VALUE;
    while (grants.hasNext()) {
      DLockGrantToken grant = (DLockGrantToken) grants.next();
      if (grant.isDestroyed()) {
        continue;    
      }
      long expire = grant.expireAndGrantLock();
      if (expire < smallestExpire) {
        smallestExpire = expire;
      }
    }
    return smallestExpire;
  }
  
  /**
   * Iterates over grants and handles any that have timed out.
   * <p>
   * Synchronizes on each grant token.
   * 
   * @param grants the grants to iterate over
   * @return the next smallest timeout
   */
  protected long handleRequestTimeouts(Iterator grants) {
    long smallestTimeout = Long.MAX_VALUE;
    while (grants.hasNext()) {
      DLockGrantToken grant = (DLockGrantToken) grants.next();
      if (grant.isDestroyed()) {
        continue;
      }
      long timeout = grant.handleRequestTimeouts();
      if (timeout < smallestTimeout) {
        smallestTimeout = timeout;
      }
    }
    return smallestTimeout;
  }

  /**
   * TEST HOOK: Specifies time to sleep while handling suspend in order to
   * cause a timeout.
   * <p>
   * Synchronizes on suspendLock.
   * 
   * @param value
   */
  public void setDebugHandleSuspendTimeouts(int value) {
    synchronized (suspendLock) {
      debugHandleSuspendTimeouts = value;
    }
  }
  
  /**
   * True to enable test hook to sleep while handling suspend to cause timeout.
   * 
   * @guarded.By {@link #suspendLock}
   */
  private int debugHandleSuspendTimeouts = 0;
  
  /**
   * Iterates through a copy of suspendQueue and handles any requests that
   * have timed out.
   * <p>
   * Synchronizes on suspendLock.
   * 
   * @return the next smallest timeout in the suspendQueue
   */
  protected long handleSuspendTimeouts() {
    long smallestTimeout = Long.MAX_VALUE;
    synchronized (suspendLock) {
      if (suspendQueue.isEmpty()) return smallestTimeout;
      if (isDestroyed()) return smallestTimeout;
    }
    List timeouts = new ArrayList();

    List copySuspendQueue = null;
    synchronized (suspendLock) {
      copySuspendQueue = new ArrayList(suspendQueue);
    }
      
    for (Iterator iter = copySuspendQueue.iterator(); iter.hasNext();) {
      DLockRequestMessage req = (DLockRequestMessage) iter.next();
      if (req.checkForTimeout()) { // sends DLockResponseMessage if timeout
        cleanupSuspendState(req);
        timeouts.add(req);
      }
      else {
        long timeout = req.getTimeoutTS();
        if (timeout < smallestTimeout) {
          smallestTimeout = timeout;
        }
      }
    }

    int localDebugHandleSuspendTimeouts = 0;
    synchronized (suspendLock) {
      localDebugHandleSuspendTimeouts = debugHandleSuspendTimeouts;
    }
    if (localDebugHandleSuspendTimeouts > 0) {
      try {
        logger.info(LogMarker.DLS, LocalizedMessage.create(
            LocalizedStrings.DLockGrantor_DEBUGHANDLESUSPENDTIMEOUTS_SLEEPING_FOR__0, localDebugHandleSuspendTimeouts));
        Thread.sleep(localDebugHandleSuspendTimeouts);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    
    if (!timeouts.isEmpty()) {
      synchronized (suspendLock) {
        if (writeLockWaiters > 0) {
          // suspenders exist... must iterate through for safe removal
          for (Iterator iter = timeouts.iterator(); iter.hasNext();) {
            DLockRequestMessage req = (DLockRequestMessage) iter.next();
          
            // attempt to remove timed out req from suspendQueue
            if (suspendQueue.remove(req)) {
              // request was still in suspendQueue, so check if suspender
              if (req.isSuspendLockingRequest()) {
                writeLockWaiters--;
              }
            }
          } // for
        }
        else {
          // no suspenders so safe to removeAll
          Assert.assertTrue(writeLockWaiters == 0,
            "Grantor state writeLockWaiters changed while holding suspendLock");
          suspendQueue.removeAll(timeouts);
        }
      checkWriteLockWaiters();
      } // synchronized
    }
    return smallestTimeout;
  }
  
  /**
   * Returns string representation for the enumerated grantor state.
   * 
   * @param stateInt the number of the state to return a string for
   * @return string representation for the enumerated grantor state
   */
  private String stateToString(int stateInt) {
    String stateDesc = null;
    switch (stateInt) {
      case INITIALIZING:  stateDesc = "INITIALIZING"; break;
      case READY:         stateDesc = "READY"; break;
      case DESTROYED:     stateDesc = "DESTROYED"; break;
      default:            stateDesc = null; break;
    }
    if (stateDesc == null) {
      throw new IllegalArgumentException(LocalizedStrings.DLockGrantor_UNKNOWN_STATE_FOR_GRANTOR_0.toLocalizedString(Integer.valueOf(state)));
    }
    return stateDesc;
  }
  
  /**
   * Throws IllegalStateException if this grantor is not still initializing.
   * 
   * @throws IllegalStateException if this grantor is not still initializing
   */
  private void assertInitializing() {
    if (this.state != INITIALIZING) {
      String stateDesc = stateToString(this.state);
      throw new IllegalStateException(LocalizedStrings.DLockGrantor_DLOCKGRANTOR_OPERATION_ONLY_ALLOWED_WHEN_INITIALIZING_NOT_0.toLocalizedString(stateDesc));
    }
  }
  
  /** 
   * Suspends locking by the remote thread and lease id. 
   * <p>
   * Caller must synchronize on suspendLock.
   * 
   * Concurrency: protected by synchronization of {@link #suspendLock}
   * 
   * @param myRThread the remote thread that is about to suspend locking
   * @param lockId the id of the lock request used to suspend locking
   */
  protected void suspendLocking(final RemoteThread myRThread, final int lockId) {
    if (DEBUG_SUSPEND_LOCK) {
      Assert.assertHoldsLock(this.suspendLock,true);
    }
    if (logger.isTraceEnabled(LogMarker.DLS)) {
      logger.trace(LogMarker.DLS, "Suspend locking of {} by {} with lockId of {}", this.dlock, myRThread, lockId);
    }
    Assert.assertTrue(myRThread != null,
        "Attempted to suspend locking for null RemoteThread");
    Assert.assertTrue(this.lockingSuspendedBy == null || 
        this.lockingSuspendedBy.equals(myRThread),
        "Attempted to suspend locking for " + myRThread + 
        " but locking is already suspended by " + this.lockingSuspendedBy); // KIRK: assert fails in bug 37945
    this.suspendedLockId = lockId;
    this.lockingSuspendedBy = myRThread;
  }
  
  /** 
   * Resume locking after it has been suspended. 
   * <p>
   * Caller must synchronize on suspendLock.
   */ 
  private void resumeLocking() {
    if (DEBUG_SUSPEND_LOCK) {
      Assert.assertHoldsLock(this.suspendLock,true);
    }
    if (logger.isTraceEnabled(LogMarker.DLS)) {
      logger.trace(LogMarker.DLS, "Resume locking of {}", this.dlock);
    }
    this.lockingSuspendedBy = null;
    this.suspendedLockId = INVALID_LOCK_ID;
  }
  
  /** 
   * Returns true if locking has been suspended. 
   * <p>
   * Caller must synchronize on suspendLock.
   * 
   * Concurrency: protected by synchronization of {@link #suspendLock}
   * 
   * @return true if locking has been suspended
   */
  protected boolean isLockingSuspended() {
    if (DEBUG_SUSPEND_LOCK) {
      Assert.assertHoldsLock(this.suspendLock,true);
    }
    return this.lockingSuspendedBy != null;
  }
  
  /** 
   * Returns true if locking has been suspended. 
   * <p>
   * Synchronizes on suspendLock.
   * 
   * @return true if locking has been suspended
   */
  protected boolean isLockingSuspendedWithSync() {
    synchronized (this.suspendLock) {
      return this.lockingSuspendedBy != null;
    }
  }
  
  /** 
   * Returns true if locking has been suspended by the remote thread. 
   * <p>
   * Caller must synchronize on suspendLock.
   * 
   * Concurrency: protected by synchronization of {@link #suspendLock}
   * 
   * @return true if locking has been suspended by the remote thread
   */
  protected boolean isLockingSuspendedBy(final RemoteThread rThread) {
    if (DEBUG_SUSPEND_LOCK) {
      Assert.assertHoldsLock(this.suspendLock,true);
    }
    if (rThread == null) return false;
    return rThread.equals(this.lockingSuspendedBy);
  }
  
  String displayStatus(RemoteThread rThread, Object name) {
    StringBuffer sb = new StringBuffer();
    synchronized (this.suspendLock) {
      sb.append(' ');
      sb.append(this.toString());
      sb.append(" id=" + this.hashCode());
      sb.append(" rThread=" + rThread);
      if (name != null) {
        sb.append(" name=" + name);
      }
      sb.append(" permittedRequests (" + permittedRequests.size() + ")=" 
          + permittedRequests.toString() + "");
      sb.append(" suspendedLockId = " + suspendedLockId);
      sb.append(" lockingSuspendedBy = " + lockingSuspendedBy);
      sb.append(" writeLockWaiters = " + writeLockWaiters);
      sb.append(" totalReadLockCount = " + totalReadLockCount);
      sb.append("\nsuspendQueue (" + suspendQueue.size() + ")=" 
          + suspendQueue.toString());
      // Kirk said it was ok to not log the list of readLockers to cut
      // down on how much logging is done at fine level.
      sb.append("\nreadLockers (" + readLockCountMap.size()
                + ")" /* + "=" + readLockCountMap.toString()*/);
    }
    return sb.toString();
  }
  
  /**
   * @guarded.By {@link #suspendLock}
   */
  private void postReleaseSuspendLock(RemoteThread rThread, Object lock) {
    if (!isLockingSuspendedBy(rThread)) {
      // hit bug related to 35749
      if (logger.isTraceEnabled(LogMarker.DLS)) {
        logger.trace(LogMarker.DLS, "[postReleaseSuspendLock] locking is no longer suspended by {}", rThread);
      }
      return;
    }
    boolean resume = true;
    Integer integer = (Integer) readLockCountMap.get(rThread);
    int readLockCount = integer == null ? 0 : integer.intValue();
    if (readLockCount == 0 && !suspendQueue.isEmpty()) {
      final DLockRequestMessage nextRequest = 
	  (DLockRequestMessage) suspendQueue.getFirst();
      if (nextRequest.isSuspendLockingRequest()) {
	resume = false;
	//final RemoteThread myRemoteThread = nextRequest.getRemoteThread();
	// hand-off suspendLocking while under sync...
	resumeLocking();
	suspendLocking(nextRequest.getRemoteThread(), nextRequest.getLockId());
	permittedRequests.add(suspendQueue.removeFirst());
	writeLockWaiters--;
	checkWriteLockWaiters();
      }
    }
    if (resume) {
      resumeLocking();
      // drain readLocks from suspendQueue into permittedRequests queue
      while (!suspendQueue.isEmpty()) {
	final DLockRequestMessage nextRequest = 
	    (DLockRequestMessage) suspendQueue.getFirst();
	if (nextRequest.isSuspendLockingRequest()) {
	  Assert.assertTrue(writeLockWaiters > 0,
	    "SuspendLocking request is waiting but writeLockWaiters is 0");
	  break;
	}
	RemoteThread nextRThread = nextRequest.getRemoteThread();
	integer =(Integer)  readLockCountMap.get(nextRThread);
	readLockCount = integer == null ? 0 : integer.intValue();
	readLockCount++;
	readLockCountMap.put(nextRThread, Integer.valueOf(readLockCount));
	totalReadLockCount++;
	checkTotalReadLockCount();
	permittedRequests.add(suspendQueue.removeFirst());
      }
    }
    if (logger.isTraceEnabled(LogMarker.DLS)) {
      logger.trace(LogMarker.DLS, "[postReleaseSuspendLock] new status {}", displayStatus(rThread, null));
    }
  }

  /**
   * @guarded.By {@link #suspendLock}
   */
  private void postReleaseReadLock(RemoteThread rThread, Object lock) {
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
    
    // handle release of regular lock
    //boolean permitSuspend = false;
    Integer integer = (Integer) readLockCountMap.get(rThread);
    int readLockCount = integer == null ? 0 : integer.intValue();
    //Assert.assertTrue(readLockCount > 0, rThread + " not found in " + readLockCountMap); // KIRK
    if (readLockCount < 1) {
      // hit bug 35749
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "[postReleaseReadLock] no locks are currently held by {}", rThread);
      }
      return;
    }
    readLockCount--;

    if (readLockCount == 0) {
      readLockCountMap.remove(rThread);
    }
    else {
      readLockCountMap.put(rThread, Integer.valueOf(readLockCount));
    }
    totalReadLockCount--;

    if (totalReadLockCount < 0) {
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "Total readlock count has dropped to {} for {}", totalReadLockCount, this);
      }
    }
    if (totalReadLockCount == 0 && !suspendQueue.isEmpty()) {
      final DLockRequestMessage nextRequest = 
	  (DLockRequestMessage) suspendQueue.getFirst();
      if (nextRequest.isSuspendLockingRequest()) {
	suspendLocking(nextRequest.getRemoteThread(), nextRequest.getLockId());
	writeLockWaiters--;
	permittedRequests.add(suspendQueue.removeFirst());
	checkWriteLockWaiters();
      }
      else {
        String s = new StringBuilder("\n (readLockCount=").append(readLockCount)
                  .append(", totalReadLockCount=").append(totalReadLockCount)
                  .append(", writeLockWaiters=").append(writeLockWaiters)
                  .append(",\nsuspendQueue=").append(suspendQueue)
                  .append(",\npermittedRequests=").append(permittedRequests).toString();
        logger.warn(LocalizedMessage.create(LocalizedStrings.DLockGrantor_RELEASED_REGULAR_LOCK_WITH_WAITING_READ_LOCK_0, s));
	Assert.assertTrue(false, LocalizedStrings.DLockGrantor_RELEASED_REGULAR_LOCK_WITH_WAITING_READ_LOCK_0.toString(s));
      }
    }
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS, "[postReleaseReadLock] new status {}", displayStatus(rThread, null));
    }
    checkTotalReadLockCount();
  }

  /**
   * Handles post release lock tasks including tracking the current suspend
   * locking states.
   * <p>
   * Synchronizes on suspendLock.
   * 
   * @param rThread the remote thread that released the lock
   * @param lock the named lock that was released
   */
  protected void postReleaseLock(RemoteThread rThread, Object lock) {
    Assert.assertTrue(rThread != null);
    synchronized(suspendLock) {
      checkDestroyed();
      if (logger.isTraceEnabled(LogMarker.DLS)){ 
        logger.trace(LogMarker.DLS, "[postReleaseLock] rThread={} lock={} permittedRequests={} suspendQueue={}", rThread, lock, permittedRequests, suspendQueue);
      }
      if (DLockService.SUSPEND_LOCKING_TOKEN.equals(lock)) {
        postReleaseSuspendLock(rThread, lock);
      }
      else {
        postReleaseReadLock(rThread, lock);
      }
    } // suspendLock sync
  }
  
  /**
   * Departure or other codepath NOT specific to unlock requires that we
   * cleanup suspend state that was already permitted to request. This needs
   * to be invoked for both regular and suspend locks.
   * <p>
   * Synchronizes on suspendLock.
   * 
   * @param request the request to cleanup after due to departure of sender
   */
  protected void cleanupSuspendState(DLockRequestMessage request) {
    postReleaseLock(request.getRemoteThread(), request.getObjectName());
  }
  
  /** 
   * Drains newly permitted requests that have been removed from suspendQueue.
   * All requests in the permittedRequests queue already have permission to proceed
   * with granting or scheduling.
   * <p>
   * Caller must acquire destroyReadLock. Synchronizes on suspendLock,
   * grantTokens and each grant token.
   * 
   * Concurrency: protected by {@link #destroyLock} via invoking {@link #acquireDestroyReadLock(long)}
   */
  protected void drainPermittedRequests() {
    ArrayList drain = null;
    synchronized(suspendLock) {
      checkDestroyed();
      if (this.permittedRequests.isEmpty()) {
        return;
      }
      drain = this.permittedRequests;
      this.permittedRequestsDrain.add(drain);
      this.permittedRequests = new ArrayList();
    } // suspendLock sync
    
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS, "[drainPermittedRequests] draining {}", drain);
    }
    
    // iterate and attempt to grantOrSchedule each request
    for (Iterator iter = drain.iterator(); iter.hasNext();) {
      DLockRequestMessage request = (DLockRequestMessage) iter.next();
      checkDestroyed(); // destroyAndRemove should respond to all of these
      try {
        handlePermittedLockRequest(request); // synchronizes on grant instance
      }
      catch (LockGrantorDestroyedException e) {
        try {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS, "LockGrantorDestroyedException respondWithNotGrantor to {}", request);
          }
          request.respondWithNotGrantor();
        }
        finally {
          
        }
      }
      catch (LockServiceDestroyedException e) {
        try {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS, "LockServiceDestroyedException respondWithNotGrantor to {}", request);
          }
          request.respondWithNotGrantor();
        }
        finally {
          
        }
      }
      catch (RuntimeException e) {
        logger.error(LocalizedMessage.create(
            LocalizedStrings.DLockGrantor_PROCESSING_OF_POSTREMOTERELEASELOCK_THREW_UNEXPECTED_RUNTIMEEXCEPTION, e));
        request.respondWithException(e);
      }
      finally {
        
      }
    }
    
    synchronized(suspendLock) {
      checkDestroyed();
      this.permittedRequestsDrain.remove(drain);
    }
  }
  
  /**
   * Synchronizes on suspendLock.
   */
  private boolean acquireSuspendLockPermission(DLockRequestMessage request) {
    boolean permitLockRequest = false;
    final RemoteThread rThread = request.getRemoteThread();
    Assert.assertTrue(rThread != null);

    synchronized(suspendLock) {
      checkDestroyed();
      if (!dm.isCurrentMember(request.getSender())) {
        logger.info(LogMarker.DLS, LocalizedMessage.create(
            LocalizedStrings.DLockGrantor_IGNORING_LOCK_REQUEST_FROM_NONMEMBER_0, request));
        return false;
      }
      Integer integer = (Integer) readLockCountMap.get(rThread);
      int readLockCount = integer == null ? 0 : integer.intValue();
      boolean othersHaveReadLocks = totalReadLockCount > readLockCount;
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      if (isLockingSuspended() || writeLockWaiters > 0 || othersHaveReadLocks) {
        writeLockWaiters++;
        suspendQueue.addLast(request);
        this.thread.checkTimeToWait(calcWaitMillisFromNow(request), false);
        checkWriteLockWaiters();
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.acquireSuspend] added '{}' to end of suspendQueue.", request);
        }
      }
      else {
        permitLockRequest = true;
        suspendLocking(rThread, request.getLockId());
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.acquireSuspendLockPermission] permitted and suspended for {}", request);
        }
      }
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "[DLockGrantor.acquireSuspendLockPermission] new status  permitLockRequest = {}{}", permitLockRequest, displayStatus(rThread, null));
      }
    } // suspendLock sync
    return permitLockRequest;
  }

  /**
   * Synchronizes on suspendLock.
   */
  private boolean acquireReadLockPermission(DLockRequestMessage request) {
    boolean permitLockRequest = false;
    final RemoteThread rThread = request.getRemoteThread();
    Assert.assertTrue(rThread != null);
    synchronized(suspendLock) {
      checkDestroyed();
      if (!dm.isCurrentMember(request.getSender())) {
        logger.info(LogMarker.DLS, LocalizedMessage.create(LocalizedStrings.DLockGrantor_IGNORING_LOCK_REQUEST_FROM_NONMEMBER_0, request));
        return false;
      }
      Integer integer = (Integer) readLockCountMap.get(rThread);
      int readLockCount = integer == null ? 0 : integer.intValue();
      boolean threadHoldsLock = 
	  readLockCount > 0 || isLockingSuspendedBy(rThread);
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      if (!threadHoldsLock &&
         (isLockingSuspended() || writeLockWaiters > 0)) {
        suspendQueue.addLast(request);
        this.thread.checkTimeToWait(calcWaitMillisFromNow(request), false);
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.acquireReadLockPermission] added {} to end of suspendQueue.", request);
        }
      }
      else {
        readLockCount++;
        readLockCountMap.put(rThread, Integer.valueOf(readLockCount));
        totalReadLockCount++;
        permitLockRequest = true;
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.acquireReadLockPermission] permitted {}", request);
        }
      }
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "[DLockGrantor.acquireReadLockPermission] new status  threadHoldsLock = {} permitLockRequest = {}{}", threadHoldsLock, permitLockRequest, displayStatus(rThread, null));
      }
      checkTotalReadLockCount();
    } // suspendLock sync
    return permitLockRequest;
  }

  /** 
   * Returns true if lock request has permission to proceed; else adds the
   * request to the end of suspendQueue and returns false.
   * <p>
   * Synchronizes on suspendLock.
   * 
   * @param request the lock request to acquire permission for
   */
  private boolean acquireLockPermission(final DLockRequestMessage request) {
    if (logger.isTraceEnabled(LogMarker.DLS)) {
      logger.trace(LogMarker.DLS, "[DLockGrantor.acquireLockPermission] {}", request);
    }
    
    boolean permitLockRequest = false;
    if (request.getObjectName().equals(DLockService.SUSPEND_LOCKING_TOKEN)) {
      permitLockRequest = acquireSuspendLockPermission(request);
    }
    else {
      permitLockRequest = acquireReadLockPermission(request);
    }
    
    return permitLockRequest;
  }

  /** 
   * Throws InterruptedException if local lock request exists and is 
   * interruptible or CancelException if DistributionManager is forcing us to cancel
   * for shutdown.
   * 
   * @param e the throwable that caused this check
   */
  private void throwIfInterruptible(InterruptedException e) throws InterruptedException {
    // This needs to be first, otherwise user gets a complaint that
    // the TV is off when the problem is the house is burning down...
    this.dm.getCancelCriterion().checkCancelInProgress(e);
    
    if (this.dlock.isInterruptibleLockRequest()) {
      throw e;
    }
  }
  
  /**
   * TEST HOOK: Logs all grant tokens and other lock information for this 
   * service at INFO level. 
   * <p>
   * Synchronizes on grantTokens.
   */
  protected void dumpService() {
    synchronized (this.grantTokens) {
      StringBuffer buffer = new StringBuffer();
      buffer.append("DLockGrantor.dumpService() for ").append(this);
      buffer.append("\n").append(this.grantTokens.size()).append(" grantTokens\n");
      for (Iterator iter = this.grantTokens.entrySet().iterator(); iter.hasNext();) {
        Map.Entry entry = (Map.Entry)iter.next();
        buffer.append("    ").append(entry.getKey()).append(": ");
        DLockGrantToken token = (DLockGrantToken)entry.getValue();
        buffer.append(token.toString()).append("\n");
      }
      logger.info(LogMarker.DLS, LocalizedMessage.create(LocalizedStrings.TESTING, buffer));
      logger.info(LogMarker.DLS, LocalizedMessage.create(LocalizedStrings.TESTING, "\nreadLockCountMap:\n" + readLockCountMap));
    }
  }
  
  /**
   * Verify the waiters (for debugging)
   * 
   * @guarded.By {@link #suspendLock}
   */
  private void checkWriteLockWaiters() {
    if (!DEBUG_SUSPEND_LOCK) {
      return;
    }
    Assert.assertHoldsLock(this.suspendLock,true);
    int result = 0;
    Iterator it = this.suspendQueue.iterator();
    while (it.hasNext()) {
      DLockRequestMessage r = (DLockRequestMessage)it.next();
      if (r.isSuspendLockingRequest()) {
        result ++;
      }
    } // while
    Assert.assertTrue(result == this.writeLockWaiters);
  }
  
  /**
   * Debugging method
   * 
   * @guarded.By {@link #suspendLock}
   */
  private void checkTotalReadLockCount() {
    if (!DEBUG_SUSPEND_LOCK) {
      return;
    }
    Assert.assertHoldsLock(this.suspendLock,true);
    int result = 0;
    Iterator it = readLockCountMap.values().iterator();
    while (it.hasNext()) {
      result += ((Integer)it.next()).intValue();
    } 
    Assert.assertTrue(result == totalReadLockCount);
  }
  
  // -------------------------------------------------------------------------
  //   DLockGrantToken (static inner class)
  // -------------------------------------------------------------------------
  /**
   * Handles leasing and queued scheduling for an individual distributed lock.
   */
  public static class DLockGrantToken {
    
    /**
     * DLS which contains this lock. Reference is used for stats and lifecycle.
     */
    private final DLockService dlock;
    
    /**
     * Grantor instance that handles leasing of this lock.
     */
    private final DLockGrantor grantor;
    
    /** 
     * The uniquely identifying object name for this lock
     */
    private final Object lockName;
    
    /** 
     * Pending requests queued up for the lock
     * 
     * @guarded.By this
     */
    private LinkedList pendingRequests;
    
    /** 
     * The reply processor id is used to identify the specific lock operation
     * used by the lessee to lease this lock 
     * 
     * @guarded.By this
     */
    private int leaseId = -1;
    
    /** 
     * Distributed member that currently has a lease on this lock
     * 
     * @guarded.By this
     */
    private InternalDistributedMember lessee;

    /**
     * Absolute time in milliseconds when the current lease will expire.
     * When this lock is not leased out, the value is -1. When the lock is
     * leased out, the value is > 0. A value of Long.MAX_VALUE indicates a
     * non-expiring (infinite) lease.
     * 
     * @guarded.By this
     */
    private long leaseExpireTime = -1;
    
    /** 
     * Current count of threads attempting to access this grant token.
     * 
     * @guarded.By this
     */
    private int accessCount = 0;
    
    /** 
     * True if this token has been destroyed and removed from usage.
     * 
     * @guarded.By this
     */
    private boolean destroyed = false;
    
    /** 
     * RemoteThread identity of thread currently holding lease on this lock
     * 
     * @guarded.By this
     */
    private RemoteThread lesseeThread = null;
    
    /**
     * Instatiates a new instance of DLockGrantToken.
     * 
     * @param dlock the lock service scope for this lock
     * @param grantor the grantor handling locks for the lock service
     * @param name the name of this lock
     */
    protected DLockGrantToken(DLockService dlock,
                              DLockGrantor grantor,
                              Object name) {
      this.lockName = name;
      this.dlock = dlock;
      this.grantor = grantor;
    }

    /**
     * Schedules the lock request for immediate or later granting of lock.
     * This will grant the lock if it is available, otherwise it will add
     * the request at the end of the pending requests queue. 
     * <p>
     * Synchronizes on this grant token.
     * 
     * @param request the request to grant or schedule
     * @return true if the lock request was immediately granted
     */
    protected synchronized boolean schedule(DLockRequestMessage request) {
      if (!this.grantor.dm.isCurrentMember(request.getSender())) {
        this.grantor.cleanupSuspendState(request);
        return false;
      }
      
      if (!isGranted(false) && !hasWaitingRequests()) {
        // don't need to schedule... just grant it
        if (grantLockToRequest(request)) {
          return true;
        }
      }

      // add the request to the sorted set...
      if (logger.isTraceEnabled(LogMarker.DLS)) {
        logger.trace(LogMarker.DLS, "[DLockGrantToken.schedule] {} scheduling: {}", this, request);
      }
      if (this.pendingRequests == null) {
        this.pendingRequests = new LinkedList();
        this.dlock.getStats().incRequestQueues(1);
      }
      this.pendingRequests.add(request);
      this.dlock.getStats().incPendingRequests(1);
      return true;
    }
    
    /**
     * Sends NOT_GRANTOR replies to every request waiting for this grant token
     * and then destroys the grant token.
     * <p>
     * Synchronizes on this grant token.
     */
    protected synchronized void handleGrantorDestruction() {
      try {
        if (this.pendingRequests != null) {
          for (Iterator iter = this.pendingRequests.iterator(); iter.hasNext();) {
            DLockRequestMessage request = (DLockRequestMessage) iter.next();
            request.respondWithNotGrantor();
          }
        }
      }
      finally {
        destroy();
      }
    }
    
    /**
     * Checks current lock for expiration and attempts to grant the lock if
     * it is available.
     * <p>
     * Synchronizes on this grant token.
     * <p>
     * NOTE: expiration is only as accurate as clock synchronization on the
     *       hardware that the members are running on
     * probably should have Requestors handle expirations and send Release msg
     * - need an Evictor thread in each Requestor
     * 
     * @return the lease expiration time in millis for the currently held lock
     * or Long.MAX_VALUE if lock has no owner
     */
    protected synchronized long expireAndGrantLock() {
      // isGranted calls checkForExpiration...
      if (this.grantor.isDestroyed()) return Long.MAX_VALUE;
      if (!isGranted(true) && !this.grantor.isLockingSuspendedWithSync()) {
        grantLockToNextRequest();
      }
      long result = getLeaseExpireTime();
      if (result <=0) {
        result = Long.MAX_VALUE;
      }
      return result;
    }
    
    /**
     * Returns true if there are pending requests waiting to lock this grant
     * token. 
     * <p>
     * Caller must synchronize on this grant token.
     *
     * Concurrency: protected by synchronization of *this* DLockGrantToken
     * 
     * @return true if there are pending requests waiting to lock this
     */
    protected synchronized boolean hasWaitingRequests() {
      if (this.pendingRequests == null) return false;
      return !this.pendingRequests.isEmpty();
    }
    
    /**
     * Grant this lock to the request if possible. Returns true if lock was
     * granted to the request.
     * <p>
     * Synchronizes on this grant token.
     * 
     * @param request the lock request asking for this lock
     * @return true if lock was granted to the request
     */
    protected synchronized boolean grantLockToRequest(
                           DLockRequestMessage request) {
      Assert.assertTrue(request.getRemoteThread() != null); // KIRK search for these assertions and remove
      if (isGranted(true) || hasWaitingRequests()) {
        return false;
      }
      
      if (logger.isTraceEnabled(LogMarker.DLS)) {
        logger.trace(LogMarker.DLS, "[DLockGrantToken.grantLockToRequest] granting: {}", request);
      }
      
      long newLeaseExpireTime = grantAndRespondToRequest(request);
      if (newLeaseExpireTime == -1) return false;

      if (newLeaseExpireTime < Long.MAX_VALUE) {
        long now = DLockService.getLockTimeStamp(
            this.grantor.dm);
        this.grantor.thread.checkTimeToWait(newLeaseExpireTime - now, true);
      }
      
      return true;
    }      
    
    /**
     * Called to release a remote lock when processing a DLockReleaseMessage. 
     * <p>
     * Caller must synchronize on this grant token.
     * <p>
     * Call stack: DLockReleaseMessage -> releaseIfLocked -> 
     *             getAndReleaseGrantIfLockedBy -> grant.releaseIfLockedBy
     *             
     * Concurrency: protected by synchronization of *this* DLockGrantToken
     * 
     * @param owner the member to release the lock for
     * @param lockId the lock id that the member used to acquire the lock
     */
    protected void releaseIfLockedBy(InternalDistributedMember owner, 
                                     int lockId) {
      final RemoteThread rThread = getRemoteThread();
      boolean released = false;
      try {
        released = releaseLock(owner, lockId);
      }
      catch (IllegalStateException e) {
        this.dlock.checkDestroyed();
        this.grantor.checkDestroyed();
        // must have hit race... grantor doesn't have the token
        return;
      }
      if (released) {
        // don't bother synchronizing requests for this log statement...
        if (logger.isTraceEnabled(LogMarker.DLS)) {
          synchronized (this) {
            logger.trace(LogMarker.DLS, "[DLockGrantToken.releaseIfLockedBy] pending requests: {}", 
                (this.pendingRequests == null ? "none" : ""+this.pendingRequests.size()));
          }
        }
        Assert.assertTrue(rThread != null);
        // releaseIfLockedBy (remote unlock)
        this.grantor.postReleaseLock(rThread, getName());
        // note: DLockReleaseMessage calls drainPermittedRequests next...
      }
    }
    
    /**
     * Returns true if lock is currently leased by the owner with the
     * specified lock id.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * Concurrency: protected by synchronization of *this* DLockGrantToken
     * 
     * @param owner the member to check for lock ownership
     * @param lockId the lock id that the member used for locking
     * @return true if lock is currently leased by the owner with the
     * specified lock id
     */
    protected boolean isLockedBy(InternalDistributedMember owner, 
                                 int lockId) {
      return isLeaseHeldBy(owner, lockId);
    }
    
    /**
     * Handle timeouts for requests waiting on this lock. Any requests that
     * have timed out will be removed. Calculates and returns the next
     * smallest timeout of the requests still waiting on this lock.
     * <p>
     * Synchronizes on this grant token.
     * 
     * @return next smallest timeout of the requests still waiting on this lock
     */
    protected long handleRequestTimeouts() {
      long smallestTimeout = Long.MAX_VALUE;
      synchronized (this) {
        if (this.pendingRequests == null) return smallestTimeout;
        if (this.grantor.isDestroyed()) return smallestTimeout;
      }
      List timeouts = new ArrayList();
      
      // narrow timeouts to just contain requests that have timed out...
      DLockRequestMessage req = null;
      // ... copyRequests is synchronized on this ...
      synchronized (this) {
        for (Iterator iter = this.pendingRequests.iterator(); iter.hasNext();) {
          req = (DLockRequestMessage) iter.next();
          if (req.checkForTimeout()) { // sends DLockResponseMessage if timeout
            this.grantor.cleanupSuspendState(req);
            timeouts.add(req);
          }
          else {
            long timeout = req.getTimeoutTS();
            if (timeout < smallestTimeout) {
              smallestTimeout = timeout;
            }
          }
        }
        removeRequests(timeouts);
      }
      
      return smallestTimeout;
    }
    
    /**
     * Cleans up any state for the departed member. If the lock is held by
     * this member, it will be released. Any pending lock requests for this
     * member will be removed.
     * <p>
     * Synchronizes on this grant token, suspendLock, and grantTokens.
     * 
     * @param member the departed member
     */
    protected void handleDepartureOf(final InternalDistributedMember member,
                                     final ArrayList grantsToRemoveIfUnused) {
      boolean released = false;
      RemoteThread rThread = null;
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      try {
        synchronized (this) {
          try {
            if (isDestroyed()) return;
            if (this.pendingRequests == null) return;
            // remove member from pendingRequests...
            DLockRequestMessage req = null;
            for (Iterator iter = this.pendingRequests.iterator(); iter.hasNext();) {
              req = (DLockRequestMessage) iter.next();
              if (member.equals(req.getSender())) {
                // found departed member, respondWithNotHolder to end dlock stats
                try {
                  req.handleDepartureOfSender();
                  // cleanup suspend state for this request
                  this.grantor.cleanupSuspendState(req);
                }
                catch (CancelException e) {
                  if (isDebugEnabled_DLS) {
                    logger.trace(LogMarker.DLS, "[DLockGrantToken.handleDepartureOf] ignored cancellation (1)");
                  }
                }
                // remove the request
                iter.remove();
                this.dlock.getStats().incPendingRequests(-1);
              }
            }
          }
          finally {
            synchronized (this) {
              // bugfix 32657 release lock AFTER removing member from queued requests
              //   because release will grant to first request in queued requests
              rThread = getRemoteThread();
              boolean releasedToken = false;
              try {
                releasedToken = releaseLock(member, getLockId());
              }
              catch (IllegalStateException e) {
                this.dlock.checkDestroyed();
                this.grantor.checkDestroyed();
                // must have hit race... grantor doesn't have the token
                return;
              }
              if (releasedToken) {
                released = true;
                if (isDebugEnabled_DLS) {
                  logger.trace(LogMarker.DLS, "[DLockGrantToken.handleDepartureOf] pending requests: {}", (this.pendingRequests == null ? "none" : ""+this.pendingRequests.size()));
                }
                Assert.assertTrue(rThread != null);
              }
            }
          }
        }
      }
      finally {
        if (released) {
          try {
            this.grantor.postReleaseLock(rThread, getName());
          }
          catch (CancelException e) {
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS, "[DLockGrantToken.handleDepartureOf] ignored cancellation (2)");
            }
          }
          this.grantor.drainPermittedRequests(); // destroyReadLock{grant{}, suspendLock{}}
          grantsToRemoveIfUnused.add(this);
        }
      }
    }
    
    /**
     * Adds this grant to the list if it references the departed member. 
     * <p>
     * Synchronizes on this grant token.
     * 
     * @param member the departed member
     * @param grantsReferencingMember list to add grant to if it references
     * departed member
     */
    protected synchronized void checkDepartureOf(
                           final InternalDistributedMember member,
                           final List grantsReferencingMember) {
      
      if (this.destroyed) {
        return;
      }
      if (member.equals(this.lessee)) {
        grantsReferencingMember.add(this);
        return;
      }
      if (this.pendingRequests != null) {
        DLockRequestMessage req = null;
        for (Iterator iter = this.pendingRequests.iterator(); iter.hasNext();) {
          req = (DLockRequestMessage) iter.next();
          if (member.equals(req.getSender())) {
            grantsReferencingMember.add(this);
            return;
          }
        }
      }
    }
    
    /**
     * Remove all the specified pending requests.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @param requestsToRemove the pending requests to remove
     * @guarded.By this
     */
    private void removeRequests(Collection requestsToRemove) {
      if (!requestsToRemove.isEmpty()) {
        synchronized (this) {
          this.pendingRequests.removeAll(requestsToRemove);
        }
        this.dlock.getStats().incPendingRequests(-requestsToRemove.size());
      }
    }
    
    /**
     * Grants this lock to the next waiting request if one exists.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * Concurrency: protected by synchronization of *this* DLockGrantToken
     * 
     * @return true if the lock was granted to next request
     */
    protected boolean grantLockToNextRequest() {
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "[DLockGrantToken.grantLock] {} isGranted={} hasWaitingRequests={}", getName(), isLeaseHeld(), hasWaitingRequests());
      }
                                                          
      while (!isGranted(true) && hasWaitingRequests()) {
        try {
          // get request at front of queue...
          DLockRequestMessage request = null;
          synchronized (this) {
            request = (DLockRequestMessage) this.pendingRequests.remove(0);
          }
          this.dlock.getStats().incPendingRequests(-1);
        
          // grant lock to the request unless it is timed out... 
          if (request.checkForTimeout()) {
            this.grantor.cleanupSuspendState(request);
            continue;
          }
          
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS, "[DLockGrantToken.grantLock] granting {} to {}", getName(), request.getSender());
          }
          
          long newLeaseExpireTime = grantAndRespondToRequest(request);
          if (newLeaseExpireTime == -1) continue;
          
          if (newLeaseExpireTime < Long.MAX_VALUE) {
            long now = DLockService.getLockTimeStamp(
                this.grantor.dm);
            this.grantor.thread.checkTimeToWait(newLeaseExpireTime - now, true);
          }
      
        }
        catch (IndexOutOfBoundsException e) {
          // ignore... entry may have timed out between empty check and remove
        }
      }
      
      return isGranted(false);
    }
    
    /** 
     * Grants the lock to the specified request and sends a reply to the
     * member that initiated the request.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @param request the request to grant the lock to
     * @return leaseExpireTime or -1 if failed to grant. 
     * @guarded.By this
     */
    private long grantAndRespondToRequest(DLockRequestMessage request) {
      
      synchronized (request) {
        if (request.respondedNoSync()) {
          return -1;
        }
          
        Assert.assertTrue(request.getRemoteThread() != null);
        if (!this.grantor.dm.isCurrentMember(request.getSender())) {
          this.grantor.cleanupSuspendState(request);
          return -1;
        }
        
        if (isSuspendLockingToken()) {
          synchronized (this.grantor.suspendLock) {
            Assert.assertTrue(
                this.grantor.lockingSuspendedBy == null ||
                this.grantor.isLockingSuspendedBy(request.getRemoteThread()),
                "Locking is suspended by " + this.grantor.lockingSuspendedBy +
                " with lockId of " + this.grantor.suspendedLockId +
                " instead of " + request.getRemoteThread() +
                " with lockId of " + request.getLockId());
          } // suspendLock sync
        }
        
        long newLeaseExpireTime = calcLeaseExpireTime(request.getLeaseTime());
        
        grantLock(request.getSender(), 
                  newLeaseExpireTime, 
                  request.getLockId(), 
                  request.getRemoteThread());
        
        if (isSuspendLockingToken()) {
          synchronized (this.grantor.suspendLock) {
            // no-op if already suspend by this RemoteThread...
            this.grantor.suspendLocking(request.getRemoteThread(), request.getLockId());
            Assert.assertTrue(
                this.grantor.isLockingSuspendedBy(request.getRemoteThread()),
                "Locking should now be suspended by " + request.getRemoteThread() +
                " with lockId of " + request.getLockId() +
                " instead of " + this.grantor.lockingSuspendedBy +
                " with lockId of " + this.grantor.suspendedLockId);
          } // suspendLock sync
        }
  
        // NOTE: if grantor is local and client interrupts the request, the
        //   following will release the lock because the reply processor is gone
        request.respondWithGrant(newLeaseExpireTime);
        if (!isLeaseHeldBy(request.getSender(), request.getLockId())) {
          // lock request was local and interrupted then released
          return -1;
        }
        return newLeaseExpireTime;
      }
    }
    
    /**
     * Returns the absolute time at which the specified lease time will expire
     * from now. This call does not change or check any state other than 
     * current time.
     * 
     * @param leaseTime the desired length of lease time
     * @return the absolute time at which the lease will expire
     */
    protected long calcLeaseExpireTime(long leaseTime) {
      if (leaseTime == Long.MAX_VALUE || leaseTime == -1) {
        return Long.MAX_VALUE;
      }
      
      long currentTime = getCurrentTime();
      long newLeaseExpireTime = currentTime + leaseTime;
      if (newLeaseExpireTime < leaseTime) { // rolled over MAX_VALUE...
        newLeaseExpireTime = Long.MAX_VALUE;
      }
      if (logger.isTraceEnabled(LogMarker.DLS)) {
        logger.trace(LogMarker.DLS, "[DLockGrantToken.calcLeaseExpireTime] currentTime={} newLeaseExpireTime={}", currentTime, newLeaseExpireTime);
      }
      return newLeaseExpireTime;
    }
    
    /**
     * Returns true if this grant token is currently granted.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * Concurrency: protected by synchronization of *this* DLockGrantToken
     * 
     * @param checkForExpiration true if expiration should be attempted before
     * checking if this grant token is currently granted
     * @return true if this grant token is currently granted
     */
    protected boolean isGranted(boolean checkForExpiration) {
      if (checkForExpiration) {
        checkForExpiration();
      }
      return isLeaseHeld(); 
    }

    /**
     * Creates a string of the pending requests for logging or debugging.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @return a string of the pending requests for logging or debugging
     * @guarded.By this
     */
    private String pendingRequestsToString() {
      if (this.pendingRequests == null) {
        return "(null)";
      }
      StringBuffer sb = new StringBuffer();
      Iterator it = this.pendingRequests.iterator();
      while (it.hasNext()) {
        Object req = it.next();
        sb.append("[");
        sb.append(req.toString());
        sb.append("]");
      }
      return sb.toString();
    }
    
    /**
     * Returns string representation of this object.
     * <p>
     * Synchronizes on this grant token.
     */
    @Override
    public String toString() {
      return toString(true);
    }
    
    /**
     * Returns string representation of this object.
     * <p>
     * Synchronizes on this grant token.
     * <p>
     * @param displayPendingRequests true if string should include pendingRequests
     */
    public String toString(boolean displayPendingRequests) {
      StringBuffer sb = new StringBuffer("DLockGrantToken");
      sb.append("@").append(Integer.toHexString(hashCode()));
      synchronized(this) {
        sb.append(" {name: ").append(getName());
        sb.append(", isGranted: ").append(isLeaseHeld());
        sb.append(", isDestroyed: ").append(this.destroyed);
        sb.append(", accessCount: ").append(this.accessCount);
        sb.append(", lessee: ").append(this.lessee);
        sb.append(", leaseExpireTime: ").append(this.leaseExpireTime);
        sb.append(", leaseId: ").append(this.leaseId);
        sb.append(", lesseeThread: ").append(this.lesseeThread);
        if (displayPendingRequests) {
          sb.append(", pendingRequests: ").append(pendingRequestsToString());
        }
        sb.append("}");
      }
      return sb.toString();
    }
    
    /**
     * Returns the name of this lock.
     * 
     * @return the name of this lock
     */
    Object getName() {
      return this.lockName;
    }
    
    /**
     * Returns true if this lock represents suspend locking.
     * 
     * @return true if this lock represents suspend locking
     * @see com.gemstone.gemfire.distributed.DistributedLockService#suspendLocking(long)
     */
    boolean isSuspendLockingToken() {
      return DLockService.SUSPEND_LOCKING_TOKEN.equals(this.lockName);
    }
    
    /**
     * Returns the lock id used to lease this lock.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @return the lock id used to lease this lock
     * @guarded.By this
     */
    int getLockId() {
      return this.leaseId;
    }
    
    /**
     * Returns the identity of the thread that has this lock leased.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @return the identity of the thread that has this lock leased
     * @guarded.By this
     */
    RemoteThread getRemoteThread() {
      return this.lesseeThread;
    }
    
    /**
     * Increments or decrements access count by the specified amount.
     * <p>
     * Synchronizes on this grant token.
     * 
     * @param amount the amount to inc or dec access count by
     */
    private synchronized void incAccess(int amount) {
      if (amount < 0) {
        Assert.assertTrue(this.accessCount-amount >= 0, amount +
            " cannot be subtracted from accessCount " + this.accessCount);
      }
      this.accessCount += amount;
    }
    
    /**
     * Increments the access count by one.
     * <p>
     * Synchronizes on this grant token.
     */
    void incAccess() {
      incAccess(1);
    }
    
    /**
     * Decrements the access count by one.
     * <p>
     * Synchronizes on this grant token.
     */
    void decAccess() {
      incAccess(-1);
    }
    
    /**
     * Returns true if the access count is greater than zero.
     * <p>
     * Synchronizes on this grant token.
     * 
     * @return true if the access count is greater than zero
     */
    boolean isBeingAccessed() {
      synchronized (this) {
        return this.accessCount > 0;
      }
    }

    /**
     * Returns the member that currently holds a lease on this lock.
     * <p>
     * Synchronizes on this grant token.
     * 
     * @return the member that currently holds a lease on this lock
     */
    public synchronized InternalDistributedMember getOwner() {
      return this.lessee;
    }
    
    /**
     * Returns the lease expiration time. This the absolute time in milliseconds
     * when the current lease will expire.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * Concurrency: protected by synchronization of *this* DLockGrantToken
     * 
     * @return the lease expiration time
     */
    public long getLeaseExpireTime() {
      return this.leaseExpireTime;
    }
    
    /**
     * Returns true if this grant token has been destroyed.
     * <p>
     * Synchronizes on this grant token.
     * 
     * @return true if this grant token has been destroyed
     */
    public synchronized boolean isDestroyed() {
      return this.destroyed;
    }
    
    /**
     * Returns the current time in milliseconds.
     * 
     * @return the current time in milliseconds
     */
    long getCurrentTime() {
      return DLockService.getLockTimeStamp(this.grantor.dm);
    }
    
    /**
     * Handle expiration if the lease expire time has been reached for the
     * current lease on this grant token.
     * <p>
     * Synchronizes on this grant token.
     * 
     * @return true if the lease is expired
     */
    synchronized boolean checkForExpiration() {
      if (this.lessee != null && this.leaseId > -1) {
        if (this.leaseExpireTime == Long.MAX_VALUE) return false;
        long currentTime = getCurrentTime();
        if (currentTime > this.leaseExpireTime) {
          // expired!
          
          final RemoteThread rThread = this.lesseeThread;
          
          this.lessee = null;
          this.leaseId = -1;
          this.lesseeThread = null;
          this.leaseExpireTime = -1;

          if (logger.isTraceEnabled(LogMarker.DLS)) {
            logger.trace(LogMarker.DLS, "[checkForExpiration] Expired token at {}: {}", currentTime, toString(true));
          }
          
          this.grantor.postReleaseLock(rThread, this.lockName);
          
          return true;
        }
        /*else if (this.log.fineEnabled()) {
          this.log.fine("[checkForExpiration] not expired: " + this);
        }*/
      }
      return false;
    }
    
    /**
     * Grants this lock.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @param owner the member that is being granted the lock
     * @param newLeaseExpireTime the absolute expiration time
     * @param lockId the lock id used to request the lock
     * @param remoteThread identity of the locking thread
     * @guarded.By this
     */
    void grantLock(InternalDistributedMember owner, 
                   long newLeaseExpireTime, 
                   int lockId,
                   RemoteThread remoteThread) {
      Assert.assertTrue(remoteThread != null);
      checkDestroyed();
      basicGrantLock(owner, newLeaseExpireTime, lockId, remoteThread);
    }  
    
    /**
     * Modify grant token state to mark the lock as granted.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @param owner the member that has been granted the lock
     * @param newLeaseExpireTime the absolute expiration time
     * @param lockId the lock id used to request the lock
     * @param remoteThread identity of the locking thread
     * @guarded.By this
     */
    private void basicGrantLock(InternalDistributedMember owner, 
                                long newLeaseExpireTime, 
                                int lockId,
                                RemoteThread remoteThread) {
      Assert.assertTrue(remoteThread != null);
      Assert.assertTrue(lockId > -1, "Invalid attempt to grant lock with lockId " + lockId);
      this.lessee = owner;
      this.leaseExpireTime = newLeaseExpireTime;
      this.leaseId = lockId;
      this.lesseeThread = remoteThread;
      if (logger.isTraceEnabled(LogMarker.DLS)) {
        logger.trace(LogMarker.DLS, "[DLockGrantToken.grantLock.grantor] Granting {}", toString(false));
      }
    }

    /**
     * Returns true if this lock is currently leased out.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @return true if this lock is currently leased out
     * @guarded.By this
     */
    boolean isLeaseHeld() {
      return this.lessee != null && this.leaseId > -1;
    }

    /**
     * Mark this grant token as destroyed. This should only happen to a token
     * that is no longer in use.
     * <p>
     * Caller must synchronize on this grant token.
     * @guarded.By this
     */
    void destroy() {
      if (!this.destroyed) {
        this.destroyed = true;
        this.dlock.getStats().incGrantTokens(-1);
        if (this.pendingRequests != null) {
          this.dlock.getStats().incPendingRequests(-this.pendingRequests.size());
          this.dlock.getStats().incRequestQueues(-1);
        }
      }
    }

    /**
     * Throws IllegalStateException if this grant token has been destroyed.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @throws IllegalStateException if this grant token has been destroyed
     * @guarded.By this
     */
    private void checkDestroyed() {
      if (this.destroyed) {
        String s = "Attempting to use destroyed grant token: " + this;
        IllegalStateException e = new IllegalStateException(s);
        //log.warning(e); -enable for debugging
        throw e;
      }
    }
    
    /** 
     * Called by the grantor. Releases lock on this token if it is currently
     * locked by the specified member and lockId.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @param member the member to release the lock from
     * @param lockId the lock id that the member used when locking
     * @return true if lock was released
     * @guarded.By this
     */
    private boolean releaseLock(InternalDistributedMember member, 
                                int lockId) {
      if (lockId == -1) return false;
      checkDestroyed();
      
      if (isLeaseHeldBy(member, lockId)) {
        if (logger.isTraceEnabled(LogMarker.DLS)) {
          logger.trace(LogMarker.DLS, "[DLockGrantToken.releaseLock] releasing ownership: {}", this);
        }
        
        this.lessee = null;
        this.leaseId = -1;
        this.lesseeThread = null;
        this.leaseExpireTime = -1;
        
        return true;
      }
      if (logger.isTraceEnabled(LogMarker.DLS)) {
        logger.trace(LogMarker.DLS, "[DLockGrantToken.releaseLock] {} attempted to release: {}", member, this);
      }
      return false;
    }
    
    /**
     * Returns true if the sender holds a lease on this lock using lockId.
     * <p>
     * Caller must synchronize on this grant token.
     * 
     * @param sender the member that potentially holds a lease
     * @param lockId the lock id provided by the member
     * @return true if the sender holds a lease on this lock
     * @guarded.By this
     */
    private boolean isLeaseHeldBy(InternalDistributedMember sender, 
                                       int lockId) {
      Assert.assertTrue(sender != null, "sender is null: " + this);
      Assert.assertTrue(lockId > -1, "lockId is < 0: " + this);
      return sender.equals(this.lessee) && lockId == this.leaseId;
    }
  }
  
  // -------------------------------------------------------------------------
  //   DLockGrantorThread (static inner class)
  // -------------------------------------------------------------------------
  /**
   * Thread dedicated to handling background tasks for this grantor.
   */
  private static class DLockGrantorThread extends Thread {
    private static final long MAX_WAIT = 60 * 1000; // 60 seconds...
    private volatile boolean shutdown = false;
    private boolean waiting = false;
    private boolean requireTimeToWait = false;
    private boolean goIntoWait = false;
    private long timeToWait = MAX_WAIT;
    private long expectedWakeupTimeStamp = 0;
    private final Object lock = new Object();
    private final DLockGrantor grantor;
    private final CancelCriterion stopper;

    /** Time in millis that next pending request will timeout */
    private long nextTimeout = DLockGrantorThread.MAX_WAIT;
    
    /** Time in millis that next lock is due to expire */
    private long nextExpire = DLockGrantorThread.MAX_WAIT;
    
    DLockGrantorThread(DLockGrantor grantor, CancelCriterion stopper) { 
      super(DLockService.getThreadGroup(), 
            "Lock Grantor for " + grantor.dlock.getName());
      setDaemon(true);
      this.grantor = grantor;
      this.stopper = stopper;
    }

    private long now() {
      DM dm = this.grantor.dlock.getDistributionManager();
      return DLockService.getLockTimeStamp(dm);
    }
    
    protected void shutdown() {
      this.shutdown = true;
      this.interrupt();
    }
    
    protected void checkTimeToWait(long newTimeToWaitArg, boolean expire) {
      long newTimeToWait = newTimeToWaitArg;
      if (newTimeToWait == Long.MAX_VALUE) {
        // never expire
        return;
      } else if (newTimeToWait < 0) {
        // negative means already expired or timed out so we wakeup immediately
        newTimeToWait = 0;
      }

      synchronized(this.lock) {
        if (expire && newTimeToWait < this.nextExpire) {
          this.nextExpire = newTimeToWait;
        }
        if (!expire && newTimeToWait < this.nextTimeout) {
          this.nextTimeout = newTimeToWait;
        }

        if (newTimeToWait < this.timeToWait) {
          if (this.waiting) {
            long newWakeupTimeStamp = now()+newTimeToWait;
            if (newWakeupTimeStamp > -1 // accounts for overflow
                && newWakeupTimeStamp < this.expectedWakeupTimeStamp) {
              this.timeToWait = newTimeToWait;
              this.requireTimeToWait = true;
              this.goIntoWait = true;
              this.lock.notify();
            }
            /*if (this.log.fineEnabled()) {
              this.log.fine("[DLockGrantorThread.checkTimeToWait.k2]" +
                  " newTimeToWait=" + newTimeToWait +
                  " expire=" + expire +
                  " newWakeupTimeStamp=" + newWakeupTimeStamp +
                  " expectedWakeupTimeStamp=" + expectedWakeupTimeStamp +
                  " nextExpire=" + this.nextExpire +
                  " nextTimeout=" + this.nextTimeout +
                  " timeToWait=" + this.timeToWait +
                  " goIntoWait=" + this.goIntoWait
                  );
            }*/
          }
          else {
            this.timeToWait = newTimeToWait;
            this.requireTimeToWait = true;
            /*if (this.log.fineEnabled()) {
              this.log.fine("[DLockGrantorThread.checkTimeToWait.k3]" +
                  " newTimeToWait=" + newTimeToWait +
                  " expire=" + expire +
                  " expectedWakeupTimeStamp=" + expectedWakeupTimeStamp +
                  " nextExpire=" + this.nextExpire +
                  " nextTimeout=" + this.nextTimeout +
                  " timeToWait=" + this.timeToWait +
                  " goIntoWait=" + this.goIntoWait
                  );
            }*/
          }
        } // end if newTimeToWait
        
        /*else if (this.log.fineEnabled()) {
          this.log.fine("[DLockGrantorThread.checkTimeToWait.k4]" +
              " newTimeToWait=" + newTimeToWait +
              " expire=" + expire +
              " expectedWakeupTimeStamp=" + expectedWakeupTimeStamp +
              " nextExpire=" + this.nextExpire +
              " nextTimeout=" + this.nextTimeout +
              " timeToWait=" + this.timeToWait +
              " goIntoWait=" + this.goIntoWait
              );
        }*/
      } // end sync this.lock
    }

    @Override
    public void run() {
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      
      DistributedLockStats stats = this.grantor.dlock.getStats();
      boolean recalcTimeToWait = false;
      while (!this.shutdown) {
//        SystemFailure.checkFailure(); stopper checks this
        if (stopper.cancelInProgress() != null) {
          break; // done
        }
        try {
          // go into wait if we know we have no timeouts or expires for a while
          synchronized(this.lock) { // synchronized
            if (recalcTimeToWait || this.requireTimeToWait) {
              recalcTimeToWait = false;
              long nextTS = Math.min(this.nextExpire, this.nextTimeout);
              this.nextExpire = Long.MAX_VALUE;
              this.nextTimeout = Long.MAX_VALUE;
              if (nextTS != Long.MAX_VALUE || this.requireTimeToWait) {
                this.requireTimeToWait = false;
                long now = now();
                
                // fix bug 39355 by using current timeToWait if smaller
                long newTimeToWait = nextTS - now;
                if (this.requireTimeToWait) {
                  this.timeToWait = Math.min(this.timeToWait, newTimeToWait);
                }
                else {
                  this.timeToWait = newTimeToWait;
                }
                
                if (this.timeToWait < 0) this.timeToWait = 0;
                if (isDebugEnabled_DLS) {
                  logger.trace(LogMarker.DLS, "DLockGrantorThread will wait for {} ms. nextExpire={} nextTimeout={} now={}",
                      this.timeToWait, this.nextExpire, this.nextTimeout, now);
                }
              } else {
                this.timeToWait = Long.MAX_VALUE;
                if (isDebugEnabled_DLS) {
                  logger.trace(LogMarker.DLS, "DLockGrantorThread will wait until rescheduled.");
                }
              }
            }
            if (this.timeToWait > 0) {
              if (isDebugEnabled_DLS) {
                logger.trace(LogMarker.DLS, "DLockGrantorThread is about to wait for {} ms.", this.timeToWait);
              }
              if (this.timeToWait != Long.MAX_VALUE) {
                this.expectedWakeupTimeStamp = now() + this.timeToWait;
                if (this.expectedWakeupTimeStamp < 0) {
                  // overflow
                  this.expectedWakeupTimeStamp = Long.MAX_VALUE;
                }
              } else {
                this.expectedWakeupTimeStamp = Long.MAX_VALUE;
              }
              if (this.expectedWakeupTimeStamp == Long.MAX_VALUE) {
                while (!this.goIntoWait) {
                  this.waiting = true;
                  this.lock.wait(); // spurious wakeup ok
                  this.waiting = false;
                }
              }
              else {
                long timeToWaitThisTime = this.timeToWait;
                for (;;) {
                  this.waiting = true;
                  this.lock.wait(timeToWaitThisTime); // spurious wakeup ok
                  this.waiting = false;
                  if (this.goIntoWait) break; // out of for loop
                  timeToWaitThisTime = this.expectedWakeupTimeStamp - now();
                  if (timeToWaitThisTime <= 0) break; // out of for loop
                }
              }
              if (isDebugEnabled_DLS) {
                logger.trace(LogMarker.DLS, "DLockGrantorThread has woken up...");
              }
              if (this.shutdown) break;
              // if goIntoWait, continue back around and enter wait again
              if (this.goIntoWait) {
                this.goIntoWait = false;
                continue;
              }
            }
          } // synchronized
          long statStart = stats.startGrantorThread();
          try {
            Collection grants = this.grantor.snapshotGrantTokens();
          
            // TASK: expire and grant locks
            if (this.shutdown) {
              return;
            }
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS, "DLockGrantorThread about to expireAndGrantLocks...");
            }
            {
              long smallestExpire = this.grantor.expireAndGrantLocks(grants.iterator());
              synchronized(this.lock) {
                if (smallestExpire < this.nextExpire) {
                  this.nextExpire = smallestExpire;
                }

              }
            }
            long timing = stats.endGrantorThreadExpireAndGrantLocks(statStart);
  
            // TASK: timeout waiting requests
            if (this.shutdown) {
              return;
            }
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS, "DLockGrantorThread about to handleRequestTimeouts...");
            }
            {
              long smallestRequestTimeout = this.grantor.handleRequestTimeouts(grants.iterator());
              long smallestSuspendTimeout = this.grantor.handleSuspendTimeouts();
              synchronized(this.lock) {
                if (smallestRequestTimeout < this.nextTimeout) {
                  this.nextTimeout = smallestRequestTimeout;
                }
                if (smallestSuspendTimeout < this.nextTimeout) {
                  this.nextTimeout = smallestSuspendTimeout;
                }
              }
            }
            timing = stats.endGrantorThreadHandleRequestTimeouts(timing);
  
            // TASK: remove unused tokens
            if (this.shutdown) {
              return;
            }
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS, "DLockGrantorThread about to removeUnusedGrants...");
            }
            this.grantor.removeUnusedGrants(grants.iterator());
            stats.endGrantorThreadRemoveUnusedTokens(timing);
  
          }
          catch (CancelException e) {
            // so, exit then.
          }
          finally {
            recalcTimeToWait = true;
            stats.endGrantorThread(statStart);
          }
          
        } 
        catch (InterruptedException e) {
          // shutdown probably interrupted us
          
          // Not necessary to reset the interrupt bit, we're going to go
          // away of our own accord.
          
          if (this.shutdown) {
            // ok to ignore since this thread will now shutdown
          }
          else {
            logger.warn(LocalizedMessage.create(LocalizedStrings.DLockGrantor_DLOCKGRANTORTHREAD_WAS_UNEXPECTEDLY_INTERRUPTED), e);
            // do not set interrupt flag since this thread needs to resume
            stopper.checkCancelInProgress(e);
          }
        }
//        catch (VirtualMachineError err) {
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch (Throwable e) {
//          // Whenever you catch Error or Throwable, you must also
//          // catch VirtualMachineError (see above).  However, there is
//          // _still_ a possibility that you are dealing with a cascading
//          // error condition, so you also need to check to see if the JVM
//          // is still usable:
//          SystemFailure.checkFailure();
//          this.log.warning(LocalizedStrings.DLockGrantor_DLOCKGRANTORTHREAD_CAUGHT_EXCEPTION, e);
//        }
        finally {
          
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  //   MembershipListener inner classes
  // -------------------------------------------------------------------------
  
  /** Detects loss of the lock grantor and initiates grantor recovery. */
  private MembershipListener membershipListener = new MembershipListener() {
    public void memberJoined(InternalDistributedMember id) {
    }
    public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
    }
    public void memberSuspect(InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {
    }
    public void memberDeparted(final InternalDistributedMember id, final boolean crashed) {
      final DLockGrantor me = DLockGrantor.this;
      final DM distMgr = me.dlock.getDistributionManager();
      // if the VM is being forcibly disconnected, we shouldn't release locks as it
      // will take longer than the time allowed by the InternalDistributedSystem
      // shutdown mechanism.
      if (distMgr.getCancelCriterion().cancelInProgress() != null) {
        return;
      }
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      try {
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.memberDeparted] waiting thread pool will process id={}", id);
        }
        distMgr.getWaitingThreadPool().execute(new Runnable() {
          public void run() {
            try {
              processMemberDeparted(id, crashed, me);
            }
            catch (InterruptedException e) {
              // ignore
              if (isDebugEnabled_DLS) {
                logger.trace(LogMarker.DLS, "Ignored interrupt processing departed member");
              }
            }
          }
        });
      }
      catch (RejectedExecutionException e) {
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[DLockGrantor.memberDeparted] rejected handling of id={}", id);
        }
      }
    }
    protected void processMemberDeparted(InternalDistributedMember id, boolean crashed, DLockGrantor me) 
        throws InterruptedException {
      if (logger.isTraceEnabled(LogMarker.DLS)) {
        logger.trace(LogMarker.DLS, "[DLockGrantor.processMemberDeparted] id={}", id);
      }
      try {
        me.waitWhileInitializing();
  
        // one cause of bug 32657 is "if (crashed) {" around handleDepartureOf
        // ... we cannot rely on the value of crashed to determine if grantor
        // has or will receive a NonGrantorDestroy message
        me.handleDepartureOf(id);
      }
      catch (LockGrantorDestroyedException e) {
        // ignore... grantor was destroyed
      } // outer try-catch
    }
  };

}

