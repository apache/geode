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

package org.apache.geode.distributed.internal.locks;

import static java.util.Collections.emptySet;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.LeaseExpiredException;
import org.apache.geode.distributed.LockNotHeldException;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.deadlock.UnsafeThreadLocal;
import org.apache.geode.distributed.internal.locks.DLockQueryProcessor.DLockQueryReplyMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.internal.util.concurrent.FutureResult;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Implements the distributed locking service with distributed lock grantors.
 *
 */
public class DLockService extends DistributedLockService {

  private static final Logger logger = LogService.getLogger();

  private static final long NOT_GRANTOR_SLEEP = Long
      .getLong(GeodeGlossary.GEMFIRE_PREFIX + "DLockService.notGrantorSleep", 100);

  private static final boolean DEBUG_NONGRANTOR_DESTROY_LOOP = Boolean
      .getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "DLockService.debug.nonGrantorDestroyLoop");

  private static final int DEBUG_NONGRANTOR_DESTROY_LOOP_COUNT = Integer
      .getInteger(
          GeodeGlossary.GEMFIRE_PREFIX + "DLockService.debug.nonGrantorDestroyLoopCount", 20);

  private static final boolean AUTOMATE_FREE_RESOURCES =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "DLockService.automateFreeResources");

  static final int INVALID_LEASE_ID = -1;

  /** Unique name for this instance of the named locking service */
  protected final String serviceName;

  /** DistributionManager for this member */
  private final DistributionManager dm;

  /**
   * DistributedSystem connection for this member (used for DisconnectListener, logging, etc)
   */
  protected final InternalDistributedSystem ds;

  /** Known lock tokens for this service. Key:Object(name), Value:DLockToken */
  private final Map<Object, DLockToken> tokens = new HashMap<>();

  /**
   * True if this member has destroyed this named locking service. Field is volatile only because
   * it's referenced in {@link #toString()} (never synchronize in <code>toString</code>).
   */
  private volatile boolean destroyed = false;

  /**
   * True if this is a distributed lock service; false if local to this vm only. TX has a "local"
   * lock service which sets this to false.
   */
  private final boolean isDistributed;

  /** Optional handler for departure of lease holders; used by grantor */
  private DLockLessorDepartureHandler lessorDepartureHandler;

  /**
   * Hook for transactions which allows custom behavior in processing DLockRecoverGrantorMessage
   */
  private DLockRecoverGrantorProcessor.MessageProcessor recoverGrantorProcessor;

  /** Thread-safe reference to DistributedLockStats */
  private final DistributedLockStats dlockStats;

  /**
   * Protects {@link #lockGrantorId}, {@link #grantor} and {@link #lockGrantorFutureResult}. Final
   * granting of a lock occurs under this synchronization and only if <code>lockGrantorId</code>
   * matches the grantor that granted the lock.
   */
  private final Object lockGrantorIdLock = new Object();

  /** Identifies the current grantor for this lock service. */
  private LockGrantorId lockGrantorId;

  /**
   * Local instance of the lock grantor if this process is the grantor. This field is volatile for
   * one use: 1) {@link #toString()} which should not use synchronization due to potential for wrong
   * lock ordering. Can we make this non-volatile??
   */
  private volatile DLockGrantor grantor;

  /**
   * Count of currently active locks and lock requests. Used to determine if destroy must tell the
   * grantor to release all held locks.
   */
  private int activeLocks = 0;

  /** True if this service should automatically freeResources */
  private final boolean automateFreeResources;

  /** Identifies the thread that is destroying this lock service. */
  private final ThreadLocal<Boolean> destroyingThread = new ThreadLocal<>();

  /// ** Held during destroy and creation of this lock service. */
  // private final Object serviceLock = new Object();

  /** Protects access to {@link #destroyed} and {@link #activeLocks}. */
  private final Object destroyLock = new Object();

  /**
   * Created by the thread communicating directly with the elder. Other threads will wait on this
   * and then use the resulting lockGrantorId. This ensures that only one message is sent to the
   * elder and that only one thread does so at a time. Protected by {@link #lockGrantorIdLock} and
   * holds a reference to a {@link LockGrantorId}.
   * <p>
   * Only outbound threads and operations should ever wait on this. Do NOT allow inbound threads to
   * use the <code>lockGrantorFutureResult</code>.
   */
  private FutureResult<Object> lockGrantorFutureResult;

  private final DLockStopper stopper;

  // -------------------------------------------------------------------------
  // State and concurrency construct methods
  // -------------------------------------------------------------------------

  public boolean isDestroyed() {
    synchronized (destroyLock) {
      if (destroyed) {
        return !isCurrentThreadDoingDestroy();
      }
      return false;
    }
  }

  public void checkDestroyed() {
    if (isDestroyed()) {
      throw generateLockServiceDestroyedException(generateLockServiceDestroyedMessage());
    }
  }

  /**
   * Create a new LockServiceDestroyedException for this lock service.
   *
   * @param message the detail message that explains the exception
   * @return new LockServiceDestroyedException
   */
  protected LockServiceDestroyedException generateLockServiceDestroyedException(String message) {
    return new LockServiceDestroyedException(message);
  }

  /**
   * Returns the string message to use in a LockServiceDestroyedException for this lock service.
   *
   * @return the detail message that explains LockServiceDestroyedException
   */
  protected String generateLockServiceDestroyedMessage() {
    return String.format("%s has been destroyed", this);
  }

  /**
   * Returns true if {@link #lockGrantorId} is the same as the specified LockGrantorId. Caller must
   * synchronize on {@link #lockGrantorIdLock}.
   *
   * @param someLockGrantorId the LockGrantorId to check
   */
  private boolean checkLockGrantorId(LockGrantorId someLockGrantorId) {
    Assert.assertHoldsLock(lockGrantorIdLock, true);
    if (lockGrantorId == null) {
      return false;
    }
    return lockGrantorId.sameAs(someLockGrantorId);
  }

  /**
   * Returns true if lockGrantorId is the same as the specified LockGrantorId. Caller must
   * synchronize on lockGrantorIdLock.
   *
   * @param someLockGrantorId the LockGrantorId to check
   */
  public boolean isLockGrantorId(LockGrantorId someLockGrantorId) {
    synchronized (lockGrantorIdLock) {
      return checkLockGrantorId(someLockGrantorId);
    }
  }

  private boolean isCurrentThreadDoingDestroy() {
    return Boolean.TRUE.equals(destroyingThread.get());
  }

  private void setDestroyingThread() {
    destroyingThread.set(Boolean.TRUE);
  }

  private void clearDestroyingThread() {
    destroyingThread.remove();
  }

  private InternalDistributedMember getElderId() {
    InternalDistributedMember elder = dm.getElderId();
    if (elder == null) {
      dm.getSystem().getCancelCriterion().checkCancelInProgress(null);
    }
    Assert.assertTrue(elder != null);
    return elder;
  }

  /**
   * Returns id of the current lock grantor for this service. If necessary, a request will be sent
   * to the elder to fetch this information.
   */
  public LockGrantorId getLockGrantorId() {
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
    boolean ownLockGrantorFutureResult;
    FutureResult<Object> lockGrantorFutureResultRef = null;

    long statStart = -1;
    LockGrantorId theLockGrantorId = null;
    while (theLockGrantorId == null) {

      ownLockGrantorFutureResult = false;
      try {
        Assert.assertHoldsLock(destroyLock, false);
        synchronized (lockGrantorIdLock) {
          if (lockGrantorFutureResult != null) {
            lockGrantorFutureResultRef = lockGrantorFutureResult;
          } else if (lockGrantorId != null) {
            return lockGrantorId;
          } else {
            ownLockGrantorFutureResult = true;
            lockGrantorFutureResultRef = new FutureResult<>(dm.getCancelCriterion());
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE,
                  "[getLockGrantorId] creating lockGrantorFutureResult");
            }
            lockGrantorFutureResult = lockGrantorFutureResultRef;
          }
        }

        statStart = getStats().startGrantorWait();
        if (!ownLockGrantorFutureResult) {
          LockGrantorId lockGrantorIdRef =
              waitForLockGrantorFutureResult(uncheckedCast(lockGrantorFutureResultRef), 0,
                  TimeUnit.MILLISECONDS);
          if (lockGrantorIdRef != null) {
            return lockGrantorIdRef;
          } else {
            continue;
          }
        }

        InternalDistributedMember elder = getElderId();
        Assert.assertTrue(elder != null);

        GrantorInfo gi = getGrantorRequest();
        theLockGrantorId =
            new LockGrantorId(dm, gi.getId(), gi.getVersionId(), gi.getSerialNumber());

        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE, "[getLockGrantorId] elder says grantor is {}",
              theLockGrantorId);
        }

        // elder tells us to be the grantor...
        if (theLockGrantorId.isLocal(getSerialNumber())) {
          boolean needsRecovery = gi.needsRecovery();
          if (!needsRecovery) {
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE, "[getLockGrantorId] needsRecovery is false");
            }
            synchronized (lockGrantorIdLock) {
              // either no previous grantor or grantor is newer
              Assert.assertTrue(
                  lockGrantorId == null || lockGrantorId.isNewerThan(theLockGrantorId)
                      || lockGrantorId.sameAs(theLockGrantorId),
                  lockGrantorId + " should be null or newer than or same as "
                      + theLockGrantorId);
            }
          }
          if (!createLocalGrantor(elder, needsRecovery, theLockGrantorId)) {
            theLockGrantorId = lockGrantorId;
          }
        }

        // elder says another member is the grantor
        else {
          synchronized (lockGrantorIdLock) {
            if (!setLockGrantorId(theLockGrantorId)) {
              theLockGrantorId = lockGrantorId;
            }
          }
        }
      } finally {
        synchronized (lockGrantorIdLock) {
          boolean getLockGrantorIdFailed = theLockGrantorId == null;
          if (statStart > -1) {
            getStats().endGrantorWait(statStart, getLockGrantorIdFailed);
          }
          if (ownLockGrantorFutureResult) {
            // this thread is doing the real work and must finish the future
            Assert.assertTrue(lockGrantorFutureResult == lockGrantorFutureResultRef);
            if (getLockGrantorIdFailed) {
              // failed so cancel lockGrantorFutureResult
              lockGrantorFutureResultRef.cancel(false);
            } else {
              // succeeded so set lockGrantorFutureResult
              lockGrantorFutureResultRef.set(theLockGrantorId);
            }
            // null out the reference so it is free for next usage
            lockGrantorFutureResult = null;
          }
        }
      } // finally block for lockGrantorFutureResult
    } // while theLockGrantorId == null
    return theLockGrantorId;
  }

  /**
   * Creates a local {@link DLockGrantor}.
   *
   * if (!createLocalGrantor(xxx)) { theLockGrantorId = this.lockGrantorId; }
   *
   * @param elder the elder that told us to be the grantor
   * @param needsRecovery true if recovery is required
   * @param myLockGrantorId lockGrantorId to use
   * @return true if successfully created local grantor; false if aborted
   */
  private boolean createLocalGrantor(InternalDistributedMember elder, boolean needsRecovery,
      LockGrantorId myLockGrantorId) {
    DLockGrantor myGrantor =
        DLockGrantor.createGrantor(this, myLockGrantorId.getLockGrantorVersion());
    if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
      logger.trace(LogMarker.DLS_VERBOSE, "[createLocalGrantor] Calling makeLocalGrantor");
    }
    return makeLocalGrantor(elder, needsRecovery, myLockGrantorId, myGrantor);
  }

  private boolean makeLocalGrantor(InternalDistributedMember elder, boolean needsRecovery,
      LockGrantorId myLockGrantorId, DLockGrantor myGrantor) {
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
    boolean success = false;
    try {
      synchronized (lockGrantorIdLock) {
        if (isDestroyed()) {
          checkDestroyed(); // exit
        }

        InternalDistributedMember currentElder = getElderId();
        if (!currentElder.equals(elder)) {
          // abort because elder changed
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "Failed to create {} because elder changed from {} to {}", myLockGrantorId, elder,
                currentElder);
          }
          return false; // exit
        }

        if (deposingLockGrantorId != null) {
          if (deposingLockGrantorId.isNewerThan(myLockGrantorId)) {
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE, "Failed to create {} because I was deposed by {}",
                  myLockGrantorId, deposingLockGrantorId);
            }
            deposingLockGrantorId = null;
            return false; // exit
          }

          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE, "{} failed to depose {}",
                deposingLockGrantorId, myLockGrantorId);
          }
          // older grantor couldn't depose us, so null it out...
          deposingLockGrantorId = null;
        }

        if (!setLockGrantorId(myLockGrantorId, myGrantor)) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "[getLockGrantorId] failed to create {} because current grantor is {}",
                myLockGrantorId, lockGrantorId);
          }
          return false; // exit
        }
      } // release sync on this.lockGrantorIdLock

      // do NOT sync while doing recovery (because it waits for replies)
      if (needsRecovery) {
        boolean recovered =
            DLockRecoverGrantorProcessor.recoverLockGrantor(dm.getDistributionManagerIds(),
                this, // this lock service
                myGrantor, dm, elder); // the elder that told us to be the grantor
        if (!recovered) {
          checkDestroyed();
          return false; // exit
        }
      }

      // after recovery, resynchronize on lockGrantorIdLock again
      // check to see if myLockGrantorId has been deposed
      synchronized (lockGrantorIdLock) {
        if (isDestroyed()) {
          checkDestroyed(); // exit
        }

        if (deposingLockGrantorId != null) {
          if (deposingLockGrantorId.isNewerThan(myLockGrantorId)) {
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE, "Failed to create {} because I was deposed by {}",
                  myLockGrantorId, deposingLockGrantorId);
            }
            deposingLockGrantorId = null;
            return false; // exit
          }

          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE, "{} failed to depose {}",
                deposingLockGrantorId, myLockGrantorId);
          }
          deposingLockGrantorId = null;
        }

        if (checkLockGrantorId(myLockGrantorId)) {
          success = myGrantor.makeReady(true); // do not enforce initializing
        }
      }

      return success; // exit
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE, "[makeLocalGrantor] throwing Error", e);
      }
      throw e;
    } catch (RuntimeException e) {
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE, "[makeLocalGrantor] throwing RuntimeException", e);
      }
      throw e;
    } finally {

      try {
        // abort if unsuccessful or if lock service was destroyed
        if (!success || isDestroyed()) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE, "[makeLocalGrantor] aborting {} and {}",
                myLockGrantorId, myGrantor);
          }
          nullLockGrantorId(myLockGrantorId);
          if (!myGrantor.isDestroyed()) {
            myGrantor.destroy();
          }
        }
      } finally {
        // assertion: grantor should now be either ready or destroyed!

        if (myGrantor.isInitializing() && !dm.getCancelCriterion().isCancelInProgress()) {
          logger.error(LogMarker.DLS_MARKER,
              "Grantor is still initializing");
        }
        if (!success && !myGrantor.isDestroyed() && !dm.getCancelCriterion().isCancelInProgress()) {
          logger.error(LogMarker.DLS_MARKER,
              "Grantor creation was aborted but grantor was not destroyed");
        }
      }
    }
  }

  /**
   * Set {@link #lockGrantorId} to the given new value if the current value is null or is an older
   * grantor version. Caller must hold {@link #lockGrantorIdLock}.
   *
   * @param newLockGrantorId the new value for lockGrantorId
   */
  private boolean setLockGrantorId(LockGrantorId newLockGrantorId) {
    Assert.assertHoldsLock(lockGrantorIdLock, true);
    if (equalsLockGrantorId(newLockGrantorId)) {
      return true;
    } else if (!newLockGrantorId.hasLockGrantorVersion()) {
      // proceed with temporary placeholder used by become grantor
      lockGrantorId = newLockGrantorId;
      return true;
    } else if (newLockGrantorId.isRemote() && lockGrantorId != null
        && lockGrantorId.hasLockGrantorVersion()) {
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "[setLockGrantorId] tried to replace {} with {}",
            lockGrantorId, newLockGrantorId);
      }
      return false;
    } else if (newLockGrantorId.isNewerThan(lockGrantorId)) {
      lockGrantorId = newLockGrantorId;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Set {@link #lockGrantorId} to the <code>localLockGrantorId</code> if current value is null or
   * is an older grantor version. This also atomically sets {@link #grantor} to ensure that the two
   * fields are kept in sync. Caller must hold {@link #lockGrantorIdLock}.
   *
   * @param localLockGrantorId the new value for lockGrantorId
   * @param localGrantor the new local intance of DLockGrantor
   */
  private boolean setLockGrantorId(LockGrantorId localLockGrantorId, DLockGrantor localGrantor) {
    Assert.assertHoldsLock(lockGrantorIdLock, true);
    Assert.assertTrue(localLockGrantorId.isLocal(getSerialNumber()));
    if (setLockGrantorId(localLockGrantorId)) {
      grantor = localGrantor;
      return true;
    }
    return false;
  }

  private LockGrantorId deposingLockGrantorId;

  /**
   * Deposes {@link #lockGrantorId} if <code>newLockGrantorId</code> is newer.
   *
   * @param newLockGrantorId the new lock grantor
   */
  void deposeOlderLockGrantorId(LockGrantorId newLockGrantorId) {
    LockGrantorId deposedLockGrantorId = null;
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
    synchronized (lockGrantorIdLock) {
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE, "[deposeOlderLockGrantorId] pre-deposing {} for new {}",
            deposedLockGrantorId, newLockGrantorId);
      }
      deposingLockGrantorId = newLockGrantorId;
      deposedLockGrantorId = lockGrantorId;
    }
    if (deposedLockGrantorId != null && deposedLockGrantorId.hasLockGrantorVersion()
        && newLockGrantorId.isNewerThan(deposedLockGrantorId)) {
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE,
            "[deposeOlderLockGrantorId] post-deposing {} for new {}", deposedLockGrantorId,
            newLockGrantorId);
      }
      nullLockGrantorId(deposedLockGrantorId);
    }
  }

  /**
   * Sets {@link #lockGrantorId} to null if the current value equals the expected old value. Caller
   * must hold {@link #lockGrantorIdLock}.
   *
   * @param oldLockGrantorId the expected old value
   * @return true if lockGrantorId was set to null
   */
  private boolean nullLockGrantorId(LockGrantorId oldLockGrantorId) {
    Assert.assertHoldsLock(destroyLock, false);
    Assert.assertHoldsLock(lockGrantorIdLock, false);
    if (oldLockGrantorId == null) {
      return false;
    }
    DLockGrantor grantorToDestroy = null;
    try {
      synchronized (lockGrantorIdLock) {
        if (equalsLockGrantorId(oldLockGrantorId)
            || (oldLockGrantorId.isLocal(getSerialNumber()) && isMakingLockGrantor())) {
          // this.lockGrantorId != null && this.lockGrantorId.isLocal())) {
          if (oldLockGrantorId.isLocal(getSerialNumber())
              && isLockGrantorVersion(grantor, oldLockGrantorId.getLockGrantorVersion())) {
            // need to destroy and remove grantor
            grantorToDestroy = grantor;
            grantor = null;
          }
          lockGrantorId = null;
          return true;
        } else {
          return false;
        }
      }
    } finally {
      if (grantorToDestroy != null) {
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE, "[nullLockGrantorId] destroying {}",
              grantorToDestroy);
        }
        grantorToDestroy.destroy();
      }
    }
  }

  /**
   * Returns true if the grantor version of <code>dlockGrantor</code> equals the
   * <code>grantorVersion</code>.
   *
   * @param dlockGrantor the grantor instance to compare to grantorVersion
   * @param grantorVersion the grantor version number
   * @return true if dlockGrantor is the same grantor version
   */
  private boolean isLockGrantorVersion(DLockGrantor dlockGrantor, long grantorVersion) {
    if (dlockGrantor == null) {
      return false;
    }
    return dlockGrantor.getVersionId() == grantorVersion;
  }

  /**
   * Returns true if <code>someLockGrantor</code> equals the current {@link #lockGrantorId}.
   *
   * @return true if someLockGrantor equals the current lockGrantorId
   */
  private boolean equalsLockGrantorId(LockGrantorId someLockGrantor) {
    Assert.assertHoldsLock(lockGrantorIdLock, true);
    if (someLockGrantor == null) {
      return lockGrantorId == null;
    }
    return someLockGrantor.equals(lockGrantorId);
  }

  /**
   * Returns id of the current lock grantor for this service. If necessary, a request will be sent
   * to the elder to fetch this information. Unlike getLockGrantorId this call will not become the
   * lock grantor.
   */
  public LockGrantorId peekLockGrantorId() {
    Assert.assertHoldsLock(destroyLock, false);
    synchronized (lockGrantorIdLock) {
      LockGrantorId currentLockGrantorId = lockGrantorId;
      if (currentLockGrantorId != null) {
        return currentLockGrantorId;
      }
    }

    long statStart = getStats().startGrantorWait();
    LockGrantorId theLockGrantorId = null;
    try {
      // 1st thread wins the right to request grantor info from elder
      GrantorInfo gi = peekGrantor();
      InternalDistributedMember lockGrantorMember = gi.getId();
      if (lockGrantorMember == null) {
        return null;
      }
      theLockGrantorId =
          new LockGrantorId(dm, lockGrantorMember, gi.getVersionId(), gi.getSerialNumber());
      return theLockGrantorId;
    } finally {
      boolean getLockGrantorIdFailed = theLockGrantorId == null;
      getStats().endGrantorWait(statStart, getLockGrantorIdFailed);
    }
  }

  /**
   * Increments {@link #activeLocks} while synchronized on {@link #destroyLock} after calling
   * {@link #checkDestroyed()}.
   */
  private void incActiveLocks() {
    synchronized (destroyLock) {
      checkDestroyed();
      activeLocks++;
    }
  }

  /**
   * Decrements {@link #activeLocks} while synchronized on {@link #destroyLock}.
   */
  private void decActiveLocks() {
    synchronized (destroyLock) {
      activeLocks--;
    }
  }

  /**
   * Returns lockGrantorId when lockGrantorFutureResultRef has been set by another thread.
   *
   * @param lockGrantorFutureResultRef FutureResult to wait for
   * @param timeToWait how many ms to wait, 0 = forever
   * @param timeUnit the unit of measure for timeToWait
   * @return the LockGrantorId or null if FutureResult was cancelled
   */
  private LockGrantorId waitForLockGrantorFutureResult(
      FutureResult<LockGrantorId> lockGrantorFutureResultRef,
      long timeToWait, final TimeUnit timeUnit) {
    LockGrantorId lockGrantorIdRef = null;
    while (lockGrantorIdRef == null) {
      boolean interrupted = Thread.interrupted();
      try {
        checkDestroyed();
        if (timeToWait == 0) {
          lockGrantorIdRef = lockGrantorFutureResultRef.get();
        } else {
          lockGrantorIdRef = lockGrantorFutureResultRef.get(timeToWait, timeUnit);
        }
      } catch (TimeoutException e) {
        break;
      } catch (InterruptedException e) {
        interrupted = true;
        dm.getCancelCriterion().checkCancelInProgress(e);
        if (lockGrantorFutureResultRef.isCancelled()) {
          // cancelled Future might throw InterruptedException...?
          checkDestroyed();
          break; // return null
        }
      } catch (CancellationException e) { // Future was cancelled
        checkDestroyed();
        break; // return null
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    return lockGrantorIdRef;
  }

  /**
   * nulls out grantor to force call to elder
   *
   * @param timeToWait how long to wait for a new grantor. -1 don't wait, 0 no time limit
   * @param timeUnit the unit of measure of timeToWait
   */
  private void notLockGrantorId(LockGrantorId notLockGrantorId, long timeToWait,
      final TimeUnit timeUnit) {
    if (notLockGrantorId.isLocal(getSerialNumber())) {
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE,
            "notLockGrantorId {} returning early because notGrantor {} was equal to the local dm {}",
            serviceName, notLockGrantorId, dm.getId());
      }
      // Let the local destroy or processing of transfer do the clear
      return;
    }

    boolean ownLockGrantorFutureResult = false;
    FutureResult<Object> lockGrantorFutureResultRef = null;

    long statStart = -1;
    final LockGrantorId currentLockGrantorId;

    try {
      Assert.assertHoldsLock(destroyLock, false);
      synchronized (lockGrantorIdLock) {
        currentLockGrantorId = lockGrantorId;
        if (lockGrantorFutureResult != null) {
          // some other thread is talking to elder
          lockGrantorFutureResultRef = lockGrantorFutureResult;
        } else if (!notLockGrantorId.sameAs(currentLockGrantorId)) {
          return;
        } else {
          // this thread needs to talk to elder
          ownLockGrantorFutureResult = true;
          lockGrantorFutureResultRef = new FutureResult<>(dm.getCancelCriterion());
          lockGrantorFutureResult = lockGrantorFutureResultRef;
        }
      }

      statStart = getStats().startGrantorWait();
      if (!ownLockGrantorFutureResult) {
        if (timeToWait >= 0) {
          waitForLockGrantorFutureResult(uncheckedCast(lockGrantorFutureResultRef), timeToWait,
              timeUnit);
        }
        return;
      }

      InternalDistributedMember elder = getElderId();
      Assert.assertTrue(elder != null);

      LockGrantorId elderLockGrantorId = null;
      GrantorInfo gi = peekGrantor();
      if (gi.getId() != null) {
        elderLockGrantorId =
            new LockGrantorId(dm, gi.getId(), gi.getVersionId(), gi.getSerialNumber());
      }

      if (notLockGrantorId.sameAs(elderLockGrantorId)) {
        // elder says that notLockGrantorId is still the grantor...
        sleep(NOT_GRANTOR_SLEEP);
      } else {
        // elder says another member is the grantor
        nullLockGrantorId(notLockGrantorId);
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "notLockGrantorId cleared lockGrantorId for service {}", serviceName);
        }
      }
    } finally {
      synchronized (lockGrantorIdLock) {
        if (statStart > -1) {
          getStats().endGrantorWait(statStart, false);
        }
        if (ownLockGrantorFutureResult) {
          // this thread is doing the real work and must finish the future
          Assert.assertTrue(lockGrantorFutureResult == lockGrantorFutureResultRef);
          // cancel lockGrantorFutureResult
          lockGrantorFutureResultRef.cancel(false);
          // null out the reference so it is free for next usage
          lockGrantorFutureResult = null;
        }
      }
    } // finally block for lockGrantorFutureResult
  }

  /**
   * All calls to GrantorRequestProcessor.clearGrantor must come through this synchronization point.
   * <p>
   * This fixes a deadlock between this.becomeGrantorMonitor and DistributionManager.elderLock
   * <p>
   * All calls to the elder may result in elder recovery which may call back into dlock and acquire
   * synchronization on this.becomeGrantorMonitor.
   */
  void clearGrantor(long grantorVersion, boolean withLocks) {
    GrantorRequestProcessor.clearGrantor(grantorVersion, this, getSerialNumber(), ds,
        withLocks);
  }

  /**
   * All calls to GrantorRequestProcessor.getGrantor must come through this synchronization point.
   * <p>
   * This fixes a deadlock between this.becomeGrantorMonitor and DistributionManager.elderLock
   * <p>
   * All calls to the elder may result in elder recovery which may call back into dlock and acquire
   * synchronization on this.becomeGrantorMonitor.
   */
  private GrantorInfo getGrantorRequest() {
    return GrantorRequestProcessor.getGrantor(this, getSerialNumber(), ds);
  }

  /**
   * All calls to GrantorRequestProcessor.peekGrantor must come through this synchronization point.
   * <p>
   * This fixes a deadlock between this.becomeGrantorMonitor and DistributionManager.elderLock
   * <p>
   * All calls to the elder may result in elder recovery which may call back into dlock and acquire
   * synchronization on this.becomeGrantorMonitor.
   */
  private GrantorInfo peekGrantor() {
    return GrantorRequestProcessor.peekGrantor(this, ds);
  }

  /**
   * All calls to GrantorRequestProcessor.becomeGrantor must come through this synchronization
   * point.
   * <p>
   * This fixes a deadlock between this.becomeGrantorMonitor and DistributionManager.elderLock
   * <p>
   * All calls to the elder may result in elder recovery which may call back into dlock and acquire
   * synchronization on this.becomeGrantorMonitor.
   */
  private GrantorInfo becomeGrantor(InternalDistributedMember predecessor) {
    return GrantorRequestProcessor.becomeGrantor(this, getSerialNumber(), predecessor, ds);
  }

  // -------------------------------------------------------------------------
  // New external API methods
  // -------------------------------------------------------------------------

  @Override
  public void becomeLockGrantor() {
    becomeLockGrantor((InternalDistributedMember) null);
  }

  public DLockGrantor getGrantor() {
    Assert.assertHoldsLock(destroyLock, false);
    synchronized (lockGrantorIdLock) {
      return grantor;
    }
  }

  public DLockGrantor getGrantorWithNoSync() {
    return grantor;
  }

  /**
   * @param predecessor non-null if a predecessor asked us to take over for it
   */
  private void becomeLockGrantor(InternalDistributedMember predecessor) {
    Assert.assertTrue(predecessor == null);
    boolean ownLockGrantorFutureResult = false;
    FutureResult<Object> lockGrantorFutureResultRef = null;

    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
    LockGrantorId myLockGrantorId = null;
    try { // finally handles lockGrantorFutureResult

      // loop while other threads control the lockGrantorFutureResult
      // terminate loop if other thread has already made us lock grantor
      // terminate loop if this thread gets control of lockGrantorFutureResult
      while (!ownLockGrantorFutureResult) {
        Assert.assertHoldsLock(destroyLock, false);
        synchronized (lockGrantorIdLock) {
          if (isCurrentlyOrIsMakingLockGrantor()) {
            return;
          } else if (lockGrantorFutureResult != null) {
            // need to wait for other thread controlling lockGrantorFutureResult
            lockGrantorFutureResultRef = lockGrantorFutureResult;
          } else {
            // this thread is in control and will proceed to become grantor
            // create new lockGrantorFutureResult for other threads to block on
            ownLockGrantorFutureResult = true;
            lockGrantorFutureResultRef = new FutureResult<>(dm.getCancelCriterion());
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE,
                  "[becomeLockGrantor] creating lockGrantorFutureResult");
            }
            lockGrantorFutureResult = lockGrantorFutureResultRef;
          }
        }
        if (!ownLockGrantorFutureResult) {
          waitForLockGrantorFutureResult(uncheckedCast(lockGrantorFutureResultRef), 0,
              TimeUnit.MILLISECONDS);
        }
      }

      // this thread is now in charge of the lockGrantorFutureResult future
      getStats().incBecomeGrantorRequests();

      // create the new grantor instance in non-ready state...
      long tempGrantorVersion = -1;
      LockGrantorId tempLockGrantorId =
          new LockGrantorId(dm, dm.getId(), tempGrantorVersion, getSerialNumber());

      DLockGrantor myGrantor = DLockGrantor.createGrantor(this, tempGrantorVersion);

      try { // finally handles myGrantor

        synchronized (lockGrantorIdLock) {
          Assert.assertTrue(setLockGrantorId(tempLockGrantorId, myGrantor));
        }

        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE, "become set lockGrantorId to {} for service {}",
              lockGrantorId, serviceName);
        }

        InternalDistributedMember elder = getElderId();
        Assert.assertTrue(elder != null);

        // NOTE: elder currently returns GrantorInfo for the previous grantor
        // CONSIDER: add elderCommunicatedWith to GrantorInfo
        GrantorInfo gi = becomeGrantor(predecessor);
        boolean needsRecovery = gi.needsRecovery();
        long myGrantorVersion = gi.getVersionId() + 1;
        myGrantor.setVersionId(myGrantorVersion);

        myLockGrantorId =
            new LockGrantorId(dm, dm.getId(), myGrantorVersion, getSerialNumber());

        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE, "[becomeLockGrantor] Calling makeLocalGrantor");
        }
        if (!makeLocalGrantor(elder, needsRecovery, myLockGrantorId, myGrantor)) {
          return;
        }

      } finally {
        Assert.assertTrue(
            !myGrantor.isInitializing() || dm.getCancelCriterion().isCancelInProgress()
                || isDestroyed(),
            "BecomeLockGrantor failed and left grantor non-ready");
      }
    } finally {
      synchronized (lockGrantorIdLock) {
        if (ownLockGrantorFutureResult) {
          // this thread is doing the real work and must finish the future
          Assert.assertTrue(lockGrantorFutureResult == lockGrantorFutureResultRef);
          boolean getLockGrantorIdFailed = myLockGrantorId == null;
          if (getLockGrantorIdFailed) {
            // failed so cancel lockGrantorFutureResult
            lockGrantorFutureResultRef.cancel(true); // interrupt waiting threads
          } else {
            dm.getCancelCriterion().checkCancelInProgress(null); // don't succeed if shutting
                                                                 // down
            // succeeded so set lockGrantorFutureResult
            lockGrantorFutureResultRef.set(myLockGrantorId);
          }
          // null out the reference so it is free for next usage
          lockGrantorFutureResult = null;
        }
      }
    }
  }

  @Override
  public boolean isLockGrantor() {
    if (isDestroyed()) {
      return false;
    } else {
      return isCurrentlyLockGrantor();
    }
  }

  boolean isMakingLockGrantor() {
    Assert.assertHoldsLock(destroyLock, false);
    synchronized (lockGrantorIdLock) {
      return lockGrantorId != null && lockGrantorId.isLocal(getSerialNumber())
          && grantor != null && grantor.isInitializing();
    }
  }

  boolean isCurrentlyOrIsMakingLockGrantor() {
    Assert.assertHoldsLock(destroyLock, false);
    synchronized (lockGrantorIdLock) {
      return lockGrantorId != null && lockGrantorId.isLocal(getSerialNumber());
    }
  }

  boolean isCurrentlyLockGrantor() {
    Assert.assertHoldsLock(destroyLock, false);
    synchronized (lockGrantorIdLock) {
      return lockGrantorId != null && lockGrantorId.isLocal(getSerialNumber())
          && grantor != null && grantor.isReady();
    }
  }

  // -------------------------------------------------------------------------
  // External API methods
  // -------------------------------------------------------------------------

  @Override
  public void freeResources(Object name) {
    checkDestroyed();
    if (name == null) {
      removeAllUnusedTokens();
    } else {
      removeTokenIfUnused(name);
    }
  }

  /**
   * Attempt to destroy and remove lock token. Synchronizes on tokens map and the lock token.
   *
   * @param name the name of the lock token
   * @return true if token has been destroyed and removed
   */
  private boolean removeTokenIfUnused(Object name) {
    synchronized (tokens) {
      if (destroyed) {
        getStats().incFreeResourcesFailed();
        return false;
      }
      DLockToken token = tokens.get(name);
      if (token != null) {
        synchronized (token) {
          if (!token.isBeingUsed()) {
            if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
              logger.trace(LogMarker.DLS_VERBOSE, "Freeing {} in {}", token, this);
            }
            removeTokenFromMap(name);
            token.destroy();
            getStats().incTokens(-1);
            getStats().incFreeResourcesCompleted();
            return true;
          }
        }
      }
    }
    getStats().incFreeResourcesFailed();
    return false;
  }

  protected Object removeTokenFromMap(Object name) {
    return tokens.remove(name);
  }

  /**
   * Attempt to destroy and remove all unused lock tokens. Synchronizes on tokens map and each lock
   * token.
   */
  private void removeAllUnusedTokens() {
    synchronized (tokens) {
      if (destroyed) {
        getStats().incFreeResourcesFailed();
        return;
      }
      Set<DLockToken> unusedTokens = emptySet();
      for (DLockToken token : tokens.values()) {
        synchronized (token) {
          if (!token.isBeingUsed()) {
            if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
              logger.trace(LogMarker.DLS_VERBOSE, "Freeing {} in {}", token, this);
            }
            if (unusedTokens == Collections.EMPTY_SET) {
              unusedTokens = new HashSet<>();
            }
            unusedTokens.add(token);
          } else {
            getStats().incFreeResourcesFailed();
          }
        }
      }
      for (DLockToken token : unusedTokens) {
        synchronized (token) {
          int tokensSizeBefore = tokens.size();
          Object obj = removeTokenFromMap(token.getName());
          Assert.assertTrue(obj != null);
          int tokensSizeAfter = tokens.size();
          Assert.assertTrue(tokensSizeBefore - tokensSizeAfter == 1);
          token.destroy();
          getStats().incTokens(-1);
          getStats().incFreeResourcesCompleted();
        }
      }
    }
  }

  /**
   * Destroys and removes all lock tokens. Caller must synchronize on destroyLock. Synchronizes on
   * tokens map and each token.
   */
  private void removeAllTokens() {
    synchronized (tokens) {
      Assert.assertTrue(destroyed);
      for (DLockToken token : tokens.values()) {
        synchronized (token) {
          token.destroy();
        }
      }
      getStats().incTokens(-tokens.size());
      tokens.clear();
    }
  }

  @Override
  public boolean isHeldByCurrentThread(Object name) {
    checkDestroyed();
    synchronized (tokens) {
      DLockToken token = basicGetToken(name);
      if (token == null) {
        return false;
      }
      synchronized (token) {
        token.checkForExpiration();
        return token.isLeaseHeldByCurrentThread();
      }
    }
  }

  public boolean isHeldByThreadId(Object name, int threadId) {
    checkDestroyed();
    synchronized (tokens) {
      DLockToken token = basicGetToken(name);
      if (token == null) {
        return false;
      }
      synchronized (token) {
        token.checkForExpiration();
        if (token.getLesseeThread() == null) {
          return false;
        }
        return token.getLesseeThread().getThreadId() == threadId;
      }
    }
  }

  @Override
  public boolean isLockingSuspendedByCurrentThread() {
    checkDestroyed();
    return isHeldByCurrentThread(SUSPEND_LOCKING_TOKEN);
  }

  @Override
  public boolean lock(Object name, long waitTimeMillis, long leaseTimeMillis) {
    boolean tryLock = false;
    return lock(name, waitTimeMillis, leaseTimeMillis, tryLock);
  }

  public boolean lock(Object name, long waitTimeMillis, long leaseTimeMillis, boolean tryLock) {
    return lock(name, waitTimeMillis, leaseTimeMillis, tryLock, false);
  }

  public boolean lock(Object name, long waitTimeMillis, long leaseTimeMillis, boolean tryLock,
      boolean disallowReentrant) {
    return lock(name, waitTimeMillis, leaseTimeMillis, tryLock, disallowReentrant, false);
  }

  public boolean lock(Object name, long waitTimeMillis, long leaseTimeMillis, boolean tryLock,
      boolean disallowReentrant, boolean disableAlerts) {
    checkDestroyed();
    try {
      boolean interruptible = false;
      return lockInterruptibly(name, waitTimeMillis, leaseTimeMillis, tryLock, interruptible,
          disallowReentrant, disableAlerts);
    } catch (InterruptedException ex) { // LOST INTERRUPT
      Thread.currentThread().interrupt();
      // fail assertion
      logger.error(LogMarker.DLS_MARKER, "lock() was interrupted", ex);
      Assert.assertTrue(false, "lock() was interrupted: " + ex.getMessage());
    }
    return false;
  }

  @Override
  public boolean lockInterruptibly(Object name, long waitTimeMillis, long leaseTimeMillis)
      throws InterruptedException {
    checkDestroyed();
    boolean tryLock = false;
    boolean interruptible = true;
    return lockInterruptibly(name, waitTimeMillis, leaseTimeMillis, tryLock, interruptible, false);
  }

  /** Causes the current thread to sleep for millis and may or may not be interruptible */
  private void sleep(long millis, boolean interruptible) throws InterruptedException {
    if (interruptible) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      Thread.sleep(millis);
    } else {
      sleep(millis);
    }
  }

  /** Causes the current thread to sleep for millis uninterruptibly */
  private void sleep(long millis) {
    // Non-interruptible case
    StopWatch timer = new StopWatch(true);
    while (true) {
      boolean interrupted = Thread.interrupted();
      try {
        long timeLeft = millis - timer.elapsedTimeMillis();
        if (timeLeft <= 0) {
          break;
        }
        Thread.sleep(timeLeft);
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  protected DLockRequestProcessor createRequestProcessor(LockGrantorId grantorId, Object name,
      int threadId, long startTime, long requestLeaseTime, long requestWaitTime, boolean reentrant,
      boolean tryLock) {
    return createRequestProcessor(grantorId, name, threadId, startTime, requestLeaseTime,
        requestWaitTime, reentrant, tryLock, false);
  }

  protected DLockRequestProcessor createRequestProcessor(LockGrantorId grantorId, Object name,
      int threadId, long startTime, long requestLeaseTime, long requestWaitTime, boolean reentrant,
      boolean tryLock, boolean disableAlerts) {
    return new DLockRequestProcessor(grantorId, this, name, threadId, startTime, requestLeaseTime,
        requestWaitTime, reentrant, tryLock, disableAlerts, dm);
  }

  protected boolean callReleaseProcessor(InternalDistributedMember grantor, Object name,
      boolean lockBatch, int lockId) {
    return DLockService.callReleaseProcessor(dm, serviceName, grantor, name, lockBatch,
        lockId);
  }

  protected static boolean callReleaseProcessor(DistributionManager dm, String serviceName,
      InternalDistributedMember grantor, Object name, boolean lockBatch, int lockId) {
    DLockReleaseProcessor processor = new DLockReleaseProcessor(dm, grantor, serviceName, name);
    return processor.release(grantor, serviceName, lockBatch, lockId);
  }

  public boolean lockInterruptibly(final Object name, final long waitTimeMillis,
      final long leaseTimeMillis, final boolean tryLock, final boolean interruptible,
      final boolean disallowReentrant) throws InterruptedException {
    return lockInterruptibly(name, waitTimeMillis, leaseTimeMillis, tryLock, interruptible,
        disallowReentrant, false);
  }


  /**
   * @param name the name of the lock to acquire in this service. This object must conform to the
   *        general contract of <code>equals(Object)</code> and <code>hashCode()</code> as described
   *        in {@link java.lang.Object#hashCode()}.
   *
   * @param waitTimeMillis the number of milliseconds to try to acquire the lock before giving up
   *        and returning false. A value of -1 causes this method to block until the lock is
   *        acquired.
   *
   * @param leaseTimeMillis the number of milliseconds to hold the lock after granting it, before
   *        automatically releasing it if it hasn't already been released by invoking
   *        {@link #unlock(Object)}. If <code>leaseTimeMillis</code> is -1, hold the lock until
   *        explicitly unlocked.
   *
   * @param tryLock true if the lock should be acquired or fail if currently held. waitTimeMillis
   *        will be ignored if the lock is currently held by another client.
   *
   * @param interruptible true if this lock request is interruptible
   *
   * @param disableAlerts true to disable logging alerts if the dlock is taking a long time to
   *        acquired.
   *
   * @return true if the lock was acquired, false if the timeout <code>waitTimeMillis</code> passed
   *         without acquiring the lock.
   *
   * @throws InterruptedException if the thread is interrupted before or during this method.
   *
   * @throws UnsupportedOperationException if attempt to lock batch involves non-tryLocks
   */
  public boolean lockInterruptibly(final Object name, final long waitTimeMillis,
      final long leaseTimeMillis, final boolean tryLock, final boolean interruptible,
      final boolean disallowReentrant, final boolean disableAlerts) throws InterruptedException {
    checkDestroyed();

    boolean interrupted = Thread.interrupted();
    if (interrupted && interruptible) {
      throw new InterruptedException();
    }

    try {
      long statStart = getStats().startLockWait();
      long startTime = getLockTimeStamp(dm);

      long requestWaitTime = waitTimeMillis;
      long requestLeaseTime = leaseTimeMillis;

      // -1 means "lease forever". Long.MAX_VALUE is pretty close.
      if (requestLeaseTime == -1) {
        requestLeaseTime = Long.MAX_VALUE;
      }
      // -1 means "wait forever". Long.MAX_VALUE is pretty close.
      if (requestWaitTime == -1) {
        requestWaitTime = Long.MAX_VALUE;
      }

      long waitLimit = startTime + requestWaitTime;
      if (waitLimit < 0) {
        waitLimit = Long.MAX_VALUE;
      }

      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "{}, name: {} - entering lock()", this, name);
      }

      DLockToken token = getOrCreateToken(name);
      boolean gotLock = false;
      blockedOn.set(name);
      try { // try-block for end stats, token cleanup, and interrupt check

        ThreadRequestState requestState = threadRequestState.get();
        if (requestState == null) {
          requestState = new ThreadRequestState(incThreadSequence(), interruptible);
          threadRequestState.set(requestState);
        } else {
          requestState.interruptible = interruptible;
        }
        final int threadId = requestState.threadId;

        // if reentry and no change to expiration then grantor is not bothered

        boolean keepTrying = true;
        int lockId = -1;
        incActiveLocks();

        while (keepTrying) {
          checkDestroyed();
          interrupted = Thread.interrupted() || interrupted; // clear
          if (interrupted && interruptible) {
            throw new InterruptedException();
          }

          // Check for recursive lock
          boolean reentrant = false;
          int recursionBefore = -1;

          synchronized (token) {
            token.checkForExpiration();
            if (token.isLeaseHeldByCurrentThread()) {
              if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
                logger.trace(LogMarker.DLS_VERBOSE, "{} , name: {} - lock() is reentrant: {}", this,
                    name, token);
              }
              reentrant = true;
              if (disallowReentrant) {
                throw new IllegalStateException(
                    String.format("%s attempted to reenter non-reentrant lock %s",
                        Thread.currentThread(), token));
              }
              recursionBefore = token.getRecursion();
              lockId = token.getLeaseId(); // keep lockId
              if (lockId < 0) {
                // loop back around due to expiration
                continue;
              }
            } // isLeaseHeldByCurrentThread
          } // token sync

          LockGrantorId theLockGrantorId = getLockGrantorId();

          if (reentrant) {
            Assert.assertTrue(lockId > -1, "Reentrant lock must have lockId > -1");
            // lockId = token.getLockId(); // keep lockId
          } else {
            // this thread is not current owner...
            lockId = -1; // reset lockId back to -1
          }

          DLockRequestProcessor processor = createRequestProcessor(theLockGrantorId, name, threadId,
              startTime, requestLeaseTime, requestWaitTime, reentrant, tryLock, disableAlerts);
          if (reentrant) {
            // check for race condition... reentrant expired already...
            // related to bug 32765, but client-side... see bug 33402
            synchronized (token) {
              if (!token.isLeaseHeldByCurrentThread()) {
                reentrant = false;
                recursionBefore = -1;
                token.checkForExpiration();
              }
            }
          } else {
            // set lockId since this is the first granting (non-reentrant)
            lockId = processor.getProcessorId();
          }

          gotLock = processor.requestLock(interruptible, lockId); // can throw
                                                                  // InterruptedException

          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "Grantor {} replied {}", theLockGrantorId,
                processor.getResponseCodeString());
          }

          if (gotLock) {
            final long leaseExpireTime = processor.getLeaseExpireTime();
            int recursion = recursionBefore + 1;

            if (!grantLocalDLockAfterObtainingRemoteLock(name, token, threadId, leaseExpireTime,
                lockId, theLockGrantorId, processor, recursion)) {
              continue;
            }

            if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
              logger.trace(LogMarker.DLS_VERBOSE, "{}, name: {} - granted lock: {}", this, name,
                  token);
            }
            keepTrying = false;
          } else if (processor.repliedDestroyed()) {
            checkDestroyed(); // throws LockServiceDestroyedException
            Assert.assertTrue(isDestroyed(),
                "Grantor reports service " + this + " is destroyed: " + name);
          } else if (processor.repliedNotGrantor() || processor.hadNoResponse()) {
            long waitForGrantorTime = waitLimit - token.getCurrentTime();
            if (waitForGrantorTime <= 0) {
              waitForGrantorTime = 100;
            }
            notLockGrantorId(theLockGrantorId, waitForGrantorTime, TimeUnit.MILLISECONDS);
            // keepTrying is still true... loop back around
          } else if (processor.repliedNotHolder()) {
            // fix part of bug 32765 - reentrant/expiration problem
            // probably expired... try to get non-reentrant lock
            reentrant = false;
            recursionBefore = -1;
            synchronized (token) {
              token.checkForExpiration();
              if (token.isLeaseHeldByCurrentThread()) {
                // THIS SHOULDN'T HAPPEN -- some sort of weird consistency
                // problem. Do what the grantor says and release the lock...
                logger.warn(LogMarker.DLS_MARKER, "Grantor reports reentrant lock not held: {}",
                    token);

                // Attempt at fault tolerance: We thought we owned it, but we
                // don't; let's release it. Removes hot loop in bug 37276,
                // but does not address underlying consistency failure.
                RemoteThread rThread = new RemoteThread(getDistributionManager().getId(), threadId);
                token.releaseLock(lockId, rThread, false);
              }
            } // token sync
          } // grantor replied NOT_HOLDER for reentrant lock

          else {
            // either dlock service is suspended or tryLock failed
            // fixed the math here... bug 32765
            if (waitLimit > token.getCurrentTime() + 20) {
              sleep(20, interruptible);
            }
            keepTrying = waitLimit > token.getCurrentTime();
          }

        } // while (keepTrying)
          // try-block for end stats, token cleanup, and interrupt check
      } finally {
        getStats().endLockWait(statStart, gotLock);

        // cleanup token if failed to get lock
        if (!gotLock) {
          synchronized (token) {
            token.decUsage();
          }
          freeResources(token.getName());
        }

        // reset the interrupt state
        if (interrupted) {
          Thread.currentThread().interrupt();
        }

        // throw InterruptedException only if failed to get lock and interrupted
        if (!gotLock && interruptible && Thread.interrupted()) {
          throw new InterruptedException();
        }
        blockedOn.set(null);
      }

      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "{}, name: {} - exiting lock() returning {}", this,
            name, gotLock);
      }
      return gotLock;
    } finally {
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "{}, name: {} - exiting lock() without returning value",
            this, name);
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private boolean grantLocalDLockAfterObtainingRemoteLock(Object name, DLockToken token,
      int threadId, long leaseExpireTime, int lockId, LockGrantorId theLockGrantorId,
      DLockRequestProcessor processor, int recursion) {
    boolean needToReleaseOrphanedGrant = false;

    Assert.assertHoldsLock(destroyLock, false);
    synchronized (lockGrantorIdLock) {
      if (!checkLockGrantorId(theLockGrantorId)) {
        // race: grantor changed
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "Cannot honor grant from {} because {} is now a grantor.", theLockGrantorId,
              lockGrantorId);
        }
      } else if (isDestroyed()) {
        // race: dls was destroyed
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "Cannot honor grant from {} because this lock service has been destroyed.",
              theLockGrantorId);
        }
        needToReleaseOrphanedGrant = true;
      } else {
        synchronized (tokens) {
          checkDestroyed();
          Assert.assertTrue(token == basicGetToken(name));
          RemoteThread rThread = new RemoteThread(getDistributionManager().getId(), threadId);
          token.grantLock(leaseExpireTime, lockId, recursion, rThread);
          return true;
        } // tokens sync
      }
    }

    if (needToReleaseOrphanedGrant) {
      processor.getResponse().releaseOrphanedGrant(dm);
    }
    return false;
  }

  /**
   * Allow locking to resume.
   */
  @Override
  public void resumeLocking() {
    checkDestroyed();
    try {
      // need to resumeLocking before unlocking to avoid deadlock with
      // other thread attempting to suspendLocking
      unlock(SUSPEND_LOCKING_TOKEN);
    } catch (IllegalStateException e) {
      checkDestroyed();
      throw e;
    }
  }

  /**
   * Suspends granting of locks for this instance of DLockService. If distribute is true, sends
   * suspendLocking to all other members that have created this service. Blocks until all
   * outstanding locks have been released (excluding those held by the initial calling thread).
   *
   * @param waitTimeMillis -1 means "wait forever", >=0 = milliseconds to wait
   *
   * @return true if locking is suspended and all locks have been released. Otherwise, resumeLocking
   *         is invoked and false is returned.
   */
  @Override
  public boolean suspendLocking(final long waitTimeMillis) {

    long startTime = System.currentTimeMillis();
    long requestWaitTime = waitTimeMillis;
    boolean interrupted = false;

    try {
      do {
        checkDestroyed();
        try {
          return suspendLockingInterruptibly(requestWaitTime, false);
        } catch (InterruptedException ex) {
          interrupted = true;
          long millisPassed = System.currentTimeMillis() - startTime;
          if (requestWaitTime >= 0) {
            requestWaitTime = Math.max(0, requestWaitTime - millisPassed);
          }
        }
      } while (requestWaitTime != 0);

    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    return false;
  }

  @Override
  public boolean suspendLockingInterruptibly(long waitTimeMillis) throws InterruptedException {
    return suspendLockingInterruptibly(waitTimeMillis, true);
  }

  public boolean suspendLockingInterruptibly(long waitTimeMillis, boolean interruptible)
      throws InterruptedException {
    checkDestroyed();

    boolean wasInterrupted = false;
    if (Thread.interrupted()) {
      if (interruptible) {
        throw new InterruptedException();
      } else {
        wasInterrupted = true;
      }
    }

    try {

      if (isLockingSuspendedByCurrentThread()) {
        throw new IllegalStateException(
            "Current thread has already locked entire service");
      }

      // have to use tryLock to avoid deadlock with other members that are
      // simultaneously attempting to suspend locking
      boolean tryLock = false; // go with false to queue up suspend lock requests
      // when tryLock is false, we get deadlock:
      // thread 1 is this thread
      // thread 2 is processing a SuspendMessage... goes thru
      // suspendLocking with distribute=false and gets stuck in
      // waitForGrantorCallsInProgress

      boolean gotToken = false;
      boolean keepTrying = true;

      long startTime = System.currentTimeMillis();
      long waitLimit = startTime + waitTimeMillis;
      if (waitLimit < 0) {
        waitLimit = Long.MAX_VALUE;
      }

      while (!gotToken && keepTrying) {
        gotToken =
            lockInterruptibly(SUSPEND_LOCKING_TOKEN, waitTimeMillis, -1, tryLock, interruptible,
                false);
        keepTrying = !gotToken && waitLimit > System.currentTimeMillis();
      }
      return gotToken;
    } finally {
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void unlock(Object name) throws LockNotHeldException, LeaseExpiredException {
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);

    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS_VERBOSE, "{}, name: {} - entering unlock()", this, name);
    }

    long statStart = getStats().startLockRelease();

    boolean hadRecursion = false;
    boolean unlocked = false;
    int lockId = -1;
    DLockToken token = null;
    RemoteThread rThread = null;

    try {
      synchronized (tokens) {
        checkDestroyed();
        token = basicGetToken(name);
        if (token == null) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE, "{}, [unlock] no token found for: {}", this, name);
          }
          throw new LockNotHeldException(
              String.format(
                  "Attempting to unlock %s : %s , but this thread does not own the lock.",
                  this, name));
        }

        synchronized (token) {
          token.checkForExpiration();
          rThread = token.getLesseeThread();
          if (!token.isLeaseHeldByCurrentOrRemoteThread(rThread)) {
            token.throwIfCurrentThreadHadExpiredLease();
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE, "{}, [unlock] {} not leased by this thread.",
                  this, token);
            }
            throw new LockNotHeldException(
                String.format(
                    "Attempting to unlock %s : %s , but this thread does not own the lock. %s",
                    this, name, token));
          }
          // if recursion > 0 then token will still be locked after calling release
          hadRecursion = token.getRecursion() > 0;
          lockId = token.getLeaseId();
          Assert.assertTrue(lockId > -1);
          if (hadRecursion) {
            unlocked = token.releaseLock(lockId, rThread);
          } else {
            token.setIgnoreForRecovery(true);
          }
        } // token sync
      } // tokens map sync

      if (!hadRecursion) {
        boolean lockBatch = false;
        boolean released = false;

        while (!released) {
          checkDestroyed();
          LockGrantorId theLockGrantorId = getLockGrantorId();
          try {
            synchronized (lockGrantorIdLock) {
              unlocked = token.releaseLock(lockId, rThread);
            }
            released = callReleaseProcessor(theLockGrantorId.getLockGrantorMember(), name,
                lockBatch, lockId);

          } catch (LockGrantorDestroyedException e) { // part of fix for bug 35239
            // loop back around to get next lock grantor
          } catch (LockServiceDestroyedException e) { // part of fix for bug 35239
            // done... NonGrantorDestroyedMessage will release locks for us
            released = true;
          } finally {
            if (!released) {
              notLockGrantorId(theLockGrantorId, 0, TimeUnit.MILLISECONDS);
            }
          }
        } // while !released
      } // !hadRecursion

    } // try
    finally {
      try {
        if (!hadRecursion && lockId > -1 && token != null) {
          decActiveLocks();
          if (!unlocked) {
            token.releaseLock(lockId, rThread);
          }
        }
      } finally {
        getStats().endLockRelease(statStart);
        if (automateFreeResources) {
          freeResources(name);
        }
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE, "{}, name: {} - exiting unlock()", this, name);
        }
      }
    }
  }

  /**
   * Query the grantor for current leasing information of a lock. Returns the current lease info.
   *
   * @param name the named lock to get lease information for
   * @return snapshot of the remote lock information
   * @throws LockServiceDestroyedException if local instance of lock service has been destroyed
   */
  public DLockRemoteToken queryLock(final Object name) {

    DLockQueryReplyMessage queryReply = null;
    while (queryReply == null || queryReply.repliedNotGrantor()) {
      checkDestroyed();
      LockGrantorId theLockGrantorId = getLockGrantorId();
      try {
        queryReply = DLockQueryProcessor.query(theLockGrantorId.getLockGrantorMember(),
            serviceName, name, false /* lockBatch */, dm);
      } catch (LockGrantorDestroyedException e) {
        // loop back around to get next lock grantor
      } finally {
        if (queryReply != null && queryReply.repliedNotGrantor()) {
          notLockGrantorId(theLockGrantorId, 0, TimeUnit.MILLISECONDS);
        }
      }
    } // while querying

    return DLockRemoteToken.create(name, queryReply.getLesseeThread(), queryReply.getLeaseId(),
        queryReply.getLeaseExpireTime());

  }

  // -------------------------------------------------------------------------
  // Creation methods
  // -------------------------------------------------------------------------

  /**
   * Factory method for creating a new instance of <code>DLockService</code>. This ensures that
   * adding the {@link #disconnectListener} is done while synchronized on the fully constructed
   * instance.
   * <p>
   * Caller must be synchronized on {@link DLockService#services}.
   *
   * @see org.apache.geode.distributed.DistributedLockService#create(String, DistributedSystem)
   */
  static DLockService basicCreate(String serviceName, InternalDistributedSystem ds,
      boolean isDistributed,
      boolean automateFreeResources)
      throws IllegalArgumentException {
    Assert.assertHoldsLock(services, true);

    if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
      logger.trace(LogMarker.DLS_VERBOSE, "About to create DistributedLockService <{}>",
          serviceName);
    }

    DLockService svc = new DLockService(serviceName, ds, isDistributed,
        automateFreeResources);
    svc.init();
    return svc;
  }

  /** initialize this DLockService object */
  protected boolean init() {
    boolean success = false;
    try {
      services.put(serviceName, this);
      getStats().incServices(1);
      ds.addDisconnectListener(disconnectListener);
      success = true;
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "Created DistributedLockService <{}>",
            serviceName);
      }
    } finally {
      if (!success) {
        services.remove(serviceName);
        getStats().incServices(-1);
      }
    }

    ds.handleResourceEvent(ResourceEvent.LOCKSERVICE_CREATE, this);

    return success;
  }

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  /**
   * To create an instance, use DistributedLockService.create(Object, DistributedSystem) or
   * DLockService.create(Object, DistributedSystem, DistributionAdvisor)
   */
  protected DLockService(String serviceName, DistributedSystem ds, boolean isDistributed,
      boolean automateFreeResources) {
    super();
    dlockStats = getOrCreateStats(ds);
    serialNumber = createSerialNumber();
    this.serviceName = serviceName;
    this.ds = (InternalDistributedSystem) ds;
    dm = this.ds.getDistributionManager();
    stopper = new DLockStopper(this);
    this.isDistributed = isDistributed;
    // True if this service should be destroyed in system DisconnectListener
    this.automateFreeResources = automateFreeResources || AUTOMATE_FREE_RESOURCES;
  }

  // -------------------------------------------------------------------------
  // java.lang.Object methods
  // -------------------------------------------------------------------------

  @Override
  public String toString() {
    return '<' + "DLockService" + "@"
        + Integer.toHexString(System.identityHashCode(this)) + " named "
        + serviceName + " destroyed=" + destroyed + " grantorId="
        + lockGrantorId + " grantor=" + grantor + '>';
  }

  // -------------------------------------------------------------------------
  // Public instance methods
  // -------------------------------------------------------------------------

  public DistributedLockStats getStats() {
    return dlockStats;
  }

  public void releaseTryLocks(DLockBatchId batchId, Callable<Boolean> untilCondition) {
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS_VERBOSE, "[DLockService.releaseTryLocks] enter: {}", batchId);
    }

    long statStart = getStats().startLockRelease();

    try {
      boolean lockBatch = true;
      boolean released = false;
      while (!released) {
        try {
          boolean quit = untilCondition.call();
          if (quit) {
            return;
          }
        } catch (Exception e) {
          throw new InternalGemFireException("unexpected exception", e);
        }
        checkDestroyed();

        final LockGrantorId theLockGrantorId = batchId.getLockGrantorId();
        synchronized (lockGrantorIdLock) {
          if (!checkLockGrantorId(theLockGrantorId)) {
            // the grantor is different so break and skip DLockReleaseProcessor
            break;
          }
        }

        released =
            callReleaseProcessor(theLockGrantorId.getLockGrantorMember(), batchId, lockBatch, -1);
        if (!released) {
          notLockGrantorId(theLockGrantorId, 100, TimeUnit.MILLISECONDS);
        }
      }
    } finally {
      decActiveLocks();
      getStats().endLockRelease(statStart);
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE, "[DLockService.releaseTryLocks] exit: {}", batchId);
      }
    }
  }

  public boolean acquireTryLocks(final DLockBatch dlockBatch, final long waitTimeMillis,
      final long leaseTimeMillis, final Object[] keyIfFailed) throws InterruptedException {
    checkDestroyed();
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    if (keyIfFailed.length < 1) {
      throw new IllegalArgumentException(
          "keyIfFailed must have a length of one or greater");
    }

    long startTime = getLockTimeStamp(dm);

    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS_VERBOSE, "[acquireTryLocks] acquiring {}", dlockBatch);
    }

    long requestWaitTime = waitTimeMillis;
    long requestLeaseTime = leaseTimeMillis;

    // -1 means "lease forever". Long.MAX_VALUE is pretty close.
    if (requestLeaseTime == -1) {
      requestLeaseTime = Long.MAX_VALUE;
    }

    // -1 means "wait forever". Long.MAX_VALUE is pretty close.
    if (requestWaitTime == -1) {
      requestWaitTime = Long.MAX_VALUE;
    }
    long waitLimit = startTime + requestWaitTime;
    if (waitLimit < 0) {
      waitLimit = Long.MAX_VALUE;
    }

    long statStart = getStats().startLockWait();
    boolean gotLocks = false;

    try {
      ThreadRequestState requestState = threadRequestState.get();
      if (requestState == null) {
        requestState = new ThreadRequestState(incThreadSequence(), false);
        threadRequestState.set(requestState);
      } else {
        requestState.interruptible = false;
      }
      final int threadId = requestState.threadId;

      boolean keepTrying = true;
      incActiveLocks();

      while (keepTrying) {
        checkDestroyed();
        LockGrantorId theLockGrantorId = getLockGrantorId();

        boolean tryLock = true;
        boolean reentrant = false;
        DLockRequestProcessor processor = createRequestProcessor(theLockGrantorId, dlockBatch,
            threadId, startTime, requestLeaseTime, requestWaitTime, reentrant, tryLock, false);
        boolean interruptible = true;
        int lockId = processor.getProcessorId();
        gotLocks = processor.requestLock(interruptible, lockId);
        if (gotLocks) {
          dlockBatch.grantedBy(theLockGrantorId);
        } else if (processor.repliedDestroyed()) {
          checkDestroyed();
          // should have thrown LockServiceDestroyedException
          Assert.assertTrue(isDestroyed(), "Grantor reports service " + this + " is destroyed");
        } else if (processor.repliedNotGrantor() || processor.hadNoResponse()) {
          notLockGrantorId(theLockGrantorId, 0, TimeUnit.MILLISECONDS);
        } else {
          keyIfFailed[0] = processor.getKeyIfFailed();
          if (keyIfFailed[0] == null) {
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE,
                  "[acquireTryLocks] lock request failed but provided no conflict key; responseCode={}",
                  processor.getResponseCodeString());
            }
          } else {
            break;
          }
        }

        long timeLeft = requestWaitTime;
        if (requestWaitTime < Long.MAX_VALUE) {
          // prevent txLock from performing next line...
          timeLeft = waitLimit - getLockTimeStamp(dm);
        }
        keepTrying = !gotLocks && timeLeft > 0;
        if (keepTrying && timeLeft > 10) {
          // didn't receive msg or processor timed out... sleep briefly
          try {
            Thread.sleep(10);
          } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
          }
        }
      }

      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE, "[acquireTryLocks] {} locks for {}",
            (gotLocks ? "acquired" : "failed to acquire"), dlockBatch);
      }
    } finally {
      getStats().endLockWait(statStart, gotLocks);
    }
    return gotLocks;
  }

  /**
   * Returns copy of the tokens map. Synchronizes on token map.
   * <p>
   * Called by {@link org.apache.geode.internal.admin.remote.FetchDistLockInfoResponse}.
   *
   * @return copy of the tokens map
   */
  public Map<Object, DLockToken> snapshotService() {
    synchronized (tokens) {
      return new HashMap<>(tokens);
    }
  }

  /**
   * Used for instrumenting blocked threads
   */
  public UnsafeThreadLocal<Object> getBlockedOn() {
    return blockedOn;
  }

  /** Returns true if the lock service is distributed; false if local only */
  public boolean isDistributed() {
    return isDistributed;
  }

  public void setDLockLessorDepartureHandler(DLockLessorDepartureHandler handler) {
    lessorDepartureHandler = handler;
  }

  public DLockLessorDepartureHandler getDLockLessorDepartureHandler() {
    return lessorDepartureHandler;
  }

  /** The name of this service */
  public String getName() {
    return serviceName;
  }

  public DistributionManager getDistributionManager() {
    return dm;
  }

  public void setDLockRecoverGrantorMessageProcessor(
      DLockRecoverGrantorProcessor.MessageProcessor recoverGrantorProcessor) {
    this.recoverGrantorProcessor = recoverGrantorProcessor;
  }

  public DLockRecoverGrantorProcessor.MessageProcessor getDLockRecoverGrantorMessageProcessor() {
    return recoverGrantorProcessor;
  }

  /**
   * @see org.apache.geode.distributed.DistributedLockService#destroy(String)
   */
  public static void destroyServiceNamed(String serviceName) throws IllegalArgumentException {
    final DLockService svc;
    synchronized (services) {
      svc = services.get(serviceName);
    }
    if (svc == null) {
      throw new IllegalArgumentException(
          String.format("Service named %s not created", serviceName));
    } else {
      svc.destroyAndRemove();
    }
  }

  /** Destroys all lock services in this VM. Used in test tearDown code. */
  public static void destroyAll() {
    Collection<DLockService> svcs;
    synchronized (services) {
      svcs = new HashSet<>(services.values());
    }
    for (DLockService svc : svcs) {
      try {
        svc.destroyAndRemove();
      } catch (CancelException e) {
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "destroyAndRemove of {} terminated due to cancellation: ", svc, e);
        }
      }
    }
  }

  /**
   * Destroys an existing service and removes it from the map
   *
   * @since GemFire 3.5
   */
  public void destroyAndRemove() {
    // isLockGrantor determines if we need to tell elder of destroy
    boolean isCurrentlyLockGrantor = false;
    boolean isMakingLockGrantor = false;

    // maybeHasActiveLocks determines if we need to tell grantor of destroy
    boolean maybeHasActiveLocks = false;

    synchronized (creationLock) {
      try {
        synchronized (services) {
          try {
            if (isDestroyed()) {
              return;
            }
            setDestroyingThread();
            synchronized (lockGrantorIdLock) { // force ordering in lock request
              synchronized (destroyLock) {
                destroyed = true;
                maybeHasActiveLocks = activeLocks > 0;
              }
              isCurrentlyLockGrantor = isCurrentlyLockGrantor();
              isMakingLockGrantor = isMakingLockGrantor();
            }
          } finally {
            if (isCurrentThreadDoingDestroy()) {
              removeLockService(this);
            }
          }
        } // services sync
      } catch (CancelException e) {
        // don't report to caller
      } finally {
        if (isCurrentThreadDoingDestroy()) {
          try {
            basicDestroy(isCurrentlyLockGrantor, isMakingLockGrantor, maybeHasActiveLocks);
          } catch (CancelException e) {
            // don't propagate
          } finally {
            clearDestroyingThread();
          }
        }
        postDestroyAction();
      }
    } // creationLock sync
  }

  /**
   * Unlock all locks currently held by this process, and mark it as destroyed. Returns true if
   * caller performed actual destroy. Returns false if this lock service has already been destroyed.
   * <p>
   * {@link #services} is held for the entire operation. {@link #destroyLock} is held only while
   * setting {@link #destroyed} to true and just prior to performing any real work.
   * <p>
   * Caller must be synchronized on {@link DLockService#services};
   */
  private void basicDestroy(boolean isCurrentlyLockGrantor, boolean isMakingLockGrantor,
      boolean maybeHasActiveLocks) {
    Assert.assertHoldsLock(services, false);
    // synchronized (this.serviceLock) {
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS_VERBOSE,
          "[DLockService.basicDestroy] Destroying {}, isCurrentlyLockGrantor={}, isMakingLockGrantor={}",
          this, isCurrentlyLockGrantor, isMakingLockGrantor);
    }

    // if hasActiveLocks, tell grantor we're destroying...
    if (!isCurrentlyLockGrantor && maybeHasActiveLocks && !ds.isDisconnectThread()) {
      boolean retry;
      int nonGrantorDestroyLoopCount = 0;
      do {
        retry = false;
        LockGrantorId theLockGrantorId = peekLockGrantorId();

        if (theLockGrantorId != null && !theLockGrantorId.isLocal(getSerialNumber())) {
          if (!NonGrantorDestroyedProcessor.send(serviceName, theLockGrantorId, dm)) {
            // grantor responded NOT_GRANTOR
            notLockGrantorId(theLockGrantorId, 0, TimeUnit.MILLISECONDS); // nulls out grantor to
                                                                          // force call to elder
            retry = true;
          }
        }

        if (DEBUG_NONGRANTOR_DESTROY_LOOP) {
          nonGrantorDestroyLoopCount++;
          if (nonGrantorDestroyLoopCount >= DEBUG_NONGRANTOR_DESTROY_LOOP_COUNT) {
            logger.fatal(LogMarker.DLS_MARKER,
                "Failed to notify grantor of destruction within {} attempts.",
                DEBUG_NONGRANTOR_DESTROY_LOOP_COUNT);
            Assert.assertTrue(false,
                String.format("Failed to notify grantor of destruction within %s attempts.",

                    DEBUG_NONGRANTOR_DESTROY_LOOP_COUNT));
          }
        }

      } while (retry);
    }

    if (isCurrentlyLockGrantor || isMakingLockGrantor) {
      // If forcedDisconnect is in progress, the membership view will not
      // change and no-one else can contact this member, so don't wait for a grantor
      if (ds.getCancelCriterion().isCancelInProgress()) {
        try {
          DLockGrantor.waitForGrantor(this);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (DistributedSystemDisconnectedException e) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "No longer waiting for grantor because of disconnect.", e);
          }
        }
      }
      nullLockGrantorId(lockGrantorId);
    }
    // }
  }

  protected void postDestroyAction() {
    ds.handleResourceEvent(ResourceEvent.LOCKSERVICE_REMOVE, this);
  }

  /**
   * Called by grantor recovery to return set of locks held by this process. Synchronizes on
   * lockGrantorIdLock, tokens map, and each lock token.
   *
   * @param newlockGrantorId the newly recovering grantor
   */
  Set<DLockRemoteToken> getLockTokensForRecovery(LockGrantorId newlockGrantorId) {
    Set<DLockRemoteToken> heldLockSet = emptySet();

    final LockGrantorId currentLockGrantorId;
    synchronized (lockGrantorIdLock) {
      if (isDestroyed()) {
        return heldLockSet;
      }

      currentLockGrantorId = lockGrantorId;
    }

    // destroy local grantor if currentLockGrantorId is local
    // and grantorVersion is greater than currentGrantorVersion
    if (currentLockGrantorId != null && currentLockGrantorId.hasLockGrantorVersion()
        && newlockGrantorId.isNewerThan(currentLockGrantorId)) {
      nullLockGrantorId(currentLockGrantorId);
    }

    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
    synchronized (lockGrantorIdLock) {
      synchronized (tokens) {
        // build up set of currently held locks
        for (DLockToken token : tokens.values()) {
          synchronized (token) {
            if (token.isLeaseHeld()) {

              // skip over token if ignoreForRecovery is true
              if (token.ignoreForRecovery()) {
                // unlock of token must be in progress... ignore for recovery
                if (isDebugEnabled_DLS) {
                  logger.trace(LogMarker.DLS_VERBOSE, "getLockTokensForRecovery is skipping {}",
                      token);
                }
              }

              // add token to heldLockSet
              else {
                if (heldLockSet == Collections.EMPTY_SET) {
                  heldLockSet = new HashSet<>();
                }
                heldLockSet.add(DLockRemoteToken.createFromDLockToken(token));
              }
            } // isLeaseHeld
          } // token sync
        } // tokens iter
      } // tokens sync

      return heldLockSet;
    }
  }

  /**
   * Returns the named lock token or null if it doesn't exist. Synchronizes on tokens map.
   *
   * @return the named lock token or null if it doesn't exist
   */
  public DLockToken getToken(Object name) {
    synchronized (tokens) {
      return tokens.get(name);
    }
  }

  /**
   * Returns the named lock token or null if it doesn't exist. Caller must synchronize on tokens
   * map.
   *
   * @return the named lock token or null if it doesn't exist
   */
  private DLockToken basicGetToken(Object name) {
    return tokens.get(name);
  }

  /**
   * Returns an unmodifiable collection backed by the values of the DLockToken map for testing
   * purposes only. Synchronizes on tokens map.
   *
   * @return an unmodifiable collection of the tokens map values
   */
  public Collection<DLockToken> getTokens() {
    synchronized (tokens) {
      return Collections.unmodifiableCollection(tokens.values());
    }
  }

  /**
   * Returns an existing or creates a new DLockToken. Synchronizes on tokens map and the lock token.
   *
   * @param name the name of the lock
   * @return an existing or new instantiated lock token
   */
  DLockToken getOrCreateToken(Object name) {
    synchronized (tokens) {
      checkDestroyed(); // check destroy after acquiring tokens map sync
      DLockToken token = tokens.get(name);
      boolean createNewToken = token == null;
      if (createNewToken) {
        token = new DLockToken(dm, name);
      }
      synchronized (token) {
        if (createNewToken) {
          tokens.put(name, token);
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "Creating {} in {}", token, this);
          }
          getStats().incTokens(1);
        }
        token.incUsage();
      }
      return token;
    }
  }

  // -------------------------------------------------------------------------
  // Private instance methods
  // -------------------------------------------------------------------------

  /**
   * Returns number of lock tokens currently leased by this member. Synchronizes on tokens map and
   * each lock token.
   *
   * @return number of lock tokens currently leased by this member
   */
  private int numLocksHeldInThisVM() {
    int numLocksHeld = 0;
    synchronized (tokens) {
      for (DLockToken token : tokens.values()) {
        synchronized (token) {
          if (token.isLeaseHeld()) {
            numLocksHeld++;
          }
        }
      }
    }
    return numLocksHeld;
  }

  /**
   * TEST HOOK: Logs all lock tokens for this service at INFO level. Synchronizes on tokens map and
   * each lock token.
   */
  protected void dumpService() {
    synchronized (tokens) {
      StringBuilder buffer = new StringBuilder();
      buffer.append("  ").append(tokens.size()).append(" tokens, ");
      buffer.append(numLocksHeldInThisVM()).append(" locks held\n");
      for (Map.Entry<Object, DLockToken> entry : tokens.entrySet()) {
        buffer.append("    ").append(entry.getKey()).append(": ");
        DLockToken token = entry.getValue();
        buffer.append(token.toString()).append("\n");
      }
      logger.info(LogMarker.DLS_MARKER, buffer);
    }
  }

  // ----------- new thread state for interruptible and threadId ------------
  private final AtomicInteger threadSequence = new AtomicInteger();

  protected static class ThreadRequestState {
    protected final int threadId;
    protected boolean interruptible;

    ThreadRequestState(int threadId, boolean interruptible) {
      this.threadId = threadId;
      this.interruptible = interruptible;
    }
  }

  private final ThreadLocal<ThreadRequestState> threadRequestState = new ThreadLocal<>();

  private final UnsafeThreadLocal<Object> blockedOn = new UnsafeThreadLocal<>();

  /**
   * Returns true if the calling thread has an active lock request that is interruptible
   */
  boolean isInterruptibleLockRequest() {
    // this method is called by grantor for local lock requests in grantor vm
    final ThreadRequestState requestState = threadRequestState.get();
    if (requestState == null) {
      return false;
    }
    return requestState.interruptible;
  }

  ThreadLocal<ThreadRequestState> getThreadRequestState() {
    return threadRequestState;
  }

  protected int incThreadSequence() {
    return threadSequence.incrementAndGet();
  }

  // -------------------------------------------------------------------------
  // System disconnect listener
  // -------------------------------------------------------------------------

  /** Destroys all named locking services on disconnect from system */
  @MakeNotStatic("This object is actually immutable ... but it uses singletons")
  protected static final InternalDistributedSystem.DisconnectListener disconnectListener =
      new InternalDistributedSystem.DisconnectListener() {
        @Override
        public String toString() {
          return "Disconnect listener for DistributedLockService";
        }

        @Override
        public void onDisconnect(final InternalDistributedSystem sys) {
          final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE, "Shutting down Distributed Lock Services");
          }
          long start = System.currentTimeMillis();
          try {
            destroyAll();
          } finally {
            closeStats();
            long delta = System.currentTimeMillis() - start;
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE, "Distributed Lock Services stopped (took {} ms)",
                  delta);
            }
          }
        }
      };

  // ----------------------------------------------------------------

  @Immutable
  private static final DummyDLockStats DUMMY_STATS = new DummyDLockStats();

  @Immutable
  static final SuspendLockingToken SUSPEND_LOCKING_TOKEN = new SuspendLockingToken();

  // -------------------------------------------------------------------------
  // Static fields
  // -------------------------------------------------------------------------

  /** Map of all locking services. */
  @MakeNotStatic
  protected static final Map<String, DLockService> services = new HashMap<>();

  private static final Object creationLock = new Object();

  /** DLock statistics; static because multiple dlock instances can exist */
  @MakeNotStatic
  private static DistributedLockStats stats = DUMMY_STATS;

  // -------------------------------------------------------------------------
  // Reserved lock service names
  // -------------------------------------------------------------------------

  public static final String LTLS = "LTLS";
  public static final String DTLS = "DTLS";
  private static final String[] reservedNames = new String[] {LTLS, DTLS};

  // -------------------------------------------------------------------------
  // DLS serial number (uniquely identifies local instance of DLS)
  // -------------------------------------------------------------------------

  /**
   * Specifies the starting serial number for the serialNumberSequencer
   */
  public static final int START_SERIAL_NUMBER = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "DistributedLockService.startSerialNumber", 1);

  /**
   * Incrementing serial number used to identify order of DLS creation
   *
   * @see DLockService#getSerialNumber()
   */
  @MakeNotStatic
  private static final AtomicInteger serialNumberSequencer = new AtomicInteger(START_SERIAL_NUMBER);

  /**
   * Identifies the static order in which this DLS was created in relation to other DLS or other
   * instances of this DLS during the life of this JVM. Rollover to negative is allowed.
   */
  private final int serialNumber;

  /**
   * Generates a serial number for identifying a DLS. Later instances of the same named DLS will
   * have a greater serial number than earlier instances. This number increments statically
   * throughout the life of this JVM. Rollover to negative is allowed.
   *
   * @return the new serial number
   */
  protected static int createSerialNumber() {
    // NOTE: AtomicInteger should rollover if value is Integer.MAX_VALUE
    return serialNumberSequencer.incrementAndGet();
  }

  /**
   * Returns the serial number which identifies the static order in which this DLS was created in
   * relation to other DLS'es or other instances of this named DLS during the life of this JVM.
   */
  public int getSerialNumber() {
    return serialNumber;
  }

  // -------------------------------------------------------------------------
  // External API methods
  // -------------------------------------------------------------------------
  public static DistributedLockService getOrCreateService(String serviceName,
      InternalDistributedSystem ds) {
    DistributedLockService cmsLockService = DLockService.getServiceNamed(serviceName);
    try {
      if (cmsLockService == null) {
        cmsLockService = DLockService.create(serviceName, ds, true);
      }
    } catch (IllegalArgumentException ignore) {
      return DLockService.getServiceNamed(serviceName);
    }
    return cmsLockService;
  }

  /**
   * @see org.apache.geode.distributed.DistributedLockService#getServiceNamed(String)
   */
  public static DistributedLockService getServiceNamed(String serviceName) {
    synchronized (services) {
      return services.get(serviceName);
    }
  }

  public static DistributedLockService create(String serviceName, InternalDistributedSystem ds,
      boolean distributed) {
    return create(serviceName, ds, distributed, false);
  }

  /**
   * Creates named <code>DistributedLockService</code>.
   *
   * @param serviceName name of the service
   * @param ds InternalDistributedSystem
   * @param distributed true if lock service will be distributed; false will be local only
   * @param automateFreeResources true if freeResources should be automatically called during unlock
   *
   * @throws IllegalArgumentException if serviceName is invalid or this process has already created
   *         named service
   *
   * @throws IllegalStateException if system is in process of disconnecting
   *
   * @see org.apache.geode.distributed.DistributedLockService#create(String, DistributedSystem)
   */
  @NotNull
  public static DistributedLockService create(String serviceName, InternalDistributedSystem ds,
      boolean distributed,
      boolean automateFreeResources)
      throws IllegalArgumentException, IllegalStateException {
    // basicCreate will construct DLockService and it calls getOrCreateStats...
    synchronized (creationLock) {
      synchronized (services) { // disconnectListener syncs on this
        ds.getCancelCriterion().checkCancelInProgress(null);
        if (services.get(serviceName) != null) {
          throw new IllegalArgumentException(
              String.format("Service named %s already created",
                  serviceName));
        }
        return DLockService.basicCreate(serviceName, ds, distributed,
            automateFreeResources);
      }
    }
  }

  public static void becomeLockGrantor(String serviceName) throws IllegalArgumentException {
    becomeLockGrantor(serviceName, null);
  }

  public static void becomeLockGrantor(String serviceName, InternalDistributedMember oldTurk)
      throws IllegalArgumentException {
    if (serviceName == null || serviceName.length() == 0) {
      throw new IllegalArgumentException(String.format("Service named '%s' is not valid",
          serviceName));
    }
    DLockService svc = getInternalServiceNamed(serviceName);
    if (svc == null) {
      throw new IllegalArgumentException(
          String.format("Service named %s not created", serviceName));
    } else {
      svc.becomeLockGrantor(oldTurk);
    }
  }

  public static boolean isLockGrantor(String serviceName) throws IllegalArgumentException {
    if (serviceName == null || serviceName.length() == 0) {
      throw new IllegalArgumentException(String.format("Service named '%s' is not valid",
          serviceName));
    }

    final DLockService svc;
    synchronized (services) {
      svc = services.get(serviceName);
    }
    if (svc == null) {
      throw new IllegalArgumentException(
          String.format("Service named %s not created", serviceName));
    } else {
      return svc.isLockGrantor();
    }
  }

  /**
   * Fills lists of service names.
   *
   * @param grantors filled with service names of all services we are currently the grantor of.
   * @param grantorVersions elder assigned grantor version ids
   * @param grantorSerialNumbers member specific DLS serial number hosting grantor
   * @param nonGrantors filled with service names of all services we have that we are not the
   *        grantor of.
   */
  public static void recoverRmtElder(ArrayList<String> grantors, ArrayList<Long> grantorVersions,
      ArrayList<Integer> grantorSerialNumbers, ArrayList<String> nonGrantors) {
    synchronized (services) {
      for (final Map.Entry<String, DLockService> entry : services.entrySet()) {
        String serviceName = entry.getKey();
        DLockService service = entry.getValue();
        boolean foundGrantor = false;
        DLockGrantor grantor = service.getGrantor();
        if (grantor != null && grantor.getVersionId() != -1 && !grantor.isDestroyed()) {
          foundGrantor = true;
          grantors.add(serviceName);
          grantorVersions.add(grantor.getVersionId());
          grantorSerialNumbers.add(service.getSerialNumber());
        }
        if (!foundGrantor) {
          nonGrantors.add(serviceName);
        }
      }
    }
  }

  /**
   * Called when an elder is doing recovery. For every service that we are the grantor for add it to
   * the grantorMap For every service we have that is not in the grantorMap add its name to
   * needsRecovery set.
   *
   * @param dm our local DM
   */
  public static void recoverLocalElder(DistributionManager dm, Map<String, GrantorInfo> grantors,
      Set<String> needsRecovery) {
    synchronized (services) {
      for (final Map.Entry<String, DLockService> entry : services.entrySet()) {
        String serviceName = entry.getKey();
        DLockService service = entry.getValue();
        boolean foundGrantor = false;
        DLockGrantor grantor = service.getGrantor();
        if (grantor != null && grantor.getVersionId() != -1 && !grantor.isDestroyed()) {
          foundGrantor = true;
          GrantorInfo oldgi = grantors.get(serviceName);
          if (oldgi == null || oldgi.getVersionId() < grantor.getVersionId()) {
            grantors.put(serviceName, new GrantorInfo(dm.getId(), grantor.getVersionId(),
                service.getSerialNumber(), false));
            needsRecovery.remove(serviceName);
          }
        }
        if (!foundGrantor && !dm.isLoner()) {
          if (!grantors.containsKey(serviceName)) {
            needsRecovery.add(serviceName);
          }
        }
      }
    }
  }
  // -------------------------------------------------------------------------
  // Public static methods
  // -------------------------------------------------------------------------

  /** Convenience method to get named DLockService */
  public static DLockService getInternalServiceNamed(String serviceName) {
    synchronized (services) {
      return services.get(serviceName);
    }
  }

  /** Validates service name for external creation */
  public static void validateServiceName(String serviceName) {
    if (serviceName == null || serviceName.length() == 0) {
      throw new IllegalArgumentException(
          "Lock service name must not be null or empty");
    }
    for (String reservedName : reservedNames) {
      if (serviceName.startsWith(reservedName)) {
        throw new IllegalArgumentException(
            String.format("Service named %s is reserved for internal use only",
                serviceName));
      }
    }
  }

  /** Return a snapshot of all services */
  public static Map<String, DLockService> snapshotAllServices() { // used by: internal/admin/remote
    synchronized (services) {
      return new HashMap<>(services);
    }
  }

  /**
   * TEST HOOK: Logs all lock tokens for every service at INFO level. Synchronizes on services map,
   * service tokens maps and each lock token.
   */
  public static void dumpAllServices() { // used by: distributed/DistributedLockServiceTest
    synchronized (services) {
      logger.info(LogMarker.DLS_MARKER,
          "DLockService.dumpAllServices() - " + services.size() + " services:\n");
      for (final Map.Entry<String, DLockService> entry : services.entrySet()) {
        DLockService svc = entry.getValue();
        svc.dumpService();
        if (svc.isCurrentlyLockGrantor()) {
          svc.grantor.dumpService();
        }

      }
    }
  }

  /**
   * Return a timestamp that represents the current time for locking purposes.
   *
   * @since GemFire 3.5
   */
  static long getLockTimeStamp(DistributionManager dm) {
    return dm.cacheTimeMillis();
  }

  /** Get or create static dlock stats */
  protected static synchronized DistributedLockStats getOrCreateStats(DistributedSystem ds) {
    if (stats == DUMMY_STATS) {
      Assert.assertTrue(ds != null, "Need an instance of InternalDistributedSystem");
      long statId = OSProcess.getId();
      stats = new DLockStats(ds, statId);
    }
    return stats;
  }

  protected static synchronized DistributedLockStats getDistributedLockStats() {
    return stats;
  }

  public static void addLockServiceForTests(String name, DLockService service) {
    synchronized (services) {
      services.put(name, service);
    }
  }

  public static void removeLockServiceForTests(String name) {
    synchronized (services) {
      services.remove(name);
    }
  }

  protected static void removeLockService(DLockService service) {
    service.removeAllTokens();

    final InternalDistributedSystem system;
    synchronized (services) {
      DLockService removedService = services.remove(service.getName());
      if (removedService == null) {
        // another thread beat us to the removal... return
        return;
      }
      if (removedService != service) {
        services.put(service.getName(), removedService);
      } else {
        service.getStats().incServices(-1);
      }
      system = removedService.getDistributionManager().getSystem();
    }
    // if disconnecting and this was the last service, cleanup!
    if (services.isEmpty() && system.isDisconnecting()) {
      // get rid of stats and thread group...
      synchronized (DLockService.class) {
        closeStats();
      }
    }
  }

  static void closeStats() {
    if (stats != DUMMY_STATS) {
      ((DLockStats) stats).close();
      stats = DUMMY_STATS;
    }
  }

  /** Provide way to peek at current lock grantor id when dls does not exist */
  static GrantorInfo checkLockGrantorInfo(String serviceName, InternalDistributedSystem system) {
    GrantorInfo gi = GrantorRequestProcessor.peekGrantor(serviceName, system);
    if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
      logger.trace(LogMarker.DLS_VERBOSE, "[checkLockGrantorId] returning {}", gi);
    }
    return gi;
  }

  // -------------------------------------------------------------------------
  // SuspendLockingToken inner class
  // -------------------------------------------------------------------------

  /** Used as the name (key) for the suspend locking entry in the tokens map */
  @Immutable
  public static class SuspendLockingToken implements DataSerializableFixedID {
    public SuspendLockingToken() {}

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      return o instanceof SuspendLockingToken;
    }

    @Override
    public int hashCode() {
      // Since instances always equal each other, they return the same hashCode
      return 15325;
    }

    @Override
    public int getDSFID() {
      return SUSPEND_LOCKING_TOKEN;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) {}

    @Override
    public void toData(DataOutput out,
        SerializationContext context) {}

    @Override
    public KnownVersion[] getSerializationVersions() {
      return null;
    }
  }

  /**
   * CancelCriterion which provides cancellation for DLS request and release messages. Cancellation
   * may occur if shutdown or if DLS is destroyed.
   */
  private static class DLockStopper extends CancelCriterion {

    /**
     * The DLockService this stopper will check for cancellation.
     */
    private final DLockService dls;

    /**
     * Creates a new DLockStopper for the specified DLockService and DM.
     *
     * @param dls the DLockService to check for DLS destroy
     */
    DLockStopper(final @NotNull DLockService dls) {
      this.dls = dls;
      Assert.assertTrue(dls.getDistributionManager() != null);
    }

    @Override
    public String cancelInProgress() {
      String cancelInProgressString =
          dls.getDistributionManager().getCancelCriterion().cancelInProgress();
      if (cancelInProgressString != null) {
        // delegate to underlying DM's CancelCriterion...
        return cancelInProgressString;
      } else if (dls.isDestroyed()) {
        // this.stoppedByDLS = true;
        return dls.generateLockServiceDestroyedMessage();
      } else {
        // return null since neither DM nor DLS are shutting down
        // cannot call super.cancelInProgress because it's abstract
        return null;
      }
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      return dls.generateLockServiceDestroyedException(reason);
    }

  }

  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

}
