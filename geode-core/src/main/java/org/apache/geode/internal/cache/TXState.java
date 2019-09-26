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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

import javax.transaction.Status;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.FailedSynchronizationException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.TransactionWriter;
import org.apache.geode.cache.TransactionWriterException;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.client.internal.ServerRegionDataAccess;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.TXManagerCancelledException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.partitioned.PutAllPRMessage;
import org.apache.geode.internal.cache.partitioned.RemoveAllPRMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.TransactionalOperation.ServerRegionOperation;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.statistics.StatisticsClock;

/**
 * TXState is the entity that tracks the transaction state on a per thread basis, noting changes to
 * Region entries on a per operation basis. It lives on the node where transaction data exists.
 *
 * @since GemFire 4.0
 * @see TXManagerImpl
 */
public class TXState implements TXStateInterface {
  protected static final Logger logger = LogService.getLogger();

  // The nano-timestamp of when the transaction began
  private final long beginTime;
  // A map of transaction state by Region
  final IdentityHashMap<InternalRegion, TXRegionState> regions;

  /** whether completion has been started */
  protected boolean completionStarted;

  /** whether the transaction has been completed and cleaned up */
  protected boolean closed = false;

  /** guards the completionStarted boolean and the closed boolean */
  protected final Object completionGuard = new Object();

  protected TXLockRequest locks = null;
  // Used for jta commit lifetime
  private long jtaLifeTime;
  /**
   * Used to hand out modification serial numbers used to preserve the order of operation done by
   * this transaction.
   */
  private int modSerialNum;
  private final List<EntryEventImpl> pendingCallbacks = new ArrayList<EntryEventImpl>();
  // Access this variable should be in synchronized block.
  private boolean beforeCompletionCalled;

  /**
   * for client/server JTA transactions we need to have a single thread handle both beforeCompletion
   * and afterCompletion so that beforeCompletion can obtain locks for the afterCompletion step.
   * This is that thread
   */
  private final SingleThreadJTAExecutor singleThreadJTAExecutor;

  // Internal testing hooks
  private Runnable internalAfterReservation;
  protected Runnable internalAfterConflictCheck;
  protected Runnable internalDuringApplyChanges;
  protected Runnable internalAfterApplyChanges;
  protected Runnable internalAfterReleaseLocalLocks;
  Runnable internalDuringIndividualSend; // package scope allows TXCommitMessage use
  Runnable internalAfterIndividualSend; // package scope allows TXCommitMessage use
  Runnable internalDuringIndividualCommitProcess; // package scope allows TXCommitMessage use
  Runnable internalAfterIndividualCommitProcess; // package scope allows TXCommitMessage use
  protected Runnable internalAfterSend;
  protected Runnable internalBeforeSend;

  /**
   * Used to generate eventIDs
   */
  private byte[] baseMembershipId;
  /**
   * Used to generate eventIDs
   */
  private long baseThreadId;
  /**
   * Used to generate eventIDs
   */
  private long baseSequenceId;
  protected final TXStateProxy proxy;
  protected boolean firedWriter = false;
  protected final boolean onBehalfOfRemoteStub;
  protected boolean gotBucketLocks = false;
  protected TXCommitMessage commitMessage = null;
  ClientProxyMembershipID bridgeContext = null;
  /** keeps track of events, so as not to re-apply events */
  protected Set<EventID> seenEvents = new HashSet<EventID>();
  /** keeps track of results of txPutEntry */
  private Map<EventID, Boolean> seenResults = new HashMap<EventID, Boolean>();

  @Immutable
  static final TXEntryState ENTRY_EXISTS = new TXEntryState();

  private volatile DistributedMember proxyServer;

  private final StatisticsClock statisticsClock;

  public TXState(TXStateProxy proxy, boolean onBehalfOfRemoteStub,
      StatisticsClock statisticsClock) {
    this(proxy, onBehalfOfRemoteStub, new SingleThreadJTAExecutor(), statisticsClock);
  }

  public TXState(TXStateProxy proxy, boolean onBehalfOfRemoteStub,
      SingleThreadJTAExecutor singleThreadJTAExecutor, StatisticsClock statisticsClock) {
    this.beginTime = statisticsClock.getTime();
    this.regions = new IdentityHashMap<>();

    this.internalAfterConflictCheck = null;
    this.internalAfterApplyChanges = null;
    this.internalAfterReleaseLocalLocks = null;
    this.internalDuringIndividualSend = null;
    this.internalAfterIndividualSend = null;
    this.internalBeforeSend = null;
    this.internalAfterSend = null;
    this.proxy = proxy;
    this.onBehalfOfRemoteStub = onBehalfOfRemoteStub;
    this.singleThreadJTAExecutor = singleThreadJTAExecutor;
    this.statisticsClock = statisticsClock;
  }

  private boolean hasSeenEvent(EntryEventImpl event) {
    assert event != null;
    if (event.getEventId() == null) {
      return false;
    }
    return this.seenEvents.contains(event.getEventId());
  }

  private void recordEvent(EntryEventImpl event) {
    assert event != null;
    if (event.getEventId() != null) {
      this.seenEvents.add(event.getEventId());
    }
  }

  private void recordEventAndResult(EntryEventImpl event, boolean result) {
    recordEvent(event);
    if (event.getEventId() != null) {
      this.seenResults.put(event.getEventId(), result);
    }
  }

  private boolean getRecordedResult(EntryEventImpl event) {
    assert event != null;
    assert this.seenResults.containsKey(event.getEventId());
    return this.seenResults.get(event.getEventId());
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.getClass()).append("@").append(System.identityHashCode(this))
        .append(" onBehalfOfRemoteStub:").append(this.onBehalfOfRemoteStub);
    return builder.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getTransactionId()
   */
  @Override
  public TransactionId getTransactionId() {
    return this.proxy.getTxId();
  }

  public void firePendingCallbacks() {
    for (EntryEventImpl ee : getPendingCallbacks()) {
      if (ee.getOperation().isDestroy()) {
        ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY, ee, true);
      } else if (ee.getOperation().isInvalidate()) {
        ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_INVALIDATE, ee, true);
      } else if (ee.getOperation().isCreate()) {
        ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_CREATE, ee, true);
      } else {
        ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, ee, true);
      }
    }
  }

  public void freePendingCallbacks() {
    for (EntryEventImpl ee : getPendingCallbacks()) {
      ee.release();
    }
  }

  public List<EntryEventImpl> getPendingCallbacks() {
    return pendingCallbacks;
  }


  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.TXStateInterface#readRegion(org.apache.geode.internal.cache.
   * LocalRegion)
   */
  @Override
  public TXRegionState readRegion(InternalRegion r) {
    return this.regions.get(r);
  }

  @Override
  public void rmRegion(LocalRegion r) {
    TXRegionState txr = this.regions.remove(r);
    if (txr != null) {
      txr.cleanup(r);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.TXStateInterface#writeRegion(org.apache.geode.internal.cache.
   * LocalRegion)
   */
  @Override
  public TXRegionState writeRegion(InternalRegion r) {
    TXRegionState result = readRegion(r);
    if (result == null) {
      if (r instanceof BucketRegion) {
        result = new TXBucketRegionState((BucketRegion) r, this);
      } else {
        result = new TXRegionState(r, this);
      }
      this.regions.put(r, result);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("TXState writeRegion flag {} region-state {} ", false,
          result/* , new Throwable() */);
    }
    return result;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getBeginTime()
   */
  @Override
  public long getBeginTime() {
    return this.beginTime;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getChanges()
   */
  @Override
  public int getChanges() {
    int changes = 0;
    Iterator<TXRegionState> it = this.regions.values().iterator();
    while (it.hasNext()) {
      TXRegionState txrs = it.next();
      changes += txrs.getChanges();
    }
    return changes;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#isInProgress()
   */
  @Override
  public boolean isInProgress() {
    return !this.closed;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#nextModSerialNum()
   */
  @Override
  public int nextModSerialNum() {
    this.modSerialNum += 1;
    return this.modSerialNum;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#needsLargeModCount()
   */
  @Override
  public boolean needsLargeModCount() {
    return this.modSerialNum > Byte.MAX_VALUE;
  }

  protected void reserveAndCheck() throws CommitConflictException {
    if (this.closed) {
      return;
    }

    final long conflictStart = statisticsClock.getTime();
    this.locks = createLockRequest();
    this.locks.obtain(getCache().getInternalDistributedSystem());
    // for now check account the dlock service time
    // later this stat end should be moved to a finally block
    if (statisticsClock.isEnabled())
      this.proxy.getTxMgr().getCachePerfStats()
          .incTxConflictCheckTime(statisticsClock.getTime() - conflictStart);
    if (this.internalAfterReservation != null) {
      this.internalAfterReservation.run();
    }
    checkForConflicts();
  }

  byte[] getBaseMembershipId() {
    return this.baseMembershipId;
  }

  long getBaseThreadId() {
    return this.baseThreadId;
  }

  long getBaseSequenceId() {
    return this.baseSequenceId;
  }

  @Override
  public void precommit()
      throws CommitConflictException, UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "precommit"));
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#commit()
   */
  @Override
  public void commit() throws CommitConflictException {
    if (this.closed) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("committing transaction {}", getTransactionId());
    }
    synchronized (this.completionGuard) {
      this.completionStarted = true;
    }

    if (onBehalfOfRemoteStub && !proxy.isCommitOnBehalfOfRemoteStub()) {
      throw new UnsupportedOperationInTransactionException(
          "Cannot commit a transaction being run on behalf of a remote thread");
    }
    cleanupNonDirtyRegions();
    try {
      /*
       * Lock buckets so they can't be rebalanced then perform the conflict check to fix #43489
       */
      try {
        lockBucketRegions();
      } catch (PrimaryBucketException pbe) {
        // not sure what to do here yet
        RuntimeException re = new TransactionDataRebalancedException(
            "Transactional data moved, due to rebalancing.");
        re.initCause(pbe);
        throw re;
      }

      if (this.locks == null) {
        reserveAndCheck();
      }

      // For internal testing
      if (this.internalAfterConflictCheck != null) {
        this.internalAfterConflictCheck.run();
      }

      /*
       * If there is a TransactionWriter plugged in, we need to to give it an opportunity to cleanup
       * the transaction.
       */
      TransactionWriter writer = this.proxy.getTxMgr().getWriter();
      if (!firedWriter && writer != null) {
        try {
          firedWriter = true;
          TXEvent event = getEvent();
          if (!event.hasOnlyInternalEvents()) {
            writer.beforeCommit(event);
          }
        } catch (TransactionWriterException twe) {
          cleanup();
          throw new CommitConflictException(twe);
        } catch (VirtualMachineError err) {
          // cleanup(); this allocates objects so I don't think we can do it - that leaves the TX
          // open, but we are poison pilling so we should be ok??

          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          cleanup(); // rollback the transaction!
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          throw new CommitConflictException(t);
        }
      }

      List/* <TXEntryStateWithRegionAndKey> */ entries = generateEventOffsets();
      TXCommitMessage msg = null;
      try {

        /*
         * In order to preserve data consistency, we need to: 1. Modify the cache first
         * (applyChanges) 2. Ask for advice on who to send to (buildMessage) 3. Send out to other
         * members.
         *
         * If this is done out of order, we will have problems with GII, split brain, and HA.
         */

        attachFilterProfileInformation(entries);

        lockTXRegions(regions);

        try {
          // apply changes to the cache
          applyChanges(entries);
          // For internal testing
          if (this.internalAfterApplyChanges != null) {
            this.internalAfterApplyChanges.run();
          }

          // build and send the message
          msg = buildMessage();
          this.commitMessage = msg;
          if (this.internalBeforeSend != null) {
            this.internalBeforeSend.run();
          }

          msg.send(this.locks.getDistributedLockId());
          // For internal testing
          if (this.internalAfterSend != null) {
            this.internalAfterSend.run();
          }

          firePendingCallbacks();
          /*
           * This is to prepare the commit message for the caller, make sure all events are in
           * there.
           */
          this.commitMessage = buildCompleteMessage();
        } finally {
          unlockTXRegions(regions);
        }
      } finally {
        if (msg != null) {
          msg.releaseViewVersions();
        }
        this.locks.releaseLocal();
        // For internal testing
        if (this.internalAfterReleaseLocalLocks != null) {
          this.internalAfterReleaseLocalLocks.run();
        }
      }
    } finally {
      cleanup();
    }
  }

  private void lockTXRegions(IdentityHashMap<InternalRegion, TXRegionState> regions) {
    Iterator<Map.Entry<InternalRegion, TXRegionState>> it = regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<InternalRegion, TXRegionState> me = it.next();
      InternalRegion r = me.getKey();
      r.getRegionMap().lockRegionForAtomicTX(r);
    }
  }

  private void unlockTXRegions(IdentityHashMap<InternalRegion, TXRegionState> regions) {
    Iterator<Map.Entry<InternalRegion, TXRegionState>> it = regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<InternalRegion, TXRegionState> me = it.next();
      InternalRegion r = me.getKey();
      r.getRegionMap().unlockRegionForAtomicTX(r);
    }
  }

  protected void attachFilterProfileInformation(List entries) {
    {
      Iterator/* <TXEntryStateWithRegionAndKey> */ it = entries.iterator();
      while (it.hasNext()) {
        TXEntryStateWithRegionAndKey o = (TXEntryStateWithRegionAndKey) it.next();
        try {
          if (o.r.isUsedForPartitionedRegionBucket()) {
            BucketRegion bucket = (BucketRegion) o.r;
            /*
             * The event must contain the bucket region
             */
            @Released
            EntryEventImpl ev =
                (EntryEventImpl) o.es.getEvent(o.r, o.key, o.es.getTXRegionState().getTXState());
            try {
              /*
               * The routing information is derived from the PR advisor, not the bucket advisor.
               */
              FilterRoutingInfo fri = bucket.getPartitionedRegion().getRegionAdvisor()
                  .adviseFilterRouting(ev, Collections.EMPTY_SET);
              o.es.setFilterRoutingInfo(fri);
              Set set = bucket.getAdjunctReceivers(ev, Collections.EMPTY_SET, new HashSet(), fri);
              o.es.setAdjunctRecipients(set);
            } finally {
              ev.release();
            }
          }
        } catch (RegionDestroyedException ex) {
          // region was destroyed out from under us; after conflict checking
          // passed. So act as if the region destroy happened right after the
          // commit. We act this way by doing nothing; including distribution
          // of this region's commit data.
        } catch (CancelException ex) {
          // cache was closed out from under us; after conflict checking
          // passed. So do nothing.
        }
      }
    }

  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#rollback()
   */
  @Override
  public void rollback() {
    if (this.closed) {
      return;
    }
    synchronized (this.completionGuard) {
      this.completionStarted = true;
    }
    cleanup();
  }

  /**
   * This is a fix for bug #42228 where a client fails over from one server to another but gets a
   * conflict on completion because completion had already been initiated and had not yet completed
   *
   * @return true if a previous completion was in progress
   */
  public boolean waitForPreviousCompletion() {
    synchronized (this.completionGuard) {// should have already been done, but just to be sure
      if (!this.completionStarted) {
        return false;
      }
      while (this.commitMessage == null && !this.closed) {
        if (logger.isDebugEnabled()) {
          logger.debug("Waiting for previous completion for transaction {}", getTransactionId());
        }
        try {
          this.completionGuard.wait();
        } catch (InterruptedException e) {
          this.proxy.getCache().getCancelCriterion().checkCancelInProgress(e);
          Thread.currentThread().interrupt();
          return true;
        }
      } // while
    }
    return true;
  }

  /**
   * Generate an event id for each operation that will be done by this tx during the application
   * phase of its commit.
   *
   * @return a sorted list of TXEntryStateWithRegionAndKey that will be used to apply the ops on the
   *         nearside in the correct order.
   */
  protected List/* <TXEntryStateWithRegionAndKey> */ generateEventOffsets() {
    this.baseMembershipId = EventID.getMembershipId(this.proxy.getTxMgr().getDM().getSystem());
    this.baseThreadId = EventID.getThreadId();
    this.baseSequenceId = EventID.getSequenceId();

    List/* <TXEntryStateWithRegionAndKey> */ entries = getSortedEntries();
    if (logger.isDebugEnabled()) {
      logger
          .debug("generateEventOffsets() entries " + entries + " RegionState Map=" + this.regions);
    }
    Iterator it = entries.iterator();
    while (it.hasNext()) {
      TXEntryStateWithRegionAndKey o = (TXEntryStateWithRegionAndKey) it.next();
      o.es.generateEventOffsets(this);
    }
    return entries;
  }

  private TXLockRequest createLockRequest() {
    TXLockRequest result = new TXLockRequest();
    Iterator<Map.Entry<InternalRegion, TXRegionState>> it = this.regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<InternalRegion, TXRegionState> me = it.next();
      InternalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      txrs.createLockRequest(r, result);
    }
    return result;
  }

  private void checkForConflicts() throws CommitConflictException, PrimaryBucketException {
    Iterator<Map.Entry<InternalRegion, TXRegionState>> it = this.regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<InternalRegion, TXRegionState> me = it.next();
      InternalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      try {
        txrs.checkForConflicts(r);
      } catch (DiskAccessException dae) {
        r.handleDiskAccessException(dae);
        throw dae;
      }
    }
  }


  protected void lockBucketRegions() throws PrimaryBucketException {

    boolean lockingSucceeded;
    do {
      lockingSucceeded = true;
      Iterator<Map.Entry<InternalRegion, TXRegionState>> it = this.regions.entrySet().iterator();
      Set<BucketRegion> obtained = new HashSet<BucketRegion>();
      while (it.hasNext()) {
        Map.Entry<InternalRegion, TXRegionState> me = it.next();
        InternalRegion r = me.getKey();
        if (r instanceof BucketRegion) {
          if (isDistTx() && !((BucketRegion) r).getBucketAdvisor().isPrimary()) {
            // For distTx we skip for taking locks on secondary.
            continue;
          }
          BucketRegion b = (BucketRegion) r;
          /*
           * Lock the primary bucket so it doesnt get rebalanced until we cleanup!
           */
          boolean lockObtained = false;
          try {
            // use tryLocks to avoid hanging (bug #41708)
            boolean locked = b.doLockForPrimary(true);
            if (locked) {
              obtained.add(b);
              lockObtained = true;
            } else {
              // if we can't get locks then someone has a write-lock. To prevent
              // deadlock (see bug #41708) we release locks and re-acquire them
              r.getCancelCriterion().checkCancelInProgress(null);
              if (logger.isDebugEnabled()) {
                logger.debug("tryLock failed for commit on {}. Releasing locks and retrying",
                    r.getFullPath());
              }
              // release locks and start over
              break;
            }
          } catch (RegionDestroyedException rde) {
            if (logger.isDebugEnabled()) {
              logger.debug("RegionDestroyedException while locking bucket region {}",
                  r.getFullPath(), rde);
            }
            throw new TransactionDataRebalancedException(
                "Bucket rebalanced during commit: " + r.getFullPath());
          } finally {
            if (!lockObtained) {
              // fix for bug #41708 - unlock operation-locks already obtained
              if (logger.isDebugEnabled()) {
                logger.debug("Unexpected exception while locking bucket {}", r.getFullPath());
              }
              for (BucketRegion br : obtained) {
                br.doUnlockForPrimary();
              }
              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              lockingSucceeded = false;
            }
          }
        }
      }
    } while (!lockingSucceeded);
    gotBucketLocks = true;
  }


  protected void cleanupNonDirtyRegions() {
    Iterator<Map.Entry<InternalRegion, TXRegionState>> it = this.regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<InternalRegion, TXRegionState> me = it.next();
      InternalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      txrs.cleanupNonDirtyEntries(r);
    }
  }

  /**
   * this builds a new TXCommitMessage and returns it
   *
   * @return the new message
   */
  protected TXCommitMessage buildMessage() {
    TXCommitMessage msg =
        new TXCommitMessage(this.proxy.getTxId(), this.proxy.getTxMgr().getDM(), this);
    Iterator<Map.Entry<InternalRegion, TXRegionState>> it = this.regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<InternalRegion, TXRegionState> me = it.next();
      InternalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      txrs.buildMessage(r, msg);
    }
    return msg;
  }


  /**
   * this builds a new TXCommitMessage and returns it
   *
   * @return the new message
   */
  protected TXCommitMessage buildCompleteMessage() {
    TXCommitMessage msg =
        new TXCommitMessage(this.proxy.getTxId(), this.proxy.getTxMgr().getDM(), this);
    Iterator<Map.Entry<InternalRegion, TXRegionState>> it = this.regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<InternalRegion, TXRegionState> me = it.next();
      InternalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      txrs.buildCompleteMessage(r, msg);
      // rcl.add(r);
    }
    return msg;
  }

  /**
   * applies this transaction to the cache.
   */
  protected void applyChanges(List/* <TXEntryStateWithRegionAndKey> */ entries) {
    // applyChangesStart for each region
    for (Map.Entry<InternalRegion, TXRegionState> me : this.regions.entrySet()) {
      InternalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      txrs.applyChangesStart(r, this);
    }

    // serializePendingValue for each entry
    for (Object entry : entries) {
      TXEntryStateWithRegionAndKey o = (TXEntryStateWithRegionAndKey) entry;
      o.es.serializePendingValue();
    }

    // applyChanges for each entry
    for (Object entry : entries) {
      TXEntryStateWithRegionAndKey o = (TXEntryStateWithRegionAndKey) entry;
      if (this.internalDuringApplyChanges != null) {
        this.internalDuringApplyChanges.run();
      }
      try {
        o.es.applyChanges(o.r, o.key, this);
      } catch (RegionDestroyedException ex) {
        // region was destroyed out from under us; after conflict checking
        // passed. So act as if the region destroy happened right after the
        // commit. We act this way by doing nothing; including distribution
        // of this region's commit data.
      } catch (CancelException ex) {
        // cache was closed out from under us; after conflict checking
        // passed. So do nothing.
      }
    }

    // applyChangesEnd for each region
    for (Map.Entry<InternalRegion, TXRegionState> me : this.regions.entrySet()) {
      InternalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      txrs.applyChangesEnd(r, this);
    }
  }

  @Override
  public TXEvent getEvent() {
    return new TXEvent(this, getCache());
  }

  /**
   * Note that cleanup does more than is needed in this method. This method only needs to do stuff
   * that is required when a Cache close is done and we have txs that are still in progress.
   * Currently the only thing that is needed is to decrement off-heap refcounts since off-heap
   * memory lives after a cache close.
   */
  @Override
  public void close() {
    if (!this.closed) {
      if (locks != null) {
        cleanup();
        return;
      }
      this.closed = true;
      for (TXRegionState r : this.regions.values()) {
        r.close();
      }
    }
  }

  protected void cleanup() {
    if (singleThreadJTAExecutor.shouldDoCleanup()) {
      singleThreadJTAExecutor.cleanup();
    } else {
      doCleanup();
    }
  }

  void doCleanup() {
    RuntimeException exception = null;
    try {
      this.closed = true;
      this.seenEvents.clear();
      this.seenResults.clear();
      freePendingCallbacks();
      if (this.locks != null) {
        final long conflictStart = statisticsClock.getTime();
        try {
          this.locks.cleanup(getCache().getInternalDistributedSystem());
        } catch (IllegalArgumentException | IllegalMonitorStateException e) {
          exception = e;
        }
        if (statisticsClock.isEnabled())
          this.proxy.getTxMgr().getCachePerfStats()
              .incTxConflictCheckTime(statisticsClock.getTime() - conflictStart);
      }
      Iterator<Map.Entry<InternalRegion, TXRegionState>> it = this.regions.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<InternalRegion, TXRegionState> me = it.next();
        InternalRegion r = me.getKey();
        TXRegionState txrs = me.getValue();
        /*
         * Need to unlock the primary lock for rebalancing so that rebalancing can resume.
         */
        if (gotBucketLocks) {
          if (r instanceof BucketRegion && (((BucketRegion) r).getBucketAdvisor().isPrimary())) {
            try {
              ((BucketRegion) r).doUnlockForPrimary();
            } catch (RegionDestroyedException rde) {
              // ignore
              if (logger.isDebugEnabled()) {
                logger.debug("RegionDestroyedException while unlocking bucket region {}",
                    r.getFullPath(), rde);
              }
            } catch (Exception rde) {
              // ignore
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "Exception while unlocking bucket region {} this is probably because the bucket was destroyed and never locked initially.",
                    r.getFullPath(), rde);
              }
            }
          }
        }
        txrs.cleanup(r);
      }
    } finally {
      synchronized (this.completionGuard) {
        this.completionGuard.notifyAll();
      }

      if (exception != null && !this.proxy.getCache().isClosed()) {
        throw exception;
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getEvents()
   */
  @Override
  public List getEvents() {
    ArrayList events = new ArrayList();
    Iterator<Map.Entry<InternalRegion, TXRegionState>> it = this.regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry me = it.next();
      InternalRegion r = (InternalRegion) me.getKey();
      TXRegionState txrs = (TXRegionState) me.getValue();
      txrs.getEvents(r, events, this);
    }
    if (events.isEmpty()) {
      return Collections.EMPTY_LIST;
    } else {
      Collections.sort(events);
      return Collections.unmodifiableList(events);
    }
  }

  private List/* <TXEntryStateWithRegionAndKey> */ getSortedEntries() {
    ArrayList/* <TXEntryStateWithRegionAndKey> */ entries = new ArrayList();
    Iterator it = this.regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry me = (Map.Entry) it.next();
      InternalRegion r = (InternalRegion) me.getKey();
      TXRegionState txrs = (TXRegionState) me.getValue();
      txrs.getEntries(entries, r);
    }
    if (entries.isEmpty()) {
      return Collections.EMPTY_LIST;
    } else {
      Collections.sort(entries);
      return entries;
    }
  }

  /**
   * Used to keep track of the region and key associated with a TXEntryState. Also used to sort the
   * entries into the order in which they will be applied.
   *
   * @since GemFire 5.7
   */
  static class TXEntryStateWithRegionAndKey implements Comparable {
    public final TXEntryState es;
    public final InternalRegion r;
    public final Object key;

    public TXEntryStateWithRegionAndKey(TXEntryState es, InternalRegion r, Object key) {
      this.es = es;
      this.r = r;
      this.key = key;
    }

    private int getSortValue() {
      return this.es.getSortValue();
    }

    @Override
    public int compareTo(Object o) {
      TXEntryStateWithRegionAndKey other = (TXEntryStateWithRegionAndKey) o;
      return getSortValue() - other.getSortValue();
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || !(o instanceof TXEntryStateWithRegionAndKey))
        return false;
      return compareTo(o) == 0;
    }

    @Override
    public int hashCode() {
      return getSortValue();
    }
  }

  //////////////////////////////////////////////////////////////////
  // JTA Synchronization implementation //
  //////////////////////////////////////////////////////////////////
  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#beforeCompletion()
   */
  @Override
  public synchronized void beforeCompletion() throws SynchronizationCommitConflictException {
    proxy.getTxMgr().setTXState(null);
    if (this.closed) {
      throw new TXManagerCancelledException();
    }

    if (beforeCompletionCalled) {
      // do not re-execute beforeCompletion again
      return;
    }
    beforeCompletionCalled = true;
    singleThreadJTAExecutor.executeBeforeCompletion(this,
        getExecutor(), getCancelCriterion());
  }

  private Executor getExecutor() {
    return getCache().getDistributionManager().getExecutors().getWaitingThreadPool();
  }

  private CancelCriterion getCancelCriterion() {
    return getCache().getCancelCriterion();
  }

  void doBeforeCompletion() {
    final long opStart = statisticsClock.getTime();
    this.jtaLifeTime = opStart - getBeginTime();

    try {
      reserveAndCheck();
      /*
       * If there is a TransactionWriter plugged in, we need to to give it an opportunity to cleanup
       * the transaction.
       */
      TransactionWriter writer = this.proxy.getTxMgr().getWriter();
      if (writer != null) {
        try {
          // need to mark this so we don't fire again in commit
          firedWriter = true;
          TXEvent event = getEvent();
          if (!event.hasOnlyInternalEvents()) {
            writer.beforeCommit(event);
          }
        } catch (TransactionWriterException twe) {
          throw new CommitConflictException(twe);
        } catch (VirtualMachineError err) {
          // cleanup(); this allocates objects so I don't think we can do it - that leaves the TX
          // open, but we are poison pilling so we should be ok??

          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          throw new CommitConflictException(t);
        }
      }
    } catch (CommitConflictException commitConflict) {
      cleanup();
      proxy.getTxMgr().noteCommitFailure(opStart, this.jtaLifeTime, this);
      throw new SynchronizationCommitConflictException(
          String.format("Conflict detected in GemFire transaction %s",
              getTransactionId()),
          commitConflict);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#afterCompletion(int)
   */
  @Override
  public synchronized void afterCompletion(int status) {
    proxy.getTxMgr().setTXState(null);
    // if there was a beforeCompletion call then there will be a thread
    // sitting in the waiting pool to execute afterCompletion. Otherwise
    // throw FailedSynchronizationException().
    if (wasBeforeCompletionCalled()) {
      switch (status) {
        case Status.STATUS_COMMITTED:
          singleThreadJTAExecutor.executeAfterCompletionCommit();
          break;
        case Status.STATUS_ROLLEDBACK:
          singleThreadJTAExecutor.executeAfterCompletionRollback();
          break;
        default:
          throw new TransactionException("Unknown JTA Synchronization status " + status);
      }
    } else {
      // rollback does not run beforeCompletion.
      if (status != Status.STATUS_ROLLEDBACK) {
        throw new FailedSynchronizationException(
            "Could not execute afterCompletion when beforeCompletion was not executed");
      }
      doAfterCompletionRollback();
    }
  }

  void doAfterCompletionCommit() {
    final long opStart = statisticsClock.getTime();
    try {
      Assert.assertTrue(this.locks != null,
          "Gemfire Transaction afterCompletion called with illegal state.");
      try {
        commit();
        saveTXCommitMessageForClientFailover();
      } catch (CommitConflictException error) {
        Assert.assertTrue(false, "Gemfire Transaction " + getTransactionId()
            + " afterCompletion failed.due to CommitConflictException: " + error);
      }
      this.proxy.getTxMgr().noteCommitSuccess(opStart, this.jtaLifeTime, this);
      this.locks = null;

    } catch (InternalGemFireError error) {
      throw new TransactionException(error);
    }
  }

  void doAfterCompletionRollback() {
    final long opStart = statisticsClock.getTime();
    this.jtaLifeTime = opStart - getBeginTime();
    try {
      rollback();
      saveTXCommitMessageForClientFailover();
      this.proxy.getTxMgr().noteRollbackSuccess(opStart, this.jtaLifeTime, this);
    } catch (InternalGemFireError error) {
      throw new TransactionException(error);
    }
  }

  boolean wasBeforeCompletionCalled() {
    return beforeCompletionCalled;
  }

  void saveTXCommitMessageForClientFailover() {
    proxy.getTxMgr().saveTXStateForClientFailover(proxy);
  }


  /**
   * Add an internal callback which is run after the reservation/lock is returned from the Grantor
   * but before the local identity/conflict check. This is the first callback to be called during
   * the commit.
   */
  public void setAfterReservation(Runnable afterReservation) {
    this.internalAfterReservation = afterReservation;
  }

  /**
   * Add an internal callback which is run after the local identity/conflict check has completed but
   * before the changes have been applied to committed state.
   */
  public void setAfterConflictCheck(Runnable afterConflictCheck) {
    this.internalAfterConflictCheck = afterConflictCheck;
  }

  /**
   * Add an internal callback which is run as each transaction change is applied.
   */
  public void setDuringApplyChanges(Runnable duringApplyChanges) {
    this.internalDuringApplyChanges = duringApplyChanges;
  }

  /**
   * Add an internal callback which is run after the transaction changes have been applied to
   * committed state (locally) but before local locks are released (occurs for regions of Local and
   * Distributed No Ack scope).
   */
  public void setAfterApplyChanges(Runnable afterApplyChanges) {
    this.internalAfterApplyChanges = afterApplyChanges;
  }

  /**
   * Add an internal callback which is run after the the local locks are released (which occurs for
   * regions of Local and Distributed No Ack scope) but before commit data is sent to recipients aka
   * Far Siders (only for Distributed Scope regions).
   */
  public void setAfterReleaseLocalLocks(Runnable afterReleaseLocalLocks) {
    this.internalAfterReleaseLocalLocks = afterReleaseLocalLocks;
  }

  /**
   * Add an internal callback which is run once for each recipient (aka Far Sider) of commit data,
   * prior to actually sending the data. This is called prior to calling
   * <code>setAfterIndividualSend</code>.
   */
  public void setDuringIndividualSend(Runnable duringIndividualSend) {
    this.internalDuringIndividualSend = duringIndividualSend;
  }

  /**
   * Add an internal callback which is run once after all the commit data has been sent to each
   * recipient but before the "commit process" message is sent (only sent in the case there regions
   * with Distributed Ack scope)
   */
  public void setAfterIndividualSend(Runnable afterIndividualSend) {
    this.internalAfterIndividualSend = afterIndividualSend;
  }

  /**
   * Add an internal callback which is run once for each recipient (aka Far Sider) of the "commit
   * process" message (only for recipients with Distributed Ack regions), prior to actually sending
   * the message.
   */
  public void setDuringIndividualCommitProcess(Runnable duringIndividualCommitProcess) {
    this.internalDuringIndividualCommitProcess = duringIndividualCommitProcess;
  }

  /**
   * Add an internal callback which is run once after all the "commit process" messages (only for
   * recipients with Distributed Ack regions) have been sent but before <code>setAfterSend</code>
   * callback has been called.
   */
  public void setAfterIndividualCommitProcess(Runnable afterIndividualCommitProcess) {
    this.internalAfterIndividualCommitProcess = afterIndividualCommitProcess;
  }


  /**
   * Add an internal callback which is run after all data has been sent (for Distributed scope
   * regions) and any acknowledgements have been received (for Distributed Ack scope regions) a but
   * before the transaction has been cleaned up.
   */
  public void setAfterSend(Runnable afterSend) {
    this.internalAfterSend = afterSend;
  }

  /**
   * Add an internal callback which is run after the commit message is formed but before it is sent.
   */
  public void setBeforeSend(Runnable r) {
    this.internalBeforeSend = r;
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getCache()
   */
  @Override
  public InternalCache getCache() {
    return this.proxy.getCache();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getRegions()
   */
  @Override
  public Collection<InternalRegion> getRegions() {
    return this.regions.keySet();
  }

  @Override
  public TXRegionState txWriteRegion(final InternalRegion internalRegion, final KeyInfo entryKey) {
    InternalRegion ir = internalRegion.getDataRegionForWrite(entryKey);
    return writeRegion(ir);
  }

  @Override
  public TXRegionState txReadRegion(InternalRegion internalRegion) {
    return readRegion(internalRegion);
  }

  /**
   * @param ifNew only write the entry if it currently does not exist
   * @param requireOldValue if true set the old value in the event, even if ifNew and entry doesn't
   *        currently exist (this is needed for putIfAbsent).
   * @param expectedOldValue the required old value or null
   */
  TXEntryState txWriteEntry(InternalRegion region, EntryEventImpl event, boolean ifNew,
      boolean requireOldValue, Object expectedOldValue) throws EntryNotFoundException {
    boolean createIfAbsent = true;
    if (event.getOperation() == Operation.REPLACE) {
      // replace(K,V) and replace(K,V,V) cannot create an entry
      createIfAbsent = false;
    }
    TXEntryState tx =
        txReadEntry(event.getKeyInfo(), region, true, expectedOldValue, createIfAbsent);
    if (tx != null) {
      if (requireOldValue && tx.existsLocally()) {
        event.setOldValue(tx.getNearSidePendingValue(), true);
      }
      boolean existsLocally = tx.existsLocally();
      if (!existsLocally && event.getOperation() == Operation.REPLACE) {
        throw new EntryNotFoundException("No previously created Entry to be updated");
      }
      if (existsLocally && ifNew) {
        // Since "ifNew" is true then let caller know entry exists
        // in tx state or cmt state
        return ENTRY_EXISTS;
      } else {
        tx.updateForWrite(nextModSerialNum());
      }
    } else {
      if (!createIfAbsent) {
        throw new EntryNotFoundException("No previously created Entry to be updated");
      }
    }
    return tx;
  }

  /**
   * this version of txPutEntry takes a ConcurrentMap expectedOldValue parameter. If not null, this
   * value must match the current value of the entry or false is returned
   */
  @Override
  public boolean txPutEntry(final EntryEventImpl event, boolean ifNew, boolean requireOldValue,
      boolean checkResources, Object expectedOldValue) {

    InternalRegion region = event.getRegion();
    if (checkResources) {
      if (!MemoryThresholds.isLowMemoryExceptionDisabled()) {
        region.checkIfAboveThreshold(event);
      }
    }

    if (bridgeContext == null) {
      bridgeContext = event.getContext();
    }

    if (hasSeenEvent(event)) {
      return getRecordedResult(event);
    }

    // if requireOldValue then oldValue gets set in event
    // (even if ifNew and entry exists)
    // !!!:ezoerner:20080813 need to handle ifOld for transactional on
    // PRs when PRs become transactional
    TXEntryState tx = null;
    boolean result = false;
    try {
      tx = txWriteEntry(region, event, ifNew, requireOldValue, expectedOldValue);
      if (tx == TXState.ENTRY_EXISTS) {
        result = false;
      } else {
        result = tx.basicPut(event, ifNew, isOriginRemoteForEvents());
      }
    } catch (EntryNotFoundException e) {
      result = false;
    } finally {
      recordEventAndResult(event, result);
    }
    return result;
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#containsValueForKey(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion)
   */
  @Override
  public boolean containsValueForKey(KeyInfo keyInfo, LocalRegion region) {
    TXEntryState tx = txReadEntry(keyInfo, region, true, true/* create txEntry is absent */);
    if (tx != null) {
      /*
       * Note that we don't consult this.getDataPolicy().isProxy() when setting this because in this
       * context we don't want proxies to pretend they have a value.
       */
      boolean isProxy = false;
      return tx.isLocallyValid(isProxy);
    } else {
      return region.nonTXContainsValueForKey(keyInfo);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.TXStateInterface#destroyExistingEntry(org.apache.geode.internal
   * .cache.EntryEventImpl, boolean, java.lang.Object)
   */
  @Override
  public void destroyExistingEntry(final EntryEventImpl event, final boolean cacheWrite,
      Object expectedOldValue) {
    if (bridgeContext == null) {
      bridgeContext = event.getContext();
    }
    if (hasSeenEvent(event)) {
      return;
    }
    TXEntryState tx = txWriteExistingEntry(event, expectedOldValue);
    final InternalRegion region = event.getRegion();
    if (tx.destroy(event, cacheWrite, isOriginRemoteForEvents())) {
      Object key = event.getKey();
      LocalRegion rr = region.getDataRegionForRead(event.getKeyInfo());
      txReadRegion(rr).rmEntryUserAttr(key);
      recordEvent(event);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#invalidateExistingEntry(org.apache.geode.
   * internal.cache.EntryEventImpl, boolean, boolean)
   */
  @Override
  public void invalidateExistingEntry(final EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {
    if (bridgeContext == null) {
      bridgeContext = event.getContext();
    }
    if (hasSeenEvent(event)) {
      return;
    }
    TXEntryState tx = txWriteExistingEntry(event, null);
    assert invokeCallbacks && !forceNewEntry;
    tx.invalidate(event);
    recordEvent(event);
  }

  /**
   * Write an existing entry. This form takes an expectedOldValue which, if not null, must be equal
   * to the current value of the entry. If it is not, an EntryNotFoundException is thrown.
   *
   * @return the tx entry object
   */
  private TXEntryState txWriteExistingEntry(final EntryEventImpl event, Object expectedOldValue)
      throws EntryNotFoundException {
    assert !event.isExpiration();
    final Object entryKey = event.getKey();
    final InternalRegion region = event.getRegion();
    final Operation op = event.getOperation();
    TXEntryState tx = txReadEntry(event.getKeyInfo(), region, true, expectedOldValue,
        true/* create txEntry is absent */);
    assert tx != null;
    if (tx.existsLocally()) {
      final boolean invalidatingInvalidEntry =
          op.isInvalidate() && Token.isInvalid(tx.getValueInVM(entryKey));
      // Ignore invalidating an invalid entry
      if (!invalidatingInvalidEntry) {
        tx.updateForWrite(nextModSerialNum());
      }
    } else if (region.isProxy() && !op.isLocal() && !tx.hasOp()) {
      // Distributed operations on proxy regions need to be done
      // even if the entry does not exist locally.
      // But only if we don't already have a tx operation (once we have an op
      // then we honor tx.existsLocally since the tx has storage unlike the proxy).
      // We must not throw EntryNotFoundException in this case
      tx.updateForWrite(nextModSerialNum());
    } else {
      throw new EntryNotFoundException(entryKey.toString());
    }
    return tx;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getEntry(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion)
   */
  @Override
  public Entry getEntry(final KeyInfo keyInfo, final LocalRegion region, boolean allowTombstones) {
    TXEntryState tx = txReadEntry(keyInfo, region, true, true/* create txEntry is absent */);
    if (tx != null && tx.existsLocally()) {
      return new TXEntry(region, keyInfo, getProxy());
    } else {
      return null;
    }
  }

  @Override
  public Entry accessEntry(KeyInfo keyInfo, LocalRegion localRegion) {
    return getEntry(keyInfo, localRegion, false);
  }

  private TXStateInterface getProxy() {
    return this.proxy;
  }

  /**
   * @param rememberRead true if the value read from committed state needs to be remembered in tx
   *        state for repeatable read.
   * @param createIfAbsent should a transactional entry be created if not present.
   * @return a txEntryState or null if the entry doesn't exist in the transaction and/or committed
   *         state.
   */
  @Override
  public TXEntryState txReadEntry(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead,
      boolean createIfAbsent) {
    localRegion.cache.getCancelCriterion().checkCancelInProgress(null);
    return txReadEntry(keyInfo, localRegion, rememberRead, null, createIfAbsent);
  }

  /**
   * This form of txReadEntry takes a concurrent-map argument, expectedOldValue. If this parameter
   * is not null it must match the current value of the entry or an EntryNotFoundException is
   * thrown.
   */
  protected TXEntryState txReadEntry(KeyInfo keyInfo, InternalRegion internalRegion,
      boolean rememberRead, Object expectedOldValue, boolean createIfAbsent)
      throws EntryNotFoundException {
    InternalRegion dataReg = internalRegion.getDataRegionForWrite(keyInfo);
    TXRegionState txr = txReadRegion(dataReg);
    TXEntryState result = null;
    if (txr != null) {
      result = txr.readEntry(keyInfo.getKey());
    }
    if (result == null && rememberRead) {
      // to support repeatable read create an tx entry that reflects current committed state
      if (txr == null) {
        txr = txWriteRegion(internalRegion, keyInfo);
      }
      result = dataReg.createReadEntry(txr, keyInfo, createIfAbsent);
      if (result == null) {
        // createReadEntry will only returns null if createIfAbsent is false.
        // CreateIfAbsent will only be false when this method is called by set operations.
        // In that case we do not want the TXState to have a TXEntryState.
        assert !createIfAbsent;
        return result;
      }
    }

    if (result != null) {
      if (expectedOldValue != null) {
        Object val = result.getNearSidePendingValue();
        if (!AbstractRegionEntry.checkExpectedOldValue(expectedOldValue, val, internalRegion)) {
          throw new EntryNotFoundException(
              "The current value was not equal to expected value.");
        }
      }
    } else {
      /*
       * This means it isn't in the cache and rememberReads = false. This should only happen from
       * test hooks at this point.
       *
       */
      if (txr != null) {
        txr.cleanupNonDirtyEntries(dataReg);
      }
      if (expectedOldValue == null) {
        /*
         * They were expecting non-existence.
         */
        return result;
      } else {
        /*
         * If they pass in null to expectedOldValue, we will have it as Token.INVALID here
         */
        if (!Token.isInvalid(expectedOldValue)) {
          throw new EntryNotFoundException(
              "The current value was not equal to expected value.");
        }
      }
    }
    return result;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getDeserializedValue(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion, boolean)
   */
  @Override
  public Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion, boolean updateStats,
      boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent,
      boolean returnTombstones, boolean retainResult, boolean createIfAbsent) {
    TXEntryState tx = txReadEntry(keyInfo, localRegion, true, createIfAbsent);
    if (tx != null) {
      Object v = tx.getValue(keyInfo, localRegion, preferCD);
      if (!disableCopyOnRead) {
        v = localRegion.conditionalCopy(v);
      }
      return v;
    } else {
      return localRegion.getDeserializedValue(null, keyInfo, updateStats, disableCopyOnRead,
          preferCD, clientEvent, returnTombstones, retainResult);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#getSerializedValue(org.apache.geode.internal.
   * cache.LocalRegion, java.lang.Object, java.lang.Object)
   */
  @Override
  @Retained
  public Object getSerializedValue(LocalRegion localRegion, KeyInfo keyInfo, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws DataLocationException {
    final Object key = keyInfo.getKey();
    TXEntryState tx = txReadEntry(keyInfo, localRegion, true, true/* create txEntry is absent */);
    if (tx != null) {
      Object val = tx.getPendingValue();
      if (val == null || Token.isInvalidOrRemoved(val)) {
        val = findObject(keyInfo, localRegion, val != Token.INVALID, true, val, false, false,
            requestingClient, clientEvent, false);
      }
      return val;
    } else {
      // rememberRead is always true for now,
      // so we should never come here
      assert localRegion instanceof PartitionedRegion;
      PartitionedRegion pr = (PartitionedRegion) localRegion;
      return pr.getDataStore().getSerializedLocally(keyInfo, doNotLockEntry, null, null,
          returnTombstones);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#entryCount(org.apache.geode.internal.cache.
   * LocalRegion)
   */
  @Override
  public int entryCount(LocalRegion localRegion) {
    int result = localRegion.getRegionSize();
    TXRegionState txr = txReadRegion(localRegion);
    if (txr != null) {
      result += txr.entryCountMod();
    }
    if (result > 0) {
      return result;
    } else {
      // This is to work around bug #40946.
      // Other threads can destroy all the keys, and so our entryModCount
      // can bring us below 0
      return 0;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#containsKey(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion)
   */
  @Override
  public boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion) {
    TXEntryState tx = txReadEntry(keyInfo, localRegion, true, true/* create txEntry is absent */);
    if (tx != null) {
      return tx.existsLocally();
    } else {
      return localRegion.nonTXContainsKey(keyInfo);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getValueInVM(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion, boolean)
   */
  @Override
  @Retained
  public Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead) {
    TXEntryState tx =
        txReadEntry(keyInfo, localRegion, rememberRead, true/* create txEntry is absent */);
    if (tx != null) {
      return tx.getValueInVM(keyInfo);
    }
    return localRegion.nonTXbasicGetValueInVM(keyInfo);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#putEntry(org.apache.geode.internal.cache.
   * EntryEventImpl, boolean, boolean, java.lang.Object, boolean, long, boolean)
   */
  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    validateDelta(event);
    return txPutEntry(event, ifNew, requireOldValue, true, expectedOldValue);
  }

  /**
   * throws an exception when cloning is disabled while using delta
   */
  private void validateDelta(EntryEventImpl event) {
    if (event.getDeltaBytes() != null && !event.getRegion().getAttributes().getCloningEnabled()) {
      throw new UnsupportedOperationInTransactionException(
          "Delta without cloning cannot be used in transaction");
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#isStatsDeferred()
   */
  @Override
  public boolean isDeferredStats() {
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.TXStateInterface#findObject(org.apache.geode.internal.cache.
   * LocalRegion, java.lang.Object, java.lang.Object, boolean, boolean, java.lang.Object)
   */
  @Override
  public Object findObject(KeyInfo key, LocalRegion r, boolean isCreate, boolean generateCallbacks,
      Object value, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) {
    return r.findObjectInSystem(key, isCreate, this, generateCallbacks, value, disableCopyOnRead,
        preferCD, requestingClient, clientEvent, returnTombstones);
  }

  private TXEntryState readEntryAndCheckIfDestroyed(KeyInfo keyInfo, LocalRegion localRegion,
      boolean rememberReads, boolean createIfAbsent) {
    TXEntryState txEntryState =
        txReadEntry(keyInfo, localRegion, rememberReads, createIfAbsent);
    if (txEntryState != null) {
      if (!txEntryState.existsLocally()) {
        // It was destroyed by the transaction so skip
        // this key and try the next one
        return null; // fix for bug 34583
      }
    }
    return txEntryState;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.TXStateInterface#getEntryForIterator(org.apache.geode.internal.
   * cache.LocalRegion, java.lang.Object, boolean)
   */
  @Override
  public Object getEntryForIterator(KeyInfo curr, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    if (currRgn instanceof PartitionedRegion) {
      PartitionedRegion pr = (PartitionedRegion) currRgn;
      if (!pr.getBucketPrimary(curr.getBucketId()).equals(pr.cache.getMyId())) {
        // to fix bug 47893 suspend the tx before calling nonTXGetEntry
        final TXManagerImpl txmgr = pr.getGemFireCache().getTXMgr();
        final TXStateProxy tx = txmgr.pauseTransaction();
        try {
          return pr.nonTXGetEntry(curr, false, allowTombstones);
        } finally {
          txmgr.unpauseTransaction(tx);
        }
      }
    }
    TXEntryState txEntryState =
        readEntryAndCheckIfDestroyed(curr, currRgn, rememberReads, allowTombstones);
    if (txEntryState != null) {
      // need to create KeyInfo since higher level iterator may reuse KeyInfo
      return new TXEntry(currRgn,
          new KeyInfo(curr.getKey(), curr.getCallbackArg(), curr.getBucketId()), proxy,
          rememberReads);
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#getKeyForIterator(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion, boolean)
   */
  @Override
  public Object getKeyForIterator(KeyInfo curr, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    assert !(curr.getKey() instanceof RegionEntry);
    TXEntryState txEntryState =
        readEntryAndCheckIfDestroyed(curr, currRgn, rememberReads, allowTombstones);
    if (txEntryState != null) {
      // txEntry is created/read into txState.
      return curr.getKey();
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#getAdditionalKeysForIterator(org.apache.geode.
   * internal.cache.LocalRegion)
   */
  @Override
  public Set getAdditionalKeysForIterator(LocalRegion currRgn) {
    if (currRgn instanceof PartitionedRegion) {
      final HashSet ret = new HashSet();
      for (TXRegionState rs : this.regions.values()) {
        if (rs instanceof TXBucketRegionState) {
          TXBucketRegionState brs = (TXBucketRegionState) rs;
          if (brs.getPartitionedRegion() == currRgn) {
            brs.fillInCreatedEntryKeys(ret);
          }
        }
      }
      return ret;
    } else {
      TXRegionState txr = txReadRegion(currRgn);
      if (txr != null) {
        final HashSet ret = new HashSet();
        txr.fillInCreatedEntryKeys(ret);
        return ret;
      } else {
        return null;
      }
    }
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#isInProgressAndSameAs(org.apache.geode.
   * internal.cache.TXStateInterface)
   */
  @Override
  public boolean isInProgressAndSameAs(TXStateInterface otherState) {
    return isInProgress() && otherState == this;
  }


  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#putEntryOnRemote(org.apache.geode.internal.
   * cache.EntryEventImpl, boolean, boolean, java.lang.Object, boolean, long, boolean)
   */
  @Override
  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws DataLocationException {
    /*
     * Need to flip OriginRemote to true because it is certain that this came from a remote TxStub
     */
    event.setOriginRemote(true);

    return txPutEntry(event, ifNew, requireOldValue, true, expectedOldValue);
  }


  @Override
  public boolean isFireCallbacks() {
    return !getEvent().hasOnlyInternalEvents();
  }

  public boolean isOriginRemoteForEvents() {
    return onBehalfOfRemoteStub || this.proxy.isOnBehalfOfClient();
  }

  @Override
  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws DataLocationException {
    event.setOriginRemote(true);
    destroyExistingEntry(event, cacheWrite, expectedOldValue);
  }

  @Override
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
    event.setOriginRemote(true);
    invalidateExistingEntry(event, invokeCallbacks, forceNewEntry);
  }

  @Override
  public void checkSupportsRegionDestroy() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        "destroyRegion() is not supported while in a transaction");
  }

  @Override
  public void checkSupportsRegionInvalidate() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        "invalidateRegion() is not supported while in a transaction");
  }

  @Override
  public void checkSupportsRegionClear() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        "clear() is not supported while in a transaction");
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#getBucketKeys(org.apache.geode.internal.cache.
   * LocalRegion, int)
   */
  @Override
  public Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones) {
    PartitionedRegion pr = (PartitionedRegion) localRegion;
    return pr.getBucketKeys(bucketId, allowTombstones);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#getEntryOnRemote(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion)
   */
  @Override
  public Entry getEntryOnRemote(KeyInfo key, LocalRegion localRegion, boolean allowTombstones)
      throws DataLocationException {
    PartitionedRegion pr = (PartitionedRegion) localRegion;
    Region.Entry txval = getEntry(key, pr, allowTombstones);
    if (txval == null) {
      throw new EntryNotFoundException(
          "entry not found");
    } else {
      NonLocalRegionEntry nlre = new NonLocalRegionEntry(txval, localRegion);
      LocalRegion dataReg = localRegion.getDataRegionForRead(key);
      return new EntrySnapshot(nlre, dataReg, (LocalRegion) txval.getRegion(), allowTombstones);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#getSemaphore()
   */
  @Override
  public ReentrantLock getLock() {
    return proxy.getLock();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#getRegionKeysForIteration(org.apache.geode.
   * internal.cache.LocalRegion)
   */
  @Override
  public Set getRegionKeysForIteration(LocalRegion currRegion) {
    return currRegion.getRegionKeysForIteration();
  }


  @Override
  public boolean isRealDealLocal() {
    return true;
  }

  @Override
  public InternalDistributedMember getOriginatingMember() {
    // During set operations we need to forward it to other nodes to avoid
    // wrong transaction is masqueraded if the transaction is on behalf of a client.
    // This needs to be set to the clients member id if the client originated the tx.
    return proxy.getOnBehalfOfClientMember();
  }

  @Override
  public boolean isMemberIdForwardingRequired() {
    /*
     * State will never fwd on to other nodes so this is not relevant
     */
    return false;
  }

  @Override
  public TXCommitMessage getCommitMessage() {
    return commitMessage;
  }


  /*
   * For TX this needs to be a PR passed in as region
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#postPutAll(org.apache.geode.internal.cache.
   * DistributedPutAllOperation, java.util.Map, org.apache.geode.internal.cache.LocalRegion)
   */
  @Override
  public void postPutAll(final DistributedPutAllOperation putallOp,
      final VersionedObjectList successfulPuts, InternalRegion reg) {

    final InternalRegion theRegion;
    if (reg instanceof BucketRegion) {
      theRegion = ((BucketRegion) reg).getPartitionedRegion();
    } else {
      theRegion = reg;
    }
    /*
     * Don't fire events here.
     */
    /*
     * We are on the data store, we don't need to do anything here. Commit will push them out.
     */
    /*
     * We need to put this into the tx state.
     */
    theRegion.syncBulkOp(new Runnable() {
      @Override
      public void run() {
        // final boolean requiresRegionContext = theRegion.keyRequiresRegionContext();
        InternalDistributedMember myId =
            theRegion.getDistributionManager().getDistributionManagerId();
        for (int i = 0; i < putallOp.putAllDataSize; ++i) {
          @Released
          EntryEventImpl ev = PutAllPRMessage.getEventFromEntry(theRegion, myId, myId, i,
              putallOp.putAllData, false, putallOp.getBaseEvent().getContext(), false,
              !putallOp.getBaseEvent().isGenerateCallbacks());
          try {
            ev.setPutAllOperation(putallOp);
            if (theRegion.basicPut(ev, false, false, null, false)) {
              successfulPuts.addKeyAndVersion(putallOp.putAllData[i].key, null);
            }
          } finally {
            ev.release();
          }
        }
      }
    }, putallOp.getBaseEvent().getEventId());

  }

  @Override
  public void postRemoveAll(final DistributedRemoveAllOperation op,
      final VersionedObjectList successfulOps, InternalRegion reg) {
    final InternalRegion theRegion;
    if (reg instanceof BucketRegion) {
      theRegion = ((BucketRegion) reg).getPartitionedRegion();
    } else {
      theRegion = reg;
    }
    /*
     * Don't fire events here. We are on the data store, we don't need to do anything here. Commit
     * will push them out. We need to put this into the tx state.
     */
    theRegion.syncBulkOp(new Runnable() {
      @Override
      public void run() {
        InternalDistributedMember myId =
            theRegion.getDistributionManager().getDistributionManagerId();
        for (int i = 0; i < op.removeAllDataSize; ++i) {
          @Released
          EntryEventImpl ev = RemoveAllPRMessage.getEventFromEntry(theRegion, myId, myId, i,
              op.removeAllData, false, op.getBaseEvent().getContext(), false,
              !op.getBaseEvent().isGenerateCallbacks());
          ev.setRemoveAllOperation(op);
          try {
            theRegion.basicDestroy(ev, true/* should we invoke cacheWriter? */, null);
          } catch (EntryNotFoundException ignore) {
          } finally {
            ev.release();
          }
          successfulOps.addKeyAndVersion(op.removeAllData[i].key, null);
        }
      }
    }, op.getBaseEvent().getEventId());

  }

  @Override
  public void suspend() {
    // no special tasks to perform
  }

  @Override
  public void resume() {
    // no special tasks to perform
  }

  @Override
  public void recordTXOperation(ServerRegionDataAccess region, ServerRegionOperation op, Object key,
      Object arguments[]) {
    // no-op here
  }

  @Override
  public void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {
    // Do nothing. Not applicable for transactions.
  }

  @Override
  public boolean isTxState() {
    return true;
  }

  @Override
  public boolean isTxStateStub() {
    return false;
  }

  @Override
  public boolean isTxStateProxy() {
    return false;
  }

  @Override
  public boolean isDistTx() {
    return false;
  }

  @Override
  public boolean isCreatedOnDistTxCoordinator() {
    return false;
  }

  public void setProxyServer(DistributedMember proxyServer) {
    this.proxyServer = proxyServer;
  }

  public DistributedMember getProxyServer() {
    return this.proxyServer;
  }

  boolean isClosed() {
    return closed;
  }
}
