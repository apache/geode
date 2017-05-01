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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.client.internal.ServerRegionDataAccess;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.ClientTXStateStub;
import org.apache.geode.internal.cache.tx.TransactionalOperation.ServerRegionOperation;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

public class TXStateProxyImpl implements TXStateProxy {
  private static final Logger logger = LogService.getLogger();

  protected static final AtomicBoolean txDistributedClientWarningIssued = new AtomicBoolean();

  private boolean isJTA;
  private TXId txId;
  final protected TXManagerImpl txMgr;
  protected DistributedMember target;
  private boolean commitRequestedByOwner;
  private boolean isJCATransaction;

  /**
   * for client/server JTA transactions we need to have a single thread handle both beforeCompletion
   * and afterCompletion so that beforeC can obtain locks for the afterC step. This is that thread
   */
  protected volatile TXSynchronizationRunnable synchRunnable;

  private final ReentrantLock lock = new ReentrantLock();

  /** number of operations in this transaction */
  private int operationCount = 0;

  /**
   * tracks bucketIds of transactional operations so as to distinguish between
   * TransactionDataNotColocated and TransactionDataRebalanced exceptions.
   */
  private Map<Integer, Boolean> buckets = new HashMap<Integer, Boolean>();

  public void setSynchronizationRunnable(TXSynchronizationRunnable synch) {
    this.synchRunnable = synch;
  }

  public TXSynchronizationRunnable getSynchronizationRunnable() {
    return this.synchRunnable;
  }

  public ReentrantLock getLock() {
    return this.lock;
  }

  final boolean isJTA() {
    return isJTA;
  }

  final public TXId getTxId() {
    return txId;
  }

  public final TXManagerImpl getTxMgr() {
    return txMgr;
  }

  protected volatile TXStateInterface realDeal;

  protected boolean inProgress = true;

  protected InternalDistributedMember onBehalfOfClientMember = null;

  /**
   * This returns either the TXState for the current transaction or a proxy for the state if it is
   * held in another member. If no state currently exists, one is created
   * 
   * @param key the key of the entry that is currently being modified
   * @param r the region that is currently being modified
   * @return the state or a proxy for the state
   */
  public TXStateInterface getRealDeal(KeyInfo key, LocalRegion r) {
    if (this.realDeal == null) {
      if (r == null) { // TODO: stop gap to get tests working
        this.realDeal = new TXState(this, false);
      } else {
        // Code to keep going forward
        if (r.hasServerProxy()) {
          this.realDeal = new ClientTXStateStub(this, target, r);
          if (r.scope.isDistributed()) {
            if (txDistributedClientWarningIssued.compareAndSet(false, true)) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.TXStateProxyImpl_Distributed_Region_In_Client_TX,
                  r.getFullPath()));
            }
          }
        } else {
          target = null;
          // wait for the region to be initialized fixes bug 44652
          r.waitOnInitialization(r.initializationLatchBeforeGetInitialImage);
          target = r.getOwnerForKey(key);
          if (target == null || target.equals(this.txMgr.getDM().getId())) {
            this.realDeal = new TXState(this, false);
          } else {
            this.realDeal = new PeerTXStateStub(this, target, onBehalfOfClientMember);
          }
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Built a new TXState: {} me:{}", this.realDeal, this.txMgr.getDM().getId());
      }
    }
    return this.realDeal;
  }

  public TXStateInterface getRealDeal(DistributedMember t) {
    assert t != null;
    if (this.realDeal == null) {
      this.target = t;
      if (target.equals(getCache().getDistributedSystem().getDistributedMember())) {
        this.realDeal = new TXState(this, false);
      } else {
        /*
         * txtodo: // what to do!! We don't know if this is client or server!!!
         */
        this.realDeal = new PeerTXStateStub(this, target, onBehalfOfClientMember);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Built a new TXState: {} me:{}", this.realDeal, this.txMgr.getDM().getId());
      }
    }
    return this.realDeal;
  }

  public TXStateProxyImpl(TXManagerImpl managerImpl, TXId id,
      InternalDistributedMember clientMember) {
    this.txMgr = managerImpl;
    this.txId = id;
    this.isJTA = false;
    this.onBehalfOfClientMember = clientMember;
  }

  public TXStateProxyImpl(TXManagerImpl managerImpl, TXId id, boolean isjta) {
    this.txMgr = managerImpl;
    this.txId = id;
    this.isJTA = isjta;
  }

  protected void setTXIDForReplay(TXId id) {
    this.txId = id;
  }

  public boolean isOnBehalfOfClient() {
    return this.onBehalfOfClientMember != null;
  }

  public void setIsJTA(boolean isJTA) {
    this.isJTA = isJTA;
  }

  public void checkJTA(String errmsg) throws IllegalStateException {
    if (isJTA()) {
      throw new IllegalStateException(errmsg);
    }
  }

  @Override
  public void precommit()
      throws CommitConflictException, UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("precommit"));
  }

  public void commit() throws CommitConflictException {
    boolean preserveTx = false;
    try {
      getRealDeal(null, null).commit();
    } catch (UnsupportedOperationInTransactionException e) {
      // fix for #42490
      preserveTx = true;
      throw e;
    } finally {
      inProgress = preserveTx;
      if (this.synchRunnable != null) {
        this.synchRunnable.abort();
      }
    }
  }

  private TransactionException getTransactionException(KeyInfo keyInfo, GemFireException e) {
    if (isRealDealLocal() && !buckets.isEmpty() && !buckets.containsKey(keyInfo.getBucketId())) {
      TransactionException ex = new TransactionDataNotColocatedException(
          LocalizedStrings.PartitionedRegion_KEY_0_NOT_COLOCATED_WITH_TRANSACTION
              .toLocalizedString(keyInfo.getKey()));
      ex.initCause(e.getCause());
      return ex;
    }
    Throwable ex = e;
    while (ex != null) {
      if (ex instanceof PrimaryBucketException) {
        return new TransactionDataRebalancedException(
            LocalizedStrings.PartitionedRegion_TRANSACTIONAL_DATA_MOVED_DUE_TO_REBALANCING
                .toLocalizedString());
      }
      ex = ex.getCause();
    }
    return (TransactionException) e;
  }

  public boolean containsValueForKey(KeyInfo keyInfo, LocalRegion region) {
    try {
      this.operationCount++;
      boolean retVal = getRealDeal(keyInfo, region).containsValueForKey(keyInfo, region);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(keyInfo, re);
    }
  }

  private void trackBucketForTx(KeyInfo keyInfo) {
    if (keyInfo.getBucketId() >= 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("adding bucket:{} for tx:{}", keyInfo.getBucketId(), getTransactionId());
      }
    }
    if (keyInfo.getBucketId() >= 0) {
      buckets.put(keyInfo.getBucketId(), Boolean.TRUE);
    }
  }

  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws EntryNotFoundException {
    try {
      this.operationCount++;
      getRealDeal(event.getKeyInfo(), event.getLocalRegion()).destroyExistingEntry(event,
          cacheWrite, expectedOldValue);
      trackBucketForTx(event.getKeyInfo());
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(event.getKeyInfo(), re);
    }
  }

  public long getBeginTime() {
    return getRealDeal(null, null).getBeginTime();
  }

  public Cache getCache() {
    return txMgr.getCache();
  }

  public int getChanges() {
    assertBootstrapped();
    return getRealDeal(null, null).getChanges();
  }

  public Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion, boolean updateStats,
      boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent,
      boolean returnTombstones, boolean retainResult) {
    Object val = getRealDeal(keyInfo, localRegion).getDeserializedValue(keyInfo, localRegion,
        updateStats, disableCopyOnRead, preferCD, null, false, retainResult);
    if (val != null) {
      // fixes bug 51057: TXStateStub on client always returns null, so do not increment
      // the operation count it will be incremented in findObject()
      this.operationCount++;
    }
    return val;
  }

  public Entry getEntry(KeyInfo keyInfo, LocalRegion region, boolean allowTombstones) {
    try {
      this.operationCount++;
      Entry retVal = getRealDeal(keyInfo, region).getEntry(keyInfo, region, allowTombstones);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(keyInfo, re);
    }
  }

  public TXEvent getEvent() {
    assertBootstrapped();
    return getRealDeal(null, null).getEvent();
  }

  public List getEvents() {
    assertBootstrapped();
    return getRealDeal(null, null).getEvents();
  }

  public Collection<LocalRegion> getRegions() {
    assertBootstrapped();
    return getRealDeal(null, null).getRegions();
  }

  public TransactionId getTransactionId() {
    return txId;
  }

  public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {
    try {
      this.operationCount++;
      getRealDeal(event.getKeyInfo(), event.getLocalRegion()).invalidateExistingEntry(event,
          invokeCallbacks, forceNewEntry);
      trackBucketForTx(event.getKeyInfo());
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(event.getKeyInfo(), re);
    }
  }

  public boolean isInProgress() {
    return inProgress;
  }

  @Override
  public void setInProgress(boolean progress) {
    this.inProgress = progress;
  }

  public boolean needsLargeModCount() {
    assertBootstrapped();
    return getRealDeal(null, null).needsLargeModCount();
  }

  public int nextModSerialNum() {
    assertBootstrapped();
    return getRealDeal(null, null).nextModSerialNum();
  }

  public TXRegionState readRegion(LocalRegion r) {
    assertBootstrapped();
    return getRealDeal(null, r).readRegion(r);
  }

  public void rmRegion(LocalRegion r) {
    assertBootstrapped();
    getRealDeal(null, r).rmRegion(r);
  }

  public void rollback() {
    try {
      getRealDeal(null, null).rollback();
    } finally {
      inProgress = false;
      if (this.synchRunnable != null) {
        this.synchRunnable.abort();
      }
    }
  }

  public boolean txPutEntry(EntryEventImpl event, boolean ifNew, boolean requireOldValue,
      boolean checkResources, Object expectedOldValue) {
    try {
      this.operationCount++;
      boolean retVal = getRealDeal(event.getKeyInfo(), (LocalRegion) event.getRegion())
          .txPutEntry(event, ifNew, requireOldValue, checkResources, expectedOldValue);
      trackBucketForTx(event.getKeyInfo());
      return retVal;
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(event.getKeyInfo(), re);
    }
  }

  public TXEntryState txReadEntry(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead,
      boolean createTxEntryIfAbsent) {
    try {
      this.operationCount++;
      TXEntryState retVal = getRealDeal(keyInfo, localRegion).txReadEntry(keyInfo, localRegion,
          rememberRead, createTxEntryIfAbsent);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(keyInfo, re);
    }
  }

  public TXRegionState txReadRegion(LocalRegion localRegion) {
    assertBootstrapped();
    return getRealDeal(null, localRegion).txReadRegion(localRegion);
  }

  public TXRegionState txWriteRegion(LocalRegion localRegion, KeyInfo entryKey) {
    return getRealDeal(entryKey, localRegion).txWriteRegion(localRegion, entryKey);
  }

  public TXRegionState writeRegion(LocalRegion r) {
    assertBootstrapped();
    return getRealDeal(null, r).writeRegion(r);
  }

  private void assertBootstrapped() {
    assert realDeal != null;
  }

  public void afterCompletion(int status) {
    assertBootstrapped();
    try {
      getRealDeal(null, null).afterCompletion(status);
    } finally {
      this.inProgress = false;
      if (this.synchRunnable != null) {
        this.synchRunnable.abort();
      }
    }
  }

  public void beforeCompletion() {
    assertBootstrapped();
    getRealDeal(null, null).beforeCompletion();
  }

  public boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion) {
    try {
      this.operationCount++;
      boolean retVal = getRealDeal(keyInfo, localRegion).containsKey(keyInfo, localRegion);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(keyInfo, re);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UL_UNRELEASED_LOCK",
      justification = "This method unlocks and then conditionally undoes the unlock in the finally-block. Review again at later time.")
  public int entryCount(LocalRegion localRegion) {
    // if size is the first operation in the transaction, then reset the txState
    boolean resetTXState = this.realDeal == null;
    TXStateProxy txp = null;
    boolean txUnlocked = false;
    if (resetTXState) {
      txp = getTxMgr().internalSuspend();
    } else {
      if (getLock().isHeldByCurrentThread()) {
        txUnlocked = true; // bug #42945 - hang trying to compute size for PR
        getLock().unlock();
      }
    }
    try {
      if (resetTXState) {
        return localRegion.getSharedDataView().entryCount(localRegion);
      }
      return getRealDeal(null, localRegion).entryCount(localRegion);
    } finally {
      if (resetTXState) {
        getTxMgr().internalResume(txp);
      } else if (txUnlocked) {
        getLock().lock();
      }
    }
  }

  public Object findObject(KeyInfo key, LocalRegion r, boolean isCreate, boolean generateCallbacks,
      Object value, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) {
    try {
      this.operationCount++;
      Object retVal = getRealDeal(key, r).findObject(key, r, isCreate, generateCallbacks, value,
          disableCopyOnRead, preferCD, requestingClient, clientEvent, false);
      trackBucketForTx(key);
      return retVal;
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(key, re);
    }
  }

  public Set getAdditionalKeysForIterator(LocalRegion currRgn) {
    if (this.realDeal == null) {
      return null;
    }
    return getRealDeal(null, currRgn).getAdditionalKeysForIterator(currRgn);
  }

  public Object getEntryForIterator(KeyInfo key, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    boolean resetTxState = this.realDeal == null;
    TXStateProxy txp = null;
    if (resetTxState) {
      txp = getTxMgr().internalSuspend();
    }
    try {
      if (resetTxState) {
        return currRgn.getSharedDataView().getEntry(key, currRgn, allowTombstones);
      }
      return getRealDeal(key, currRgn).getEntryForIterator(key, currRgn, rememberReads,
          allowTombstones);
    } finally {
      if (resetTxState) {
        getTxMgr().internalResume(txp);
      }
    }
  }

  public Object getKeyForIterator(KeyInfo keyInfo, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    boolean resetTxState = this.realDeal == null;
    TXStateProxy txp = null;
    if (resetTxState) {
      txp = getTxMgr().internalSuspend();
    }
    try {
      if (resetTxState) {
        return currRgn.getSharedDataView().getKeyForIterator(keyInfo, currRgn, rememberReads,
            allowTombstones);
      }
      return getRealDeal(keyInfo, currRgn).getKeyForIterator(keyInfo, currRgn, rememberReads,
          allowTombstones);
    } finally {
      if (resetTxState) {
        getTxMgr().internalResume(txp);
      }
    }
  }

  public Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead) {
    this.operationCount++;
    return getRealDeal(keyInfo, localRegion).getValueInVM(keyInfo, localRegion, rememberRead);
  }

  public boolean isDeferredStats() {
    assertBootstrapped();
    return getRealDeal(null, null).isDeferredStats();
  }

  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    try {
      this.operationCount++;
      boolean retVal = getRealDeal(event.getKeyInfo(), event.getLocalRegion()).putEntry(event,
          ifNew, ifOld, expectedOldValue, requireOldValue, lastModified, overwriteDestroyed);
      trackBucketForTx(event.getKeyInfo());
      return retVal;
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(event.getKeyInfo(), re);
    }
  }

  public boolean isInProgressAndSameAs(TXStateInterface otherState) {
    return isInProgress() && otherState == this;
  }

  public void setLocalTXState(TXStateInterface state) {
    this.realDeal = state;
  }

  public Object getSerializedValue(LocalRegion localRegion, KeyInfo key, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws DataLocationException {
    this.operationCount++;
    return getRealDeal(key, localRegion).getSerializedValue(localRegion, key, doNotLockEntry,
        requestingClient, clientEvent, returnTombstones);
  }

  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws DataLocationException {
    this.operationCount++;
    TXStateInterface tx = getRealDeal(event.getKeyInfo(), event.getLocalRegion());
    assert (tx instanceof TXState) : tx.getClass().getSimpleName();
    return tx.putEntryOnRemote(event, ifNew, ifOld, expectedOldValue, requireOldValue, lastModified,
        overwriteDestroyed);
  }

  public boolean isFireCallbacks() {
    return getRealDeal(null, null).isFireCallbacks();
  }

  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws DataLocationException {
    this.operationCount++;
    TXStateInterface tx = getRealDeal(event.getKeyInfo(), event.getLocalRegion());
    assert (tx instanceof TXState);
    tx.destroyOnRemote(event, cacheWrite, expectedOldValue);
  }

  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
    this.operationCount++;
    TXStateInterface tx = getRealDeal(event.getKeyInfo(), event.getLocalRegion());
    assert (tx instanceof TXState);
    tx.invalidateOnRemote(event, invokeCallbacks, forceNewEntry);
  }

  public void checkSupportsRegionDestroy() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.TXState_REGION_DESTROY_NOT_SUPPORTED_IN_A_TRANSACTION.toLocalizedString());
  }

  public void checkSupportsRegionInvalidate() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.TXState_REGION_INVALIDATE_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString());
  }

  @Override
  public void checkSupportsRegionClear() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.TXState_REGION_CLEAR_NOT_SUPPORTED_IN_A_TRANSACTION.toLocalizedString());
  }

  public Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones) {
    // if this the first operation in a transaction, reset txState
    boolean resetTxState = this.realDeal == null;
    TXStateProxy txp = null;
    if (resetTxState) {
      txp = getTxMgr().internalSuspend();
    }
    try {
      if (resetTxState) {
        return localRegion.getSharedDataView().getBucketKeys(localRegion, bucketId, false);
      }
      return getRealDeal(null, localRegion).getBucketKeys(localRegion, bucketId, false);
    } finally {
      if (resetTxState) {
        getTxMgr().internalResume(txp);
      }
    }
  }

  public Entry getEntryOnRemote(KeyInfo keyInfo, LocalRegion localRegion, boolean allowTombstones)
      throws DataLocationException {
    this.operationCount++;
    TXStateInterface tx = getRealDeal(keyInfo, localRegion);
    assert (tx instanceof TXState);
    return tx.getEntryOnRemote(keyInfo, localRegion, allowTombstones);
  }

  public void forceLocalBootstrap() {
    getRealDeal(null, null);
  }

  public DistributedMember getTarget() {
    return this.target;
  }

  public void setTarget(DistributedMember target) {
    assert this.target == null;
    getRealDeal(target);
  }

  public Collection<?> getRegionKeysForIteration(LocalRegion currRegion) {
    if (currRegion.isUsedForPartitionedRegionBucket()) {
      return currRegion.getRegionKeysForIteration();
    } else {
      return getRealDeal(null, currRegion).getRegionKeysForIteration(currRegion);
    }
  }

  public boolean isCommitOnBehalfOfRemoteStub() {
    return this.commitRequestedByOwner;
  }

  public boolean setCommitOnBehalfOfRemoteStub(boolean requestedByOwner) {
    return this.commitRequestedByOwner = requestedByOwner;
  }

  public boolean isRealDealLocal() {
    if (this.realDeal != null) {
      return this.realDeal.isRealDealLocal();
    } else {
      // no real deal
      return false;
    }
  }

  /** if there is local txstate, return it */
  public TXState getLocalRealDeal() {
    if (this.realDeal != null) {
      if (this.realDeal.isRealDealLocal()) {
        return (TXState) this.realDeal;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TXStateProxyImpl@").append(System.identityHashCode(this)).append(" txId:")
        .append(this.txId).append(" realDeal:").append(this.realDeal).append(" isJTA:")
        .append(isJTA);
    return builder.toString();
  }

  public InternalDistributedMember getOriginatingMember() {
    if (this.realDeal == null) {
      return null;
    } else {
      return this.realDeal.getOriginatingMember();
    }
  }

  public boolean isMemberIdForwardingRequired() {
    if (this.realDeal == null) {
      return false;
    } else {
      return this.realDeal.isMemberIdForwardingRequired();
    }
  }

  public TXCommitMessage getCommitMessage() {
    if (this.realDeal == null) {
      return null;
    } else {
      return this.realDeal.getCommitMessage();
    }
  }

  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      LocalRegion region) {
    if (putallOp.putAllData.length == 0) {
      return;
    }
    region.getCancelCriterion().checkCancelInProgress(null); // fix for bug #43651
    Object key = null;
    if (putallOp.putAllData[0] != null) {
      key = putallOp.putAllData[0].key;
    }
    KeyInfo ki = new KeyInfo(key, null, null);
    TXStateInterface tsi = getRealDeal(ki, region);
    tsi.postPutAll(putallOp, successfulPuts, region);
  }

  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      LocalRegion region) {
    if (op.removeAllData.length == 0) {
      return;
    }
    region.getCancelCriterion().checkCancelInProgress(null); // fix for bug #43651
    Object key = null;
    if (op.removeAllData[0] != null) {
      key = op.removeAllData[0].key;
    }
    KeyInfo ki = new KeyInfo(key, null, null);
    TXStateInterface tsi = getRealDeal(ki, region);
    tsi.postRemoveAll(op, successfulOps, region);
  }

  public boolean isJCATransaction() {
    return this.isJCATransaction;
  }


  public void setJCATransaction() {
    this.isJCATransaction = true;
  }

  public Entry accessEntry(KeyInfo keyInfo, LocalRegion region) {
    try {
      this.operationCount++;
      Entry retVal = getRealDeal(keyInfo, region).accessEntry(keyInfo, region);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException | PrimaryBucketException re) {
      throw getTransactionException(keyInfo, re);
    }
  }

  public void suspend() {
    if (this.realDeal != null) {
      getRealDeal(null, null).suspend();
    }
  }

  public void resume() {
    if (this.realDeal != null) {
      getRealDeal(null, null).resume();
    }
  }

  /** test hook - record a list of ops in the transaction */
  public void recordTXOperation(ServerRegionDataAccess region, ServerRegionOperation op, Object key,
      Object arguments[]) {
    if (ClientTXStateStub.transactionRecordingEnabled()) {
      getRealDeal(null, (LocalRegion) region.getRegion()).recordTXOperation(region, op, key,
          arguments);
    }
  }

  @Override
  public int operationCount() {
    return this.operationCount;
  }

  /**
   * increments the operation count by 1
   */
  public void incOperationCount() {
    this.operationCount++;
  }

  @Override
  public void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {
    // Do nothing. Not applicable for transactions.
  }

  public void close() {
    if (this.realDeal != null) {
      this.realDeal.close();
    }
  }

  @Override
  public boolean isTxState() {
    return false;
  }

  @Override
  public boolean isTxStateStub() {
    return false;
  }

  @Override
  public boolean isTxStateProxy() {
    return true;
  }

  @Override
  public boolean isDistTx() {
    return false;
  }

  @Override
  public boolean isCreatedOnDistTxCoordinator() {
    return false;
  }

  @Override
  public void updateProxyServer(InternalDistributedMember proxy) {
    // only update in TXState if it has one
    if (this.realDeal != null && this.realDeal.isRealDealLocal() && isOnBehalfOfClient()) {
      ((TXState) this.realDeal).setProxyServer(proxy);
    }
  }
}
