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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireException;
import org.apache.geode.annotations.internal.MakeNotStatic;
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
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class TXStateProxyImpl implements TXStateProxy {
  private static final Logger logger = LogService.getLogger();

  @MakeNotStatic
  protected static final AtomicBoolean txDistributedClientWarningIssued = new AtomicBoolean();

  private boolean isJTA;
  private TXId txId;
  protected final TXManagerImpl txMgr;
  protected DistributedMember target;
  private boolean commitRequestedByOwner;
  private boolean isJCATransaction;

  private final ReentrantLock lock = new ReentrantLock();

  /** number of operations in this transaction */
  private int operationCount = 0;

  /**
   * tracks bucketIds of transactional operations so as to distinguish between
   * TransactionDataNotColocated and TransactionDataRebalanced exceptions.
   */
  private final Map<Integer, Boolean> buckets = new HashMap<Integer, Boolean>();

  private boolean firstOperationOnPartitionedRegion = false;

  protected volatile TXStateInterface realDeal;

  protected boolean inProgress = true;

  protected InternalDistributedMember onBehalfOfClientMember = null;

  private final InternalCache cache;
  private long lastOperationTimeFromClient;
  private final StatisticsClock statisticsClock;

  private boolean removedCausedByFailover = false;

  public TXStateProxyImpl(InternalCache cache, TXManagerImpl managerImpl, TXId id,
      InternalDistributedMember clientMember, StatisticsClock statisticsClock) {
    this.cache = cache;
    txMgr = managerImpl;
    txId = id;
    isJTA = false;
    onBehalfOfClientMember = clientMember;
    this.statisticsClock = statisticsClock;
  }

  public TXStateProxyImpl(InternalCache cache, TXManagerImpl managerImpl, TXId id, boolean isjta,
      StatisticsClock statisticsClock) {
    this.cache = cache;
    txMgr = managerImpl;
    txId = id;
    isJTA = isjta;
    this.statisticsClock = statisticsClock;
  }

  @Override
  public ReentrantLock getLock() {
    return lock;
  }

  protected StatisticsClock getStatisticsClock() {
    return statisticsClock;
  }

  boolean isJTA() {
    return isJTA;
  }

  @Override
  public TXId getTxId() {
    return txId;
  }

  @Override
  public TXManagerImpl getTxMgr() {
    return txMgr;
  }

  /**
   * This returns either the TXState for the current transaction or a proxy for the state if it is
   * held in another member. If no state currently exists, one is created
   *
   * @param key the key of the entry that is currently being modified
   * @param r the region that is currently being modified
   * @return the state or a proxy for the state
   */
  public TXStateInterface getRealDeal(KeyInfo key, InternalRegion r) {
    if (realDeal == null) {
      if (r == null) { // TODO: stop gap to get tests working
        realDeal = new TXState(this, false, statisticsClock);
      } else {
        // Code to keep going forward
        if (r.hasServerProxy()) {
          realDeal =
              new ClientTXStateStub(r.getCache(), r.getDistributionManager(), this, target, r);
          if (r.getScope().isDistributed()) {
            if (txDistributedClientWarningIssued.compareAndSet(false, true)) {
              logger.warn(
                  "Distributed region {} is being used in a client-initiated transaction.  The transaction will only affect servers and this client.  To keep from seeing this message use 'local' scope in client regions used in transactions.",
                  r.getFullPath());
            }
          }
        } else {
          target = null;
          // wait for the region to be initialized fixes bug 44652
          r.waitOnInitialization(r.getInitializationLatchBeforeGetInitialImage());
          target = r.getOwnerForKey(key);
          if (target == null || target.equals(txMgr.getDM().getId())) {
            realDeal = new TXState(this, false, statisticsClock);
          } else {
            realDeal = new PeerTXStateStub(this, target, onBehalfOfClientMember);
          }
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Built a new TXState: {} me:{}", realDeal, txMgr.getDM().getId());
      }
    }
    if (isRealDealLocal() && !((TXState) realDeal).hasPerformedAnyOperation()) {
      if (r != null && (r instanceof PartitionedRegion || r.isUsedForPartitionedRegionBucket())) {
        firstOperationOnPartitionedRegion = true;
      }
    }
    return realDeal;
  }

  public TXStateInterface getRealDeal(DistributedMember t) {
    assert t != null;
    if (realDeal == null) {
      target = t;
      if (target.equals(getCache().getDistributedSystem().getDistributedMember())) {
        realDeal = new TXState(this, false, statisticsClock);
      } else {
        /*
         * txtodo: // what to do!! We don't know if this is client or server!!!
         */
        realDeal = new PeerTXStateStub(this, target, onBehalfOfClientMember);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Built a new TXState: {} me:{}", realDeal, txMgr.getDM().getId());
      }
    }
    return realDeal;
  }

  protected void setTXIDForReplay(TXId id) {
    txId = id;
  }

  @Override
  public boolean isOnBehalfOfClient() {
    return onBehalfOfClientMember != null;
  }

  @Override
  public void setIsJTA(boolean isJTA) {
    this.isJTA = isJTA;
  }

  @Override
  public void checkJTA(String errmsg) throws IllegalStateException {
    if (isJTA()) {
      throw new IllegalStateException(errmsg);
    }
  }

  boolean isRemovedCausedByFailover() {
    return removedCausedByFailover;
  }

  void setRemovedCausedByFailover(boolean removedCausedByFailover) {
    this.removedCausedByFailover = removedCausedByFailover;
  }

  @Override
  public void precommit()
      throws CommitConflictException, UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "precommit"));
  }

  @Override
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
    }
  }

  TransactionException getTransactionException(KeyInfo keyInfo, GemFireException e) {
    if (isRealDealLocal() && !buckets.isEmpty() && !buckets.containsKey(keyInfo.getBucketId())) {
      TransactionException ex = new TransactionDataNotColocatedException(
          String.format("Key %s is not colocated with transaction",
              keyInfo.getKey()),
          e.getCause());
      return ex;
    }
    Throwable ex = e;
    while (ex != null) {
      if (ex instanceof PrimaryBucketException) {
        if (isRealDealLocal() && !firstOperationOnPartitionedRegion) {
          return new TransactionDataNotColocatedException(
              String.format(
                  "Key %s is not colocated with transaction. First operation in a transaction "
                      + "should be on a partitioned region when there are operations on both "
                      + "partitioned regions and replicate regions.",
                  keyInfo.getKey()));
        }
        return new TransactionDataRebalancedException(
            "Transactional data moved, due to rebalancing.");
      }
      ex = ex.getCause();
    }
    return (TransactionException) e;
  }

  @Override
  public boolean containsValueForKey(KeyInfo keyInfo, LocalRegion region) {
    try {
      operationCount++;
      boolean retVal = getRealDeal(keyInfo, region).containsValueForKey(keyInfo, region);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(keyInfo, transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(keyInfo, primaryBucketException);
    }
  }

  private TransactionException handleTransactionDataRebalancedException(KeyInfo keyInfo,
      TransactionDataRebalancedException transactionDataRebalancedException) {
    if (isRealDealLocal()) {
      return getTransactionException(keyInfo, transactionDataRebalancedException);
    }
    return transactionDataRebalancedException;
  }

  void trackBucketForTx(KeyInfo keyInfo) {
    if (keyInfo.getBucketId() >= 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("adding bucket:{} for tx:{}", keyInfo.getBucketId(), getTransactionId());
      }
    }
    if (keyInfo.getBucketId() >= 0) {
      buckets.put(keyInfo.getBucketId(), Boolean.TRUE);
    }
  }

  @Override
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws EntryNotFoundException {
    try {
      operationCount++;
      getRealDeal(event.getKeyInfo(), event.getRegion()).destroyExistingEntry(event, cacheWrite,
          expectedOldValue);
      trackBucketForTx(event.getKeyInfo());
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(event.getKeyInfo(),
          transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(event.getKeyInfo(), primaryBucketException);
    }
  }

  @Override
  public long getBeginTime() {
    return getRealDeal(null, null).getBeginTime();
  }

  @Override
  public InternalCache getCache() {
    return cache;
  }

  @Override
  public int getChanges() {
    assertBootstrapped();
    return getRealDeal(null, null).getChanges();
  }

  @Override
  public Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion, boolean updateStats,
      boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent,
      boolean returnTombstones, boolean retainResult, boolean createIfAbsent) {
    try {
      Object val = getRealDeal(keyInfo, localRegion).getDeserializedValue(keyInfo, localRegion,
          updateStats, disableCopyOnRead, preferCD, null, false, retainResult, createIfAbsent);
      trackBucketForTx(keyInfo);
      if (val != null) {
        // fixes bug 51057: TXStateStub on client always returns null, so do not increment
        // the operation count it will be incremented in findObject()
        operationCount++;
      }
      return val;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(keyInfo, transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(keyInfo, primaryBucketException);
    }
  }

  @Override
  public Entry getEntry(KeyInfo keyInfo, LocalRegion region, boolean allowTombstones) {
    try {
      operationCount++;
      Entry retVal = getRealDeal(keyInfo, region).getEntry(keyInfo, region, allowTombstones);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(keyInfo, transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(keyInfo, primaryBucketException);
    }
  }

  @Override
  public TXEvent getEvent() {
    assertBootstrapped();
    return getRealDeal(null, null).getEvent();
  }

  @Override
  public List getEvents() {
    assertBootstrapped();
    return getRealDeal(null, null).getEvents();
  }

  @Override
  public Collection<InternalRegion> getRegions() {
    assertBootstrapped();
    return getRealDeal(null, null).getRegions();
  }

  @Override
  public TransactionId getTransactionId() {
    return txId;
  }

  @Override
  public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {
    try {
      operationCount++;
      getRealDeal(event.getKeyInfo(), event.getRegion()).invalidateExistingEntry(event,
          invokeCallbacks, forceNewEntry);
      trackBucketForTx(event.getKeyInfo());
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(event.getKeyInfo(),
          transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(event.getKeyInfo(), primaryBucketException);
    }
  }

  @Override
  public boolean isInProgress() {
    return inProgress;
  }

  @Override
  public void setInProgress(boolean progress) {
    inProgress = progress;
  }

  @Override
  public boolean needsLargeModCount() {
    assertBootstrapped();
    return getRealDeal(null, null).needsLargeModCount();
  }

  @Override
  public int nextModSerialNum() {
    assertBootstrapped();
    return getRealDeal(null, null).nextModSerialNum();
  }

  @Override
  public TXRegionState readRegion(InternalRegion r) {
    assertBootstrapped();
    return getRealDeal(null, r).readRegion(r);
  }

  @Override
  public void rmRegion(LocalRegion r) {
    assertBootstrapped();
    getRealDeal(null, r).rmRegion(r);
  }

  @Override
  public void rollback() {
    try {
      getRealDeal(null, null).rollback();
    } finally {
      inProgress = false;
    }
  }

  @Override
  public boolean txPutEntry(EntryEventImpl event, boolean ifNew, boolean requireOldValue,
      boolean checkResources, Object expectedOldValue) {
    try {
      operationCount++;
      boolean retVal = getRealDeal(event.getKeyInfo(), event.getRegion())
          .txPutEntry(event, ifNew, requireOldValue, checkResources, expectedOldValue);
      trackBucketForTx(event.getKeyInfo());
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(event.getKeyInfo(),
          transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(event.getKeyInfo(), primaryBucketException);
    }
  }

  @Override
  public TXEntryState txReadEntry(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead,
      boolean createTxEntryIfAbsent) {
    try {
      operationCount++;
      TXEntryState retVal = getRealDeal(keyInfo, localRegion).txReadEntry(keyInfo, localRegion,
          rememberRead, createTxEntryIfAbsent);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(keyInfo, transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(keyInfo, primaryBucketException);
    }
  }

  @Override
  public TXRegionState txReadRegion(InternalRegion internalRegion) {
    assertBootstrapped();
    return getRealDeal(null, internalRegion).txReadRegion(internalRegion);
  }

  @Override
  public TXRegionState txWriteRegion(InternalRegion internalRegion, KeyInfo entryKey) {
    return getRealDeal(entryKey, internalRegion).txWriteRegion(internalRegion, entryKey);
  }

  @Override
  public TXRegionState writeRegion(InternalRegion r) {
    assertBootstrapped();
    return getRealDeal(null, r).writeRegion(r);
  }

  private void assertBootstrapped() {
    assert realDeal != null;
  }

  @Override
  public void afterCompletion(int status) {
    assertBootstrapped();
    try {
      getRealDeal(null, null).afterCompletion(status);
    } finally {
      inProgress = false;
    }
  }

  @Override
  public void beforeCompletion() {
    assertBootstrapped();
    getRealDeal(null, null).beforeCompletion();
  }

  @Override
  public boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion) {
    try {
      operationCount++;
      boolean retVal = getRealDeal(keyInfo, localRegion).containsKey(keyInfo, localRegion);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(keyInfo, transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(keyInfo, primaryBucketException);
    }
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UL_UNRELEASED_LOCK",
      justification = "This method unlocks and then conditionally undoes the unlock in the finally-block. Review again at later time.")
  public int entryCount(LocalRegion localRegion) {
    // if size is the first operation in the transaction, then reset the txState
    boolean resetTXState = realDeal == null;
    TXStateProxy txp = null;
    boolean txUnlocked = false;
    if (resetTXState) {
      txp = getTxMgr().pauseTransaction();
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
        getTxMgr().unpauseTransaction(txp);
      } else if (txUnlocked) {
        getLock().lock();
      }
    }
  }

  @Override
  public Object findObject(KeyInfo key, LocalRegion r, boolean isCreate, boolean generateCallbacks,
      Object value, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) {
    try {
      operationCount++;
      Object retVal = getRealDeal(key, r).findObject(key, r, isCreate, generateCallbacks, value,
          disableCopyOnRead, preferCD, requestingClient, clientEvent, false);
      trackBucketForTx(key);
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(key, transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(key, primaryBucketException);
    }
  }

  @Override
  public Set getAdditionalKeysForIterator(LocalRegion currRgn) {
    if (realDeal == null) {
      return null;
    }
    return getRealDeal(null, currRgn).getAdditionalKeysForIterator(currRgn);
  }

  protected final boolean restoreSetOperationTransactionBehavior =
      SystemPropertyHelper.restoreSetOperationTransactionBehavior();

  @Override
  public Object getEntryForIterator(KeyInfo key, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    boolean resetTxState = isTransactionInternalSuspendNeeded(currRgn);
    TXStateProxy txp = null;
    if (resetTxState) {
      txp = getTxMgr().pauseTransaction();
    }
    try {
      if (resetTxState) {
        return currRgn.getSharedDataView().getEntry(key, currRgn, allowTombstones);
      }
      return getRealDeal(key, currRgn).getEntryForIterator(key, currRgn, rememberReads,
          allowTombstones);
    } finally {
      if (resetTxState) {
        getTxMgr().unpauseTransaction(txp);
      }
    }
  }

  private boolean isTransactionInternalSuspendNeeded(LocalRegion region) {
    // for peer accessor, do not bootstrap transaction in the node as subsequent operations
    // will fail as transaction should be on data node only
    boolean resetTxState =
        realDeal == null && (isPeerAccessor(region) || restoreSetOperationTransactionBehavior);
    return resetTxState;
  }

  private boolean isPeerAccessor(LocalRegion region) {
    if (region.hasServerProxy()) {
      return false;
    }
    return !region.canStoreDataLocally();
  }

  @Override
  public Object getKeyForIterator(KeyInfo keyInfo, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    boolean resetTxState = isTransactionInternalSuspendNeeded(currRgn);
    TXStateProxy txp = null;
    if (resetTxState) {
      txp = getTxMgr().pauseTransaction();
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
        getTxMgr().unpauseTransaction(txp);
      }
    }
  }

  @Override
  public Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead) {
    operationCount++;
    return getRealDeal(keyInfo, localRegion).getValueInVM(keyInfo, localRegion, rememberRead);
  }

  @Override
  public boolean isDeferredStats() {
    assertBootstrapped();
    return getRealDeal(null, null).isDeferredStats();
  }

  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    return putEntry(event, ifNew, ifOld, expectedOldValue, requireOldValue, lastModified,
        overwriteDestroyed, true, false);
  }

  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed, boolean invokeCallbacks, boolean throwConcurrentModification) {
    try {
      operationCount++;
      boolean retVal = getRealDeal(event.getKeyInfo(), event.getRegion()).putEntry(event, ifNew,
          ifOld, expectedOldValue, requireOldValue, lastModified, overwriteDestroyed);
      trackBucketForTx(event.getKeyInfo());
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(event.getKeyInfo(),
          transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(event.getKeyInfo(), primaryBucketException);
    }
  }

  @Override
  public boolean isInProgressAndSameAs(TXStateInterface otherState) {
    return isInProgress() && otherState == this;
  }

  @Override
  public void setLocalTXState(TXStateInterface state) {
    realDeal = state;
  }

  @Override
  public Object getSerializedValue(LocalRegion localRegion, KeyInfo key, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws DataLocationException {
    operationCount++;
    try {
      Object retVal =
          getRealDeal(key, localRegion).getSerializedValue(localRegion, key, doNotLockEntry,
              requestingClient, clientEvent, returnTombstones);
      trackBucketForTx(key);
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(key, transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(key, primaryBucketException);
    }
  }

  @Override
  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws DataLocationException {
    operationCount++;
    TXStateInterface tx = getRealDeal(event.getKeyInfo(), event.getRegion());
    assert (tx instanceof TXState) : tx.getClass().getSimpleName();
    try {
      boolean retVal = tx.putEntryOnRemote(event, ifNew, ifOld, expectedOldValue, requireOldValue,
          lastModified, overwriteDestroyed);
      trackBucketForTx(event.getKeyInfo());
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(event.getKeyInfo(),
          transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(event.getKeyInfo(), primaryBucketException);
    }
  }

  @Override
  public boolean isFireCallbacks() {
    return getRealDeal(null, null).isFireCallbacks();
  }

  @Override
  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws DataLocationException {
    operationCount++;
    TXStateInterface tx = getRealDeal(event.getKeyInfo(), event.getRegion());
    assert (tx instanceof TXState);
    try {
      tx.destroyOnRemote(event, cacheWrite, expectedOldValue);
      trackBucketForTx(event.getKeyInfo());
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(event.getKeyInfo(),
          transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(event.getKeyInfo(), primaryBucketException);
    }
  }

  @Override
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
    operationCount++;
    TXStateInterface tx = getRealDeal(event.getKeyInfo(), event.getRegion());
    assert (tx instanceof TXState);
    try {
      tx.invalidateOnRemote(event, invokeCallbacks, forceNewEntry);
      trackBucketForTx(event.getKeyInfo());
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(event.getKeyInfo(),
          transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(event.getKeyInfo(), primaryBucketException);
    }
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

  @Override
  public Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones) {
    boolean resetTxState = isTransactionInternalSuspendNeeded(localRegion);
    TXStateProxy txp = null;
    if (resetTxState) {
      txp = getTxMgr().pauseTransaction();
    }
    try {
      if (resetTxState) {
        return localRegion.getSharedDataView().getBucketKeys(localRegion, bucketId, false);
      }
      return getRealDeal(null, localRegion).getBucketKeys(localRegion, bucketId, false);
    } finally {
      if (resetTxState) {
        getTxMgr().unpauseTransaction(txp);
      }
    }
  }

  @Override
  public Entry getEntryOnRemote(KeyInfo keyInfo, LocalRegion localRegion, boolean allowTombstones)
      throws DataLocationException {
    operationCount++;
    TXStateInterface tx = getRealDeal(keyInfo, localRegion);
    assert (tx instanceof TXState);
    try {
      return tx.getEntryOnRemote(keyInfo, localRegion, allowTombstones);
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(keyInfo, transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(keyInfo, primaryBucketException);
    }
  }

  public void forceLocalBootstrap() {
    getRealDeal(null, null);
  }

  @Override
  public DistributedMember getTarget() {
    return target;
  }

  @Override
  public void setTarget(DistributedMember target) {
    assert this.target == null;
    getRealDeal(target);
    if (this.target == null && isRealDealLocal()) {
      assert target.equals(getCache().getDistributedSystem().getDistributedMember());
      this.target = target;
    }
  }

  @Override
  public Collection<?> getRegionKeysForIteration(LocalRegion currRegion) {
    if (currRegion.isUsedForPartitionedRegionBucket()) {
      return currRegion.getRegionKeysForIteration();
    } else {
      boolean resetTxState = isTransactionInternalSuspendNeeded(currRegion);
      TXStateProxy txp = null;
      if (resetTxState) {
        txp = getTxMgr().pauseTransaction();
      }
      try {
        if (resetTxState) {
          return currRegion.getSharedDataView().getRegionKeysForIteration(currRegion);
        }
        return getRealDeal(null, currRegion).getRegionKeysForIteration(currRegion);
      } finally {
        if (resetTxState) {
          getTxMgr().unpauseTransaction(txp);
        }
      }
    }
  }

  @Override
  public boolean isCommitOnBehalfOfRemoteStub() {
    return commitRequestedByOwner;
  }

  @Override
  public boolean setCommitOnBehalfOfRemoteStub(boolean requestedByOwner) {
    return commitRequestedByOwner = requestedByOwner;
  }

  @Override
  public boolean isRealDealLocal() {
    if (realDeal != null) {
      return realDeal.isRealDealLocal();
    } else {
      // no real deal
      return false;
    }
  }

  /** if there is local txstate, return it */
  public TXState getLocalRealDeal() {
    if (realDeal != null) {
      if (realDeal.isRealDealLocal()) {
        return (TXState) realDeal;
      }
    }
    return null;
  }

  public boolean hasRealDeal() {
    return realDeal != null;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TXStateProxyImpl@").append(System.identityHashCode(this)).append(" txId:")
        .append(txId).append(" realDeal:").append(realDeal).append(" isJTA:")
        .append(isJTA);
    return builder.toString();
  }

  @Override
  public InternalDistributedMember getOriginatingMember() {
    if (realDeal == null) {
      return null;
    } else {
      return realDeal.getOriginatingMember();
    }
  }

  @Override
  public boolean isMemberIdForwardingRequired() {
    if (realDeal == null) {
      return false;
    } else {
      return realDeal.isMemberIdForwardingRequired();
    }
  }

  @Override
  public TXCommitMessage getCommitMessage() {
    if (realDeal == null) {
      return null;
    } else {
      return realDeal.getCommitMessage();
    }
  }

  @Override
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion reg) {
    if (putallOp.putAllData.length == 0) {
      return;
    }
    reg.getCancelCriterion().checkCancelInProgress(null); // fix for bug #43651
    Object key = null;
    if (putallOp.putAllData[0] != null) {
      key = putallOp.putAllData[0].key;
    }
    KeyInfo ki = new KeyInfo(key, null, null);
    TXStateInterface tsi = getRealDeal(ki, reg);
    tsi.postPutAll(putallOp, successfulPuts, reg);
  }

  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      InternalRegion reg) {
    if (op.removeAllData.length == 0) {
      return;
    }
    reg.getCancelCriterion().checkCancelInProgress(null); // fix for bug #43651
    Object key = null;
    if (op.removeAllData[0] != null) {
      key = op.removeAllData[0].key;
    }
    KeyInfo ki = new KeyInfo(key, null, null);
    TXStateInterface tsi = getRealDeal(ki, reg);
    tsi.postRemoveAll(op, successfulOps, reg);
  }

  @Override
  public boolean isJCATransaction() {
    return isJCATransaction;
  }


  @Override
  public void setJCATransaction() {
    isJCATransaction = true;
  }

  @Override
  public Entry accessEntry(KeyInfo keyInfo, LocalRegion region) {
    try {
      operationCount++;
      Entry retVal = getRealDeal(keyInfo, region).accessEntry(keyInfo, region);
      trackBucketForTx(keyInfo);
      return retVal;
    } catch (TransactionDataRebalancedException transactionDataRebalancedException) {
      throw handleTransactionDataRebalancedException(keyInfo, transactionDataRebalancedException);
    } catch (PrimaryBucketException primaryBucketException) {
      throw getTransactionException(keyInfo, primaryBucketException);
    }
  }

  @Override
  public void suspend() {
    if (realDeal != null) {
      getRealDeal(null, null).suspend();
    }
  }

  @Override
  public void resume() {
    if (realDeal != null) {
      getRealDeal(null, null).resume();
    }
  }

  /** test hook - record a list of ops in the transaction */
  @Override
  public void recordTXOperation(ServerRegionDataAccess region, ServerRegionOperation op, Object key,
      Object[] arguments) {
    if (ClientTXStateStub.transactionRecordingEnabled()) {
      getRealDeal(null, (LocalRegion) region.getRegion()).recordTXOperation(region, op, key,
          arguments);
    }
  }

  @Override
  public int operationCount() {
    return operationCount;
  }

  /**
   * increments the operation count by 1
   */
  public void incOperationCount() {
    operationCount++;
  }

  @Override
  public void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {
    // Do nothing. Not applicable for transactions.
  }

  @Override
  public void close() {
    if (realDeal != null) {
      realDeal.close();
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
    if (realDeal != null && realDeal.isRealDealLocal() && isOnBehalfOfClient()) {
      ((TXState) realDeal).setProxyServer(proxy);
    }
  }

  public boolean isOverTransactionTimeoutLimit() {
    return getCurrentTime() - getLastOperationTimeFromClient() > TimeUnit.SECONDS
        .toMillis(txMgr.getTransactionTimeToLive());
  }

  long getCurrentTime() {
    return System.currentTimeMillis();
  }

  synchronized long getLastOperationTimeFromClient() {
    return lastOperationTimeFromClient;
  }

  public synchronized void setLastOperationTimeFromClient(long lastOperationTimeFromClient) {
    this.lastOperationTimeFromClient = lastOperationTimeFromClient;
  }

  @Override
  public InternalDistributedMember getOnBehalfOfClientMember() {
    return onBehalfOfClientMember;
  }

  void setFirstOperationOnPartitionedRegion(boolean firstOperationOnPartitionedRegion) {
    this.firstOperationOnPartitionedRegion = firstOperationOnPartitionedRegion;
  }
}
