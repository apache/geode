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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.TransactionInDoubtException;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.TransactionWriter;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.TXManagerCancelledException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.SystemTimer.SystemTimerTask;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.MapCallback;

/**
 * The internal implementation of the {@link CacheTransactionManager} interface returned by
 * {@link InternalCache#getCacheTransactionManager}. Internal operations
 *
 * {@code TransactionListener} invocation, Region synchronization, transaction statistics and
 *
 * transaction logging are handled here
 *
 * @since GemFire 4.0
 *
 * @see CacheTransactionManager
 */
public class TXManagerImpl implements CacheTransactionManager, MembershipListener {

  private static final Logger logger = LogService.getLogger();

  // Thread specific context container
  private final ThreadLocal<TXStateProxy> txContext;

  private final ThreadLocal<Boolean> pauseJTA;

  private static TXManagerImpl currentInstance = null;

  // The unique transaction ID for this Manager
  private final AtomicInteger uniqId;

  private final DistributionManager dm;
  private final InternalCache cache;

  // The DistributionMemberID used to construct TXId's
  private final InternalDistributedMember distributionMgrId;

  private final CachePerfStats cachePerfStats;

  private static final TransactionListener[] EMPTY_LISTENERS = new TransactionListener[0];

  /**
   * Default transaction id to indicate no transaction
   */
  public static final int NOTX = -1;

  private final List<TransactionListener> txListeners = new ArrayList<>(8);

  public TransactionWriter writer = null;

  private volatile boolean closed = false;

  private final Map<TXId, TXStateProxy> hostedTXStates;

  /**
   * the number of client initiated transactions to store for client failover
   */
  public static final int FAILOVER_TX_MAP_SIZE =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "transactionFailoverMapSize", 1000);

  /**
   * used to store TXCommitMessages for client initiated transactions, so that when a client
   * failsover, (after the delegate dies) the commit message can be sent to client. //TODO we really
   * need to keep around only one msg for each thread on a client
   */
  @SuppressWarnings("unchecked")
  private Map<TXId, TXCommitMessage> failoverMap =
      Collections.synchronizedMap(new LinkedHashMap<TXId, TXCommitMessage>() {
        // TODO: inner class is serializable but outer class is not
        private static final long serialVersionUID = -4156018226167594134L;

        protected boolean removeEldestEntry(Entry eldest) {
          if (logger.isDebugEnabled()) {
            logger.debug("TX: removing client initiated transaction from failover map:{} :{}",
                eldest.getKey(), (size() > FAILOVER_TX_MAP_SIZE));
          }
          return size() > FAILOVER_TX_MAP_SIZE;
        }
      });

  /**
   * A flag to allow persistent transactions. public for testing.
   */
  public static boolean ALLOW_PERSISTENT_TRANSACTIONS =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "ALLOW_PERSISTENT_TRANSACTIONS");

  /**
   * this keeps track of all the transactions that were initiated locally.
   */
  private ConcurrentMap<TXId, TXStateProxy> localTxMap = new ConcurrentHashMap<>();

  /**
   * the time in minutes after which any suspended transaction are rolled back. default is 30
   * minutes
   */
  private volatile long suspendedTXTimeout =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "suspendedTxTimeout", 30);

  /**
   * Thread-specific flag to indicate whether the transactions managed by this
   * CacheTransactionManager for this thread should be distributed
   */
  private final ThreadLocal<Boolean> isTXDistributed;

  /**
   * The number of seconds to keep transaction states for disconnected clients. This allows the
   * client to fail over to another server and still find the transaction state to complete the
   * transaction.
   */
  private int transactionTimeToLive;

  /**
   * Constructor that implements the {@link CacheTransactionManager} interface. Only only one
   * instance per {@link org.apache.geode.cache.Cache}
   */
  public TXManagerImpl(CachePerfStats cachePerfStats, InternalCache cache) {
    this.cache = cache;
    this.dm = ((InternalDistributedSystem) cache.getDistributedSystem()).getDistributionManager();
    this.distributionMgrId = this.dm.getDistributionManagerId();
    this.uniqId = new AtomicInteger(0);
    this.cachePerfStats = cachePerfStats;
    this.hostedTXStates = new HashMap<>();
    this.txContext = new ThreadLocal<>();
    this.pauseJTA = new ThreadLocal<Boolean>();
    this.isTXDistributed = new ThreadLocal<>();
    this.transactionTimeToLive = Integer
        .getInteger(DistributionConfig.GEMFIRE_PREFIX + "cacheServer.transactionTimeToLive", 180);
    currentInstance = this;
  }

  public static TXManagerImpl getCurrentInstanceForTest() {
    return currentInstance;
  }

  public static void setCurrentInstanceForTest(TXManagerImpl instance) {
    currentInstance = instance;
  }

  InternalCache getCache() {
    return this.cache;
  }

  /**
   * Get the TransactionWriter for the cache
   *
   * @return the current TransactionWriter
   * @see TransactionWriter
   */
  public TransactionWriter getWriter() {
    return writer;
  }

  public void setWriter(TransactionWriter writer) {
    if (this.cache.isClient()) {
      throw new IllegalStateException(
          LocalizedStrings.TXManager_NO_WRITER_ON_CLIENT.toLocalizedString());
    }
    this.writer = writer;
  }

  public TransactionListener getListener() {
    synchronized (this.txListeners) {
      if (this.txListeners.isEmpty()) {
        return null;
      } else if (this.txListeners.size() == 1) {
        return this.txListeners.get(0);
      } else {
        throw new IllegalStateException(
            LocalizedStrings.TXManagerImpl_MORE_THAN_ONE_TRANSACTION_LISTENER_EXISTS
                .toLocalizedString());
      }
    }
  }

  public TransactionListener[] getListeners() {
    synchronized (this.txListeners) {
      int size = this.txListeners.size();
      if (size == 0) {
        return EMPTY_LISTENERS;
      } else {
        TransactionListener[] result = new TransactionListener[size];
        this.txListeners.toArray(result);
        return result;
      }
    }
  }

  public TransactionListener setListener(TransactionListener newListener) {
    synchronized (this.txListeners) {
      TransactionListener result = getListener();
      this.txListeners.clear();
      if (newListener != null) {
        this.txListeners.add(newListener);
      }
      if (result != null) {
        closeListener(result);
      }
      return result;
    }
  }

  public void addListener(TransactionListener aListener) {
    if (aListener == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.TXManagerImpl_ADDLISTENER_PARAMETER_WAS_NULL.toLocalizedString());
    }
    synchronized (this.txListeners) {
      if (!this.txListeners.contains(aListener)) {
        this.txListeners.add(aListener);
      }
    }
  }

  public void removeListener(TransactionListener aListener) {
    if (aListener == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.TXManagerImpl_REMOVELISTENER_PARAMETER_WAS_NULL.toLocalizedString());
    }
    synchronized (this.txListeners) {
      if (this.txListeners.remove(aListener)) {
        closeListener(aListener);
      }
    }
  }

  public void initListeners(TransactionListener[] newListeners) {
    synchronized (this.txListeners) {
      if (!this.txListeners.isEmpty()) {
        Iterator<TransactionListener> it = this.txListeners.iterator();
        while (it.hasNext()) {
          closeListener(it.next());
        }
        this.txListeners.clear();
      }
      if (newListeners != null && newListeners.length > 0) {
        List<TransactionListener> nl = Arrays.asList(newListeners);
        if (nl.contains(null)) {
          throw new IllegalArgumentException(
              LocalizedStrings.TXManagerImpl_INITLISTENERS_PARAMETER_HAD_A_NULL_ELEMENT
                  .toLocalizedString());
        }
        this.txListeners.addAll(nl);
      }
    }
  }

  CachePerfStats getCachePerfStats() {
    return this.cachePerfStats;
  }

  /**
   * Build a new {@link TXId}, use it as part of the transaction state and associate with the
   * current thread using a {@link ThreadLocal}.
   */
  public void begin() {
    checkClosed();
    {
      TransactionId tid = getTransactionId();
      if (tid != null) {
        throw new java.lang.IllegalStateException(
            LocalizedStrings.TXManagerImpl_TRANSACTION_0_ALREADY_IN_PROGRESS
                .toLocalizedString(tid));
      }
    }
    {
      TXStateProxy curProxy = txContext.get();
      if (curProxy == PAUSED) {
        throw new java.lang.IllegalStateException(
            "Current thread has paused its transaction so it can not start a new transaction");
      }
    }
    TXId id = new TXId(this.distributionMgrId, this.uniqId.incrementAndGet());
    TXStateProxyImpl proxy = null;
    if (isDistributed()) {
      proxy = new DistTXStateProxyImplOnCoordinator(cache, this, id, null);
    } else {
      proxy = new TXStateProxyImpl(cache, this, id, null);
    }
    setTXState(proxy);
    this.localTxMap.put(id, proxy);
  }


  /**
   * Build a new {@link TXId}, use it as part of the transaction state and associate with the
   * current thread using a {@link ThreadLocal}. Flag the transaction to be enlisted with a JTA
   * Transaction. Should only be called in a context where we know there is no existing transaction.
   */
  public TXStateProxy beginJTA() {
    checkClosed();
    TXId id = new TXId(this.distributionMgrId, this.uniqId.incrementAndGet());
    TXStateProxy newState = null;

    if (isDistributed()) {
      newState = new DistTXStateProxyImplOnCoordinator(cache, this, id, true);
    } else {
      newState = new TXStateProxyImpl(cache, this, id, true);
    }
    setTXState(newState);
    return newState;
  }

  /*
   * Only applicable for Distributed transaction.
   */
  public void precommit() throws CommitConflictException {
    checkClosed();

    final TXStateProxy tx = getTXState();
    if (tx == null) {
      throw new IllegalStateException(
          LocalizedStrings.TXManagerImpl_THREAD_DOES_NOT_HAVE_AN_ACTIVE_TRANSACTION
              .toLocalizedString());
    }

    tx.checkJTA(
        LocalizedStrings.TXManagerImpl_CAN_NOT_COMMIT_THIS_TRANSACTION_BECAUSE_IT_IS_ENLISTED_WITH_A_JTA_TRANSACTION_USE_THE_JTA_MANAGER_TO_PERFORM_THE_COMMIT
            .toLocalizedString());

    tx.precommit();
  }

  /**
   * Complete the transaction associated with the current thread. When this method completes, the
   * thread is no longer associated with a transaction.
   *
   */
  public void commit() throws CommitConflictException {
    checkClosed();

    final TXStateProxy tx = getTXState();
    if (tx == null) {
      throw new IllegalStateException(
          LocalizedStrings.TXManagerImpl_THREAD_DOES_NOT_HAVE_AN_ACTIVE_TRANSACTION
              .toLocalizedString());
    }

    tx.checkJTA(
        LocalizedStrings.TXManagerImpl_CAN_NOT_COMMIT_THIS_TRANSACTION_BECAUSE_IT_IS_ENLISTED_WITH_A_JTA_TRANSACTION_USE_THE_JTA_MANAGER_TO_PERFORM_THE_COMMIT
            .toLocalizedString());

    final long opStart = CachePerfStats.getStatTime();
    final long lifeTime = opStart - tx.getBeginTime();
    try {
      setTXState(null);
      tx.commit();
    } catch (CommitConflictException ex) {
      saveTXStateForClientFailover(tx, TXCommitMessage.CMT_CONFLICT_MSG); // fixes #43350
      noteCommitFailure(opStart, lifeTime, tx);
      cleanup(tx.getTransactionId()); // fixes #52086
      throw ex;
    } catch (TransactionDataRebalancedException reb) {
      saveTXStateForClientFailover(tx, TXCommitMessage.REBALANCE_MSG);
      cleanup(tx.getTransactionId()); // fixes #52086
      throw reb;
    } catch (UnsupportedOperationInTransactionException e) {
      // fix for #42490
      setTXState(tx);
      throw e;
    } catch (RuntimeException e) {
      saveTXStateForClientFailover(tx, TXCommitMessage.EXCEPTION_MSG);
      cleanup(tx.getTransactionId()); // fixes #52086
      throw e;
    }
    saveTXStateForClientFailover(tx);
    cleanup(tx.getTransactionId());
    noteCommitSuccess(opStart, lifeTime, tx);
  }

  void noteCommitFailure(long opStart, long lifeTime, TXStateInterface tx) {
    long opEnd = CachePerfStats.getStatTime();
    this.cachePerfStats.txFailure(opEnd - opStart, lifeTime, tx.getChanges());
    TransactionListener[] listeners = getListeners();
    if (tx.isFireCallbacks() && listeners.length > 0) {
      final TXEvent e = tx.getEvent();
      try {
        for (int i = 0; i < listeners.length; i++) {
          try {
            listeners[i].afterFailedCommit(e);
          } catch (VirtualMachineError err) {
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
            logger.error(LocalizedMessage.create(
                LocalizedStrings.TXManagerImpl_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER), t);
          }
        }
      } finally {
        e.release();
      }
    }
  }

  void noteCommitSuccess(long opStart, long lifeTime, TXStateInterface tx) {
    long opEnd = CachePerfStats.getStatTime();
    this.cachePerfStats.txSuccess(opEnd - opStart, lifeTime, tx.getChanges());
    TransactionListener[] listeners = getListeners();
    if (tx.isFireCallbacks() && listeners.length > 0) {
      final TXEvent e = tx.getEvent();
      try {
        for (final TransactionListener listener : listeners) {
          try {
            listener.afterCommit(e);
          } catch (VirtualMachineError err) {
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
            logger.error(LocalizedMessage.create(
                LocalizedStrings.TXManagerImpl_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER), t);
          }
        }
      } finally {
        e.release();
      }
    }
  }

  /**
   * prepare for transaction replay by assigning a new tx id to the current proxy
   */
  private void _incrementTXUniqueIDForReplay() {
    TXStateProxyImpl tx = (TXStateProxyImpl) getTXState();
    assert tx != null : "expected a transaction to be in progress";
    TXId id = new TXId(this.distributionMgrId, this.uniqId.incrementAndGet());
    tx.setTXIDForReplay(id);
  }


  /**
   * Roll back the transaction associated with the current thread. When this method completes, the
   * thread is no longer associated with a transaction.
   */
  public void rollback() {
    checkClosed();
    TXStateProxy tx = getTXState();
    if (tx == null) {
      throw new IllegalStateException(
          LocalizedStrings.TXManagerImpl_THREAD_DOES_NOT_HAVE_AN_ACTIVE_TRANSACTION
              .toLocalizedString());
    }

    tx.checkJTA(
        LocalizedStrings.TXManagerImpl_CAN_NOT_ROLLBACK_THIS_TRANSACTION_IS_ENLISTED_WITH_A_JTA_TRANSACTION_USE_THE_JTA_MANAGER_TO_PERFORM_THE_ROLLBACK
            .toLocalizedString());

    final long opStart = CachePerfStats.getStatTime();
    final long lifeTime = opStart - tx.getBeginTime();
    setTXState(null);
    tx.rollback();
    saveTXStateForClientFailover(tx);
    cleanup(tx.getTransactionId());
    noteRollbackSuccess(opStart, lifeTime, tx);
  }

  void noteRollbackSuccess(long opStart, long lifeTime, TXStateInterface tx) {
    long opEnd = CachePerfStats.getStatTime();
    this.cachePerfStats.txRollback(opEnd - opStart, lifeTime, tx.getChanges());
    TransactionListener[] listeners = getListeners();
    if (tx.isFireCallbacks() && listeners.length > 0) {
      final TXEvent e = tx.getEvent();
      try {
        for (int i = 0; i < listeners.length; i++) {
          try {
            listeners[i].afterRollback(e);
          } catch (VirtualMachineError err) {
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
            logger.error(LocalizedMessage.create(
                LocalizedStrings.TXManagerImpl_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER), t);
          }
        }
      } finally {
        e.release();
      }
    }
  }

  /**
   * Called from Commit and Rollback to unblock waiting threads
   */
  private void cleanup(TransactionId txId) {
    TXStateProxy proxy = this.localTxMap.remove(txId);
    if (proxy != null) {
      proxy.close();
    }
    Queue<Thread> waitingThreads = this.waitMap.get(txId);
    if (waitingThreads != null && !waitingThreads.isEmpty()) {
      for (Thread waitingThread : waitingThreads) {
        LockSupport.unpark(waitingThread);
      }
      waitMap.remove(txId);
    }
  }

  /**
   * Reports the existence of a Transaction for this thread
   *
   */
  public boolean exists() {
    return null != getTXState();
  }

  /**
   * Gets the current transaction identifier or null if no transaction exists
   *
   */
  public TransactionId getTransactionId() {
    TXStateProxy t = getTXState();
    TransactionId ret = null;
    if (t != null) {
      ret = t.getTransactionId();
    }
    return ret;
  }

  /**
   * Returns the TXStateProxyInterface of the current thread; null if no transaction.
   */
  public TXStateProxy getTXState() {
    TXStateProxy tsp = txContext.get();
    if (tsp == PAUSED) {
      // treats paused transaction as no transaction.
      return null;
    }
    if (tsp != null && !tsp.isInProgress()) {
      this.txContext.set(null);
      tsp = null;
    }
    return tsp;
  }

  /**
   * sets {@link TXStateProxy#setInProgress(boolean)} when a txContext is present. This method must
   * only be used in fail-over scenarios.
   *
   * @param progress value of the progress flag to be set
   * @return the previous value of inProgress flag
   * @see TXStateProxy#setInProgress(boolean)
   */
  public boolean setInProgress(boolean progress) {
    boolean retVal = false;
    TXStateProxy tsp = txContext.get();
    assert tsp != PAUSED;
    if (tsp != null) {
      retVal = tsp.isInProgress();
      tsp.setInProgress(progress);
    }
    return retVal;
  }

  public void setTXState(TXStateProxy val) {
    txContext.set(val);
  }


  public void close() {
    if (isClosed()) {
      return;
    }
    TXStateProxy[] proxies = null;
    synchronized (this.hostedTXStates) {
      // After this, newly added TXStateProxy would not operate on the TXState.
      this.closed = true;

      proxies = this.hostedTXStates.values().toArray(new TXStateProxy[this.hostedTXStates.size()]);
    }

    for (TXStateProxy proxy : proxies) {
      proxy.getLock().lock();
      try {
        proxy.close();
      } finally {
        proxy.getLock().unlock();
      }
    }
    for (TXStateProxy proxy : this.localTxMap.values()) {
      proxy.close();
    }
    TransactionListener[] listeners = getListeners();
    for (int i = 0; i < listeners.length; i++) {
      closeListener(listeners[i]);
    }
  }

  private void closeListener(TransactionListener tl) {
    try {
      tl.close();
    } catch (VirtualMachineError err) {
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
      logger.error(LocalizedMessage
          .create(LocalizedStrings.TXManagerImpl_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER), t);
    }
  }

  private static final TXStateProxy PAUSED = new PausedTXStateProxyImpl();

  /**
   * If the current thread is in a transaction then pause will cause it to no longer be in a
   * transaction. The same thread is expected to unpause/resume the transaction later. The thread
   * should not start a new transaction after it paused a transaction.
   *
   * @return the state of the transaction or null. Pass this value to
   *         {@link TXManagerImpl#unpauseTransaction} to reactivate the puased/suspended
   *         transaction.
   */
  public TXStateProxy pauseTransaction() {
    return internalSuspend(true);
  }

  /**
   * If the current thread is in a transaction then internal suspend will cause it to no longer be
   * in a transaction. The thread can start a new transaction after it internal suspended a
   * transaction.
   *
   * @return the state of the transaction or null. to reactivate the suspended transaction.
   */
  public TXStateProxy internalSuspend() {
    return internalSuspend(false);
  }

  /**
   * If the current thread is in a transaction then suspend will cause it to no longer be in a
   * transaction.
   *
   * @param needToResumeBySameThread whether a suspended transaction needs to be resumed by the same
   *        thread.
   * @return the state of the transaction or null. Pass this value to
   *         {@link TXManagerImpl#internalResume(TXStateProxy, boolean)} to reactivate the suspended
   *         transaction.
   */
  private TXStateProxy internalSuspend(boolean needToResumeBySameThread) {
    TXStateProxy result = getTXState();
    if (result != null) {
      result.suspend();
      if (needToResumeBySameThread) {
        setTXState(PAUSED);
      } else {
        setTXState(null);
      }
    } else {
      if (needToResumeBySameThread) {
        // pausedJTA is set to true when JTA is not yet bootstrapped.
        pauseJTA.set(true);
      }
    }
    return result;
  }

  /**
   * Activates the specified transaction on the calling thread. Only the same thread that pause the
   * transaction can unpause it.
   *
   * @param tx the transaction to be unpaused.
   * @throws IllegalStateException if this thread already has an active transaction or this thread
   *         did not pause the transaction.
   */
  public void unpauseTransaction(TXStateProxy tx) {
    internalResume(tx, true);
  }

  /**
   * Activates the specified transaction on the calling thread. Does not require the same thread to
   * resume it.
   *
   * @param tx the transaction to activate.
   * @throws IllegalStateException if this thread already has an active transaction
   */
  public void internalResume(TXStateProxy tx) {
    internalResume(tx, false);
  }

  /**
   * Activates the specified transaction on the calling thread.
   *
   * @param tx the transaction to activate.
   * @param needToResumeBySameThread whether a suspended transaction needs to be resumed by the same
   *        thread.
   * @throws IllegalStateException if this thread already has an active transaction
   */
  private void internalResume(TXStateProxy tx, boolean needToResumeBySameThread) {
    if (tx != null) {
      TransactionId tid = getTransactionId();
      if (tid != null) {
        throw new java.lang.IllegalStateException(
            LocalizedStrings.TXManagerImpl_TRANSACTION_0_ALREADY_IN_PROGRESS
                .toLocalizedString(tid));
      }
      if (needToResumeBySameThread) {
        TXStateProxy result = txContext.get();
        if (result != PAUSED) {
          throw new java.lang.IllegalStateException(
              "try to unpause a transaction not paused by the same thread");
        }
      }
      setTXState(tx);

      tx.resume();
    } else {
      if (needToResumeBySameThread) {
        pauseJTA.set(false);
      }
    }
  }

  public boolean isTransactionPaused() {
    return txContext.get() == PAUSED;
  }

  public boolean isJTAPaused() {
    Boolean jtaPaused = pauseJTA.get();
    if (jtaPaused == null) {
      return false;
    }
    return jtaPaused;
  }

  /**
   * @deprecated use internalResume instead
   */
  @Deprecated
  public void resume(TXStateProxy tx) {
    internalResume(tx);
  }

  public boolean isClosed() {
    return this.closed;
  }

  private void checkClosed() {
    cache.getCancelCriterion().checkCancelInProgress(null);
    if (this.closed) {
      throw new TXManagerCancelledException("This transaction manager is closed.");
    }
  }

  DistributionManager getDM() {
    return this.dm;
  }

  public static int getCurrentTXUniqueId() {
    if (currentInstance == null) {
      return NOTX;
    }
    return currentInstance.getMyTXUniqueId();
  }

  public static TXStateProxy getCurrentTXState() {
    if (currentInstance == null) {
      return null;
    }
    return currentInstance.getTXState();
  }

  public static void incrementTXUniqueIDForReplay() {
    if (currentInstance != null) {
      currentInstance._incrementTXUniqueIDForReplay();
    }
  }

  public int getMyTXUniqueId() {
    TXStateProxy t = txContext.get();
    if (t != null && t != PAUSED) {
      return t.getTxId().getUniqId();
    } else {
      return NOTX;
    }
  }

  /**
   * Associate the remote txState with the thread processing this message. Also, we acquire a lock
   * on the txState, on which this thread operates. Some messages like SizeMessage should not create
   * a new txState.
   *
   * @return {@link TXStateProxy} the txProxy for the transactional message
   */
  public TXStateProxy masqueradeAs(TransactionMessage msg) throws InterruptedException {
    if (msg.getTXUniqId() == NOTX || !msg.canParticipateInTransaction()) {
      return null;
    }
    TXId key = new TXId(msg.getMemberToMasqueradeAs(), msg.getTXUniqId());
    TXStateProxy val = getOrSetHostedTXState(key, msg);

    if (val != null) {
      boolean success = getLock(val, key);
      while (!success) {
        val = getOrSetHostedTXState(key, msg);
        if (val != null) {
          success = getLock(val, key);
        } else {
          break;
        }
      }
    }

    setTXState(val);
    return val;
  }

  TXStateProxy getOrSetHostedTXState(TXId key, TransactionMessage msg) {
    TXStateProxy val = this.hostedTXStates.get(key);
    if (val == null) {
      synchronized (this.hostedTXStates) {
        val = this.hostedTXStates.get(key);
        if (val == null && msg.canStartRemoteTransaction()) {
          if (msg.isTransactionDistributed()) {
            val = new DistTXStateProxyImplOnDatanode(cache, this, key, msg.getTXOriginatorClient());
            val.setLocalTXState(new DistTXState(val, true));
          } else {
            val = new TXStateProxyImpl(cache, this, key, msg.getTXOriginatorClient());
            val.setLocalTXState(new TXState(val, true));
          }
          this.hostedTXStates.put(key, val);
        }
      }
    }
    return val;
  }

  boolean getLock(TXStateProxy val, TXId key) {
    if (!val.getLock().isHeldByCurrentThread()) {
      val.getLock().lock();
      synchronized (this.hostedTXStates) {
        TXStateProxy curVal = this.hostedTXStates.get(key);
        // Inflight op could be received later than TXFailover operation.
        if (curVal == null) {
          if (!isHostedTxRecentlyCompleted(key)) {
            // Failover op removed the val
            // It is possible that the same operation can be executed
            // twice by two threads, but data is consistent.
            this.hostedTXStates.put(key, val);
          } else {
            // Another thread should complete the transaction
            logger.info("{} has already finished.", val.getTxId());
          }
        } else {
          if (val != curVal) {
            // Failover op replaced with a new TXStateProxyImpl
            // Use the new one instead.
            val.getLock().unlock();
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Associate the remote txState with the thread processing this message. Also, we acquire a lock
   * on the txState, on which this thread operates. Some messages like SizeMessage should not create
   * a new txState.
   *
   * @param probeOnly - do not masquerade; just look up the TX state
   * @return {@link TXStateProxy} the txProxy for the transactional message
   */
  public TXStateProxy masqueradeAs(Message msg, InternalDistributedMember memberId,
      boolean probeOnly) throws InterruptedException {
    if (msg.getTransactionId() == NOTX) {
      return null;
    }
    TXId key = new TXId(memberId, msg.getTransactionId());
    TXStateProxy val;
    val = this.hostedTXStates.get(key);
    if (val == null) {
      synchronized (this.hostedTXStates) {
        val = this.hostedTXStates.get(key);
        if (val == null) {
          // TODO: Conditionally create object based on distributed or non-distributed tx mode
          if (msg instanceof TransactionMessage
              && ((TransactionMessage) msg).isTransactionDistributed()) {
            val = new DistTXStateProxyImplOnDatanode(cache, this, key, memberId);
            // val.setLocalTXState(new DistTXState(val,true));
          } else {
            val = new TXStateProxyImpl(cache, this, key, memberId);
            // val.setLocalTXState(new TXState(val,true));
          }
          this.hostedTXStates.put(key, val);
        }
      }
    }
    if (!probeOnly) {
      if (val != null) {
        if (!val.getLock().isHeldByCurrentThread()) {
          val.getLock().lock();
          // add the TXStateProxy back to the map
          // in-case another thread removed it while we were waiting to lock.
          // This can happen during client transaction failover.
          synchronized (this.hostedTXStates) {
            this.hostedTXStates.put(key, val);
          }
        }
      }
      setTXState(val);
    }
    return val;
  }



  /**
   * Associate the transactional state with this thread.
   *
   * @param txState the transactional state.
   */
  public void masqueradeAs(TXStateProxy txState) {
    assert txState != null;
    if (!txState.getLock().isHeldByCurrentThread()) {
      txState.getLock().lock();
    }
    setTXState(txState);
  }

  /**
   * Remove the association created by {@link #masqueradeAs(TransactionMessage)}
   */
  public void unmasquerade(TXStateProxy tx) {
    if (tx != null) {
      cleanupTransactionIfNoLongerHost(tx);
      setTXState(null);
      tx.getLock().unlock();
    }
  }

  private void cleanupTransactionIfNoLongerHost(TXStateProxy tx) {
    synchronized (hostedTXStates) {
      if (!hostedTXStates.containsKey(tx.getTxId())) {
        // clean up the transaction if no longer the host of the transaction
        // this could occur when a failover command removed the transaction.
        if (tx.isRealDealLocal()) {
          ((TXStateProxyImpl) tx).getLocalRealDeal().cleanup();
        }
      }
    }
  }

  /**
   * Cleanup the remote txState after commit and rollback
   *
   * @return the TXStateProxy
   */
  public TXStateProxy removeHostedTXState(TXId txId) {
    synchronized (this.hostedTXStates) {
      TXStateProxy result = this.hostedTXStates.remove(txId);
      if (result != null) {
        result.close();
      }
      return result;
    }
  }

  /**
   * Called when the CacheServer is shutdown. Removes txStates hosted on client's behalf
   */
  protected void removeHostedTXStatesForClients() {
    synchronized (this.hostedTXStates) {
      Iterator<Entry<TXId, TXStateProxy>> iterator = this.hostedTXStates.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<TXId, TXStateProxy> entry = iterator.next();
        if (entry.getValue().isOnBehalfOfClient()) {
          entry.getValue().close();
          if (logger.isDebugEnabled()) {
            logger.debug("Cleaning up TXStateProxy for {}", entry.getKey());
          }
          iterator.remove();
        }
      }
    }
  }

  /**
   * Used to verify if a transaction with a given id is hosted by this txManager.
   *
   * @return true if the transaction is in progress, false otherwise
   */
  public boolean isHostedTxInProgress(TXId txId) {
    synchronized (this.hostedTXStates) {
      TXStateProxy tx = this.hostedTXStates.get(txId);
      if (tx == null) {
        return false;
      }
      return tx.isRealDealLocal();
    }
  }

  public TXStateProxy getHostedTXState(TXId txId) {
    synchronized (this.hostedTXStates) {
      return this.hostedTXStates.get(txId);
    }
  }

  /**
   * @return number of transaction in progress on behalf of remote nodes
   */
  public int hostedTransactionsInProgressForTest() {
    synchronized (this.hostedTXStates) {
      return this.hostedTXStates.size();
    }
  }

  public int localTransactionsInProgressForTest() {
    return this.localTxMap.size();
  }

  public void memberDeparted(DistributionManager distributionManager, InternalDistributedMember id,
      boolean crashed) {
    synchronized (this.hostedTXStates) {
      Iterator<Map.Entry<TXId, TXStateProxy>> iterator = this.hostedTXStates.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<TXId, TXStateProxy> me = iterator.next();
        TXId txId = me.getKey();
        if (txId.getMemberId().equals(id)) {
          me.getValue().close();
          if (logger.isDebugEnabled()) {
            logger.debug("Received memberDeparted, cleaning up txState:{}", txId);
          }
          iterator.remove();
        }
      }
    }
    expireClientTransactionsSentFromDepartedProxy(id);
  }

  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {}

  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}


  /**
   * retrieve the transaction TXIds for the given client
   *
   * @param id the client's membership ID
   * @return a set of the currently open TXIds
   */
  public Set<TXId> getTransactionsForClient(InternalDistributedMember id) {
    Set<TXId> result = new HashSet<TXId>();
    synchronized (this.hostedTXStates) {
      for (Map.Entry<TXId, TXStateProxy> entry : this.hostedTXStates.entrySet()) {
        if (entry.getKey().getMemberId().equals(id)) {
          result.add(entry.getKey());
        }
      }
    }
    return result;
  }

  /**
   * retrieve the transaction states for the given client
   *
   * @param id the client's membership ID
   * @return a set of the currently open transaction states
   */
  public Set<TXStateProxy> getTransactionStatesForClient(InternalDistributedMember id) {
    Set<TXStateProxy> result = new HashSet<TXStateProxy>();
    synchronized (this.hostedTXStates) {
      for (Map.Entry<TXId, TXStateProxy> entry : this.hostedTXStates.entrySet()) {
        if (entry.getKey().getMemberId().equals(id)) {
          result.add(entry.getValue());
        }
      }
    }
    return result;
  }

  /** remove the given TXStates */
  public void removeTransactions(Set<TXId> txIds, boolean distribute) {
    if (logger.isDebugEnabled()) {
      logger.debug("expiring the following transactions: {}", txIds);
    }
    synchronized (this.hostedTXStates) {
      Iterator<Map.Entry<TXId, TXStateProxy>> iterator = this.hostedTXStates.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<TXId, TXStateProxy> entry = iterator.next();
        if (txIds.contains(entry.getKey())) {
          entry.getValue().close();
          iterator.remove();
        }
      }
    }
    if (distribute) {
      // tell other VMs to also remove the transactions
      TXRemovalMessage.send(this.dm, this.dm.getOtherDistributionManagerIds(), txIds);
    }
  }

  void saveTXStateForClientFailover(TXStateProxy tx) {
    if (tx.isOnBehalfOfClient() && tx.isRealDealLocal()) {
      TXCommitMessage commitMessage =
          tx.getCommitMessage() == null ? TXCommitMessage.ROLLBACK_MSG : tx.getCommitMessage();
      failoverMap.put(tx.getTxId(), commitMessage);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "TX: storing client initiated transaction:{}; now there are {} entries in the failoverMap",
            tx.getTxId(), failoverMap.size());
      }
    }
  }

  private void saveTXStateForClientFailover(TXStateProxy tx, TXCommitMessage msg) {
    if (tx.isOnBehalfOfClient() && tx.isRealDealLocal()) {
      failoverMap.put(tx.getTxId(), msg);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "TX: storing client initiated transaction:{}; now there are {} entries in the failoverMap",
            tx.getTxId(), failoverMap.size());
      }
    }
  }

  public void saveTXCommitMessageForClientFailover(TXId txId, TXCommitMessage msg) {
    failoverMap.put(txId, msg);
  }

  public boolean isHostedTxRecentlyCompleted(TXId txId) {
    synchronized (failoverMap) {
      if (failoverMap.containsKey(txId)) {
        // if someone is asking to see if we have the txId, they will come
        // back and ask for the commit message, this could take a long time
        // specially when called from TXFailoverCommand, so we move
        // the txId back to the linked map by removing and putting it back.
        TXCommitMessage msg = failoverMap.remove(txId);
        failoverMap.put(txId, msg);
        return true;
      }
      return false;
    }
  }


  /**
   * If the given transaction is already being completed by another thread this will wait for that
   * completion to finish and will ensure that the result is saved in the client failover map.
   *
   * @return true if a wait was performed
   */
  public boolean waitForCompletingTransaction(TXId txId) {
    TXStateProxy val;
    val = this.hostedTXStates.get(txId);
    if (val == null) {
      synchronized (this.hostedTXStates) {
        val = this.hostedTXStates.get(txId);
      }
    }
    if (val != null && val.isRealDealLocal()) {
      TXStateProxyImpl impl = (TXStateProxyImpl) val;
      TXState state = impl.getLocalRealDeal();
      if (state.waitForPreviousCompletion()) {
        // the thread we were waiting for would have put a TXCommitMessage
        // in the failover map, doing so here may replace an existing token
        // like TXCommitMessage.REBALANCE_MSG with null. fixes bug 42661
        // saveTXStateForClientFailover(impl);
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the TXCommitMessage for a transaction that has been successfully completed.
   *
   * @return the commit message or an exception token e.g {@link TXCommitMessage#CMT_CONFLICT_MSG}
   *         if the transaction threw an exception
   * @see #isExceptionToken(TXCommitMessage)
   */
  public TXCommitMessage getRecentlyCompletedMessage(TXId txId) {
    return failoverMap.get(txId);
  }

  /**
   * @return true if msg is an exception token, false otherwise
   */
  public boolean isExceptionToken(TXCommitMessage msg) {
    if (msg == TXCommitMessage.CMT_CONFLICT_MSG || msg == TXCommitMessage.REBALANCE_MSG
        || msg == TXCommitMessage.EXCEPTION_MSG) {
      return true;
    }
    return false;
  }

  /**
   * Generates exception messages for the three TXCommitMessage tokens that represent exceptions
   * during transaction execution.
   *
   * @param msg the token that represents the exception
   * @return the exception
   */
  public RuntimeException getExceptionForToken(TXCommitMessage msg, TXId txId) {
    if (msg == TXCommitMessage.CMT_CONFLICT_MSG) {
      return new CommitConflictException(
          LocalizedStrings.TXState_CONFLICT_DETECTED_IN_GEMFIRE_TRANSACTION_0
              .toLocalizedString(txId));
    }
    if (msg == TXCommitMessage.REBALANCE_MSG) {
      return new TransactionDataRebalancedException(
          LocalizedStrings.PartitionedRegion_TRANSACTIONAL_DATA_MOVED_DUE_TO_REBALANCING
              .toLocalizedString());
    }
    if (msg == TXCommitMessage.EXCEPTION_MSG) {
      return new TransactionInDoubtException(
          LocalizedStrings.ClientTXStateStub_COMMIT_FAILED_ON_SERVER.toLocalizedString());
    }
    throw new InternalGemFireError("the parameter TXCommitMessage is not an exception token");
  }

  public static class TXRemovalMessage extends HighPriorityDistributionMessage {

    Set<TXId> txIds;

    /** for deserialization */
    public TXRemovalMessage() {}

    static void send(DistributionManager dm, Set recipients, Set<TXId> txIds) {
      TXRemovalMessage msg = new TXRemovalMessage();
      msg.txIds = txIds;
      msg.setRecipients(recipients);
      dm.putOutgoing(msg);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeHashSet((HashSet<TXId>) this.txIds, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.txIds = DataSerializer.readHashSet(in);
    }

    public int getDSFID() {
      return TX_MANAGER_REMOVE_TRANSACTIONS;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      InternalCache cache = dm.getCache();
      if (cache != null) {
        TXManagerImpl mgr = cache.getTXMgr();
        mgr.removeTransactions(this.txIds, false);
      }
    }
  }

  private ConcurrentMap<TransactionId, TXStateProxy> suspendedTXs =
      new ConcurrentHashMap<TransactionId, TXStateProxy>();

  public TransactionId suspend() {
    return suspend(TimeUnit.MINUTES);
  }

  TransactionId suspend(TimeUnit expiryTimeUnit) {
    TXStateProxy result = getTXState();
    if (result != null) {
      TransactionId txId = result.getTransactionId();
      result.suspend();
      setTXState(null);
      this.suspendedTXs.put(txId, result);
      // wake up waiting threads
      Queue<Thread> waitingThreads = this.waitMap.get(txId);
      if (waitingThreads != null) {
        Thread waitingThread = null;
        while (true) {
          waitingThread = waitingThreads.poll();
          if (waitingThread == null || !Thread.currentThread().equals(waitingThread)) {
            break;
          }
        }
        if (waitingThread != null) {
          LockSupport.unpark(waitingThread);
        }
      }
      scheduleExpiry(txId, expiryTimeUnit);
      return txId;
    }
    return null;
  }

  public void resume(TransactionId transactionId) {
    if (transactionId == null) {
      throw new IllegalStateException(
          LocalizedStrings.TXManagerImpl_UNKNOWN_TRANSACTION_OR_RESUMED.toLocalizedString());
    }
    if (getTXState() != null) {
      throw new IllegalStateException(
          LocalizedStrings.TXManagerImpl_TRANSACTION_ACTIVE_CANNOT_RESUME.toLocalizedString());
    }
    TXStateProxy txProxy = this.suspendedTXs.remove(transactionId);
    if (txProxy == null) {
      throw new IllegalStateException(
          LocalizedStrings.TXManagerImpl_UNKNOWN_TRANSACTION_OR_RESUMED.toLocalizedString());
    }
    resumeProxy(txProxy);
  }

  public boolean isSuspended(TransactionId transactionId) {
    return this.suspendedTXs.containsKey(transactionId);
  }

  public boolean tryResume(TransactionId transactionId) {
    if (transactionId == null || getTXState() != null) {
      return false;
    }
    TXStateProxy txProxy = this.suspendedTXs.remove(transactionId);
    if (txProxy != null) {
      resumeProxy(txProxy);
      return true;
    }
    return false;
  }

  private void resumeProxy(TXStateProxy txProxy) {
    assert txProxy != null;
    assert getTXState() == null;
    setTXState(txProxy);
    txProxy.resume();
    SystemTimerTask task = this.expiryTasks.remove(txProxy.getTransactionId());
    if (task != null) {
      if (task.cancel()) {
        this.cache.purgeCCPTimer();
      }
    }
  }

  /**
   * this map keeps track of all the threads that are waiting in
   * {@link #tryResume(TransactionId, long, TimeUnit)} for a particular transactionId
   */
  private ConcurrentMap<TransactionId, Queue<Thread>> waitMap = new ConcurrentHashMap<>();

  Queue<Thread> getWaitQueue(TransactionId transactionId) {
    return waitMap.get(transactionId);
  }

  private Queue<Thread> getOrCreateWaitQueue(TransactionId transactionId) {
    Queue<Thread> threadq = getWaitQueue(transactionId);
    if (threadq == null) {
      threadq = new ConcurrentLinkedQueue<Thread>();
      Queue<Thread> oldq = waitMap.putIfAbsent(transactionId, threadq);
      if (oldq != null) {
        threadq = oldq;
      }
    }
    return threadq;
  }

  public boolean tryResume(TransactionId transactionId, long time, TimeUnit unit) {
    if (transactionId == null || getTXState() != null || !exists(transactionId)) {
      return false;
    }
    final Thread currentThread = Thread.currentThread();
    final long endTime = System.nanoTime() + unit.toNanos(time);
    final Queue<Thread> threadq = getOrCreateWaitQueue(transactionId);

    try {
      while (true) {
        if (!threadq.contains(currentThread)) {
          threadq.add(currentThread);
        }
        if (tryResume(transactionId)) {
          return true;
        }
        if (!exists(transactionId)) {
          return false;
        }
        long parkTimeout = endTime - System.nanoTime();
        if (parkTimeout <= 0) {
          return false;
        }
        parkToRetryResume(parkTimeout);
      }
    } finally {
      threadq.remove(currentThread);
      // the queue itself will be removed at commit/rollback
    }
  }

  void parkToRetryResume(long timeout) {
    LockSupport.parkNanos(timeout);
  }

  public boolean exists(TransactionId transactionId) {
    return isHostedTxInProgress((TXId) transactionId) || isSuspended(transactionId)
        || this.localTxMap.containsKey(transactionId);
  }

  /**
   * The timeout after which any suspended transactions are rolled back if they are not resumed. If
   * a negative timeout is passed, suspended transactions will never expire.
   *
   * @param timeout the timeout in minutes
   */
  public void setSuspendedTransactionTimeout(long timeout) {
    this.suspendedTXTimeout = timeout;
  }

  /**
   * Return the timeout after which suspended transactions are rolled back.
   *
   * @return the timeout in minutes
   * @see #setSuspendedTransactionTimeout(long)
   */
  public long getSuspendedTransactionTimeout() {
    return this.suspendedTXTimeout;
  }

  /**
   * map to track the scheduled expiry tasks of suspended transactions.
   */
  private ConcurrentMap<TransactionId, SystemTimerTask> expiryTasks =
      new ConcurrentHashMap<TransactionId, SystemTimerTask>();

  /**
   * schedules the transaction to expire after {@link #suspendedTXTimeout}
   *
   * @param expiryTimeUnit the time unit to use when scheduling the expiration
   */
  private void scheduleExpiry(TransactionId txId, TimeUnit expiryTimeUnit) {
    if (suspendedTXTimeout < 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("TX: transaction: {} not scheduled to expire", txId);
      }
      return;
    }
    SystemTimerTask task = new TXExpiryTask(txId);
    if (logger.isDebugEnabled()) {
      logger.debug("TX: scheduling transaction: {} to expire after:{}", txId, suspendedTXTimeout);
    }
    cache.getCCPTimer().schedule(task,
        TimeUnit.MILLISECONDS.convert(suspendedTXTimeout, expiryTimeUnit));
    this.expiryTasks.put(txId, task);
  }

  /**
   * Task scheduled to expire a transaction when it is suspended. This task gets canceled if the
   * transaction is resumed.
   */
  public static class TXExpiryTask extends SystemTimerTask {

    /**
     * The txId to expire
     */
    private final TransactionId txId;

    public TXExpiryTask(TransactionId txId) {
      this.txId = txId;
    }

    @Override
    public void run2() {
      TXManagerImpl mgr = TXManagerImpl.currentInstance;
      TXStateProxy tx = mgr.suspendedTXs.remove(txId);
      if (tx != null) {
        try {
          if (logger.isDebugEnabled()) {
            logger.debug("TX: Expiry task rolling back transaction: {}", txId);
          }
          tx.rollback();
        } catch (GemFireException e) {
          logger.warn(LocalizedMessage
              .create(LocalizedStrings.TXManagerImpl_EXCEPTION_IN_TRANSACTION_TIMEOUT, txId), e);
        }
      }
    }
  }
  private static class RefCountMapEntryCreator implements
      CustomEntryConcurrentHashMap.HashEntryCreator<AbstractRegionEntry, RefCountMapEntry> {
    @Override
    public HashEntry<AbstractRegionEntry, RefCountMapEntry> newEntry(AbstractRegionEntry key,
        int hash, HashEntry<AbstractRegionEntry, RefCountMapEntry> next, RefCountMapEntry value) {
      value.setNextEntry(next);
      return value;
    }

    @Override
    public int keyHashCode(Object key, boolean compareValues) {
      // key will always be an AbstractRegionEntry because our map is strongly typed.
      return ((AbstractRegionEntry) key).getEntryHash();
    }
  }
  private static class RefCountMapEntry
      implements HashEntry<AbstractRegionEntry, RefCountMapEntry> {

    private final AbstractRegionEntry key;

    private HashEntry<AbstractRegionEntry, RefCountMapEntry> next;

    private volatile int refCount;

    private static final AtomicIntegerFieldUpdater<RefCountMapEntry> refCountUpdater =
        AtomicIntegerFieldUpdater.newUpdater(RefCountMapEntry.class, "refCount");

    public RefCountMapEntry(AbstractRegionEntry k) {
      this.key = k;
      this.refCount = 1;
    }

    @Override
    public AbstractRegionEntry getKey() {
      return this.key;
    }

    @Override
    public boolean isKeyEqual(Object k) {
      return this.key.equals(k);
    }

    @Override
    public RefCountMapEntry getMapValue() {
      return this;
    }

    @Override
    public void setMapValue(RefCountMapEntry newValue) {
      if (newValue != this) {
        throw new IllegalStateException("Expected newValue " + newValue + " to be this " + this);
      }
    }

    @Override
    public int getEntryHash() {
      return this.key.getEntryHash();
    }

    @Override
    public HashEntry<AbstractRegionEntry, RefCountMapEntry> getNextEntry() {
      return this.next;
    }

    @Override
    public void setNextEntry(HashEntry<AbstractRegionEntry, RefCountMapEntry> n) {
      this.next = n;
    }

    public void incRefCount() {
      refCountUpdater.addAndGet(this, 1);
    }

    /**
     * Returns true if refCount goes to 0.
     */
    public boolean decRefCount() {
      int rc = refCountUpdater.decrementAndGet(this);
      if (rc < 0) {
        throw new IllegalStateException("rc=" + rc);
      }
      return rc == 0;
    }
  }

  private final CustomEntryConcurrentHashMap<AbstractRegionEntry, RefCountMapEntry> refCountMap =
      new CustomEntryConcurrentHashMap<AbstractRegionEntry, RefCountMapEntry>(
          CustomEntryConcurrentHashMap.DEFAULT_INITIAL_CAPACITY,
          CustomEntryConcurrentHashMap.DEFAULT_LOAD_FACTOR,
          CustomEntryConcurrentHashMap.DEFAULT_CONCURRENCY_LEVEL, true,
          new RefCountMapEntryCreator());

  private static final MapCallback<AbstractRegionEntry, RefCountMapEntry, Object, Object> incCallback =
      new MapCallback<AbstractRegionEntry, RefCountMapEntry, Object, Object>() {
        @Override
        public RefCountMapEntry newValue(AbstractRegionEntry key, Object context,
            Object createParams) {
          return new RefCountMapEntry(key);
        }

        @Override
        public void oldValueRead(RefCountMapEntry value) {
          value.incRefCount();
        }

        @Override
        public boolean doRemoveValue(RefCountMapEntry value, Object context, Object removeParams) {
          throw new IllegalStateException("doRemoveValue should not be called from create");
        }
      };

  private static final MapCallback<AbstractRegionEntry, RefCountMapEntry, Object, Object> decCallback =
      new MapCallback<AbstractRegionEntry, RefCountMapEntry, Object, Object>() {
        @Override
        public RefCountMapEntry newValue(AbstractRegionEntry key, Object context,
            Object createParams) {
          throw new IllegalStateException("newValue should not be called from remove");
        }

        @Override
        public void oldValueRead(RefCountMapEntry value) {
          throw new IllegalStateException("oldValueRead should not be called from remove");
        }

        @Override
        public boolean doRemoveValue(RefCountMapEntry value, Object context, Object removeParams) {
          return value.decRefCount();
        }
      };

  public static void incRefCount(AbstractRegionEntry re) {
    TXManagerImpl mgr = currentInstance;
    if (mgr != null) {
      mgr.refCountMap.create(re, incCallback, null, null, true);
    }
  }

  /**
   * Return true if refCount went to zero.
   */
  public static boolean decRefCount(AbstractRegionEntry re) {
    TXManagerImpl mgr = currentInstance;
    if (mgr != null) {
      return mgr.refCountMap.removeConditionally(re, decCallback, null, null) != null;
    } else {
      return true;
    }
  }

  // Used by tests
  public Set<TXId> getLocalTxIds() {
    return this.localTxMap.keySet();
  }

  // Used by tests
  public ArrayList<TXId> getHostedTxIds() {
    synchronized (this.hostedTXStates) {
      return new ArrayList<TXId>(this.hostedTXStates.keySet());
    }
  }

  public void setTransactionTimeToLiveForTest(int seconds) {
    this.transactionTimeToLive = seconds;
  }

  /**
   * @return the time-to-live for abandoned transactions, in seconds
   */
  public int getTransactionTimeToLive() {
    return this.transactionTimeToLive;
  }

  public InternalDistributedMember getMemberId() {
    return this.distributionMgrId;
  }

  // expire the transaction states for the lost proxy server based on timeout setting.
  private void expireClientTransactionsSentFromDepartedProxy(
      InternalDistributedMember proxyServer) {
    if (this.cache.isClosed()) {
      return;
    }
    long timeout = getTransactionTimeToLive() * 1000;
    if (timeout <= 0) {
      removeTransactionsSentFromDepartedProxy(proxyServer);
    } else {
      if (departedProxyServers != null)
        departedProxyServers.add(proxyServer);
      SystemTimerTask task = new SystemTimerTask() {
        @Override
        public void run2() {
          removeTransactionsSentFromDepartedProxy(proxyServer);
          if (departedProxyServers != null)
            departedProxyServers.remove(proxyServer);
        }
      };
      try {
        this.cache.getCCPTimer().schedule(task, timeout);
      } catch (IllegalStateException ise) {
        if (!this.cache.isClosed()) {
          throw ise;
        }
        // task not able to be scheduled due to cache is closing,
        // do not set it in the test hook.
        if (departedProxyServers != null)
          departedProxyServers.remove(proxyServer);
      }
    }
  }

  private final Set<InternalDistributedMember> departedProxyServers =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "trackScheduledToBeRemovedTx")
          ? new ConcurrentHashSet<InternalDistributedMember>() : null;

  /**
   * provide a test hook to track departed peers
   */
  public Set<InternalDistributedMember> getDepartedProxyServers() {
    return departedProxyServers;
  }

  /**
   * Find all client originated transactions sent from the departed proxy server. Remove them from
   * the hostedTXStates map after the set TransactionTimeToLive period.
   *
   * @param proxyServer the departed proxy server
   */
  public void removeTransactionsSentFromDepartedProxy(InternalDistributedMember proxyServer) {
    final Set<TXId> txIds = getTransactionsSentFromDepartedProxy(proxyServer);
    if (txIds.isEmpty()) {
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("expiring the following transactions: {}", txIds);
    }
    synchronized (this.hostedTXStates) {
      Iterator<Map.Entry<TXId, TXStateProxy>> iterator = this.hostedTXStates.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<TXId, TXStateProxy> entry = iterator.next();
        if (txIds.contains(entry.getKey())) {
          // The TXState was not updated by any other proxy server,
          // The client would fail over to another proxy server.
          // Remove it after waiting for transactionTimeToLive period.
          entry.getValue().close();
          iterator.remove();
        }
      }
    }
  }

  /*
   * retrieve the transaction states for the given client from a certain proxy server. if
   * transactions failed over, the new proxy server information should be stored in the TXState
   *
   * @param id the proxy server
   *
   * @return a set of the currently open transaction states
   */
  private Set<TXId> getTransactionsSentFromDepartedProxy(InternalDistributedMember proxyServer) {
    Set<TXId> result = new HashSet<TXId>();
    synchronized (this.hostedTXStates) {
      for (Map.Entry<TXId, TXStateProxy> entry : this.hostedTXStates.entrySet()) {
        TXStateProxy tx = entry.getValue();
        if (tx.isRealDealLocal() && tx.isOnBehalfOfClient()) {
          TXState txstate = (TXState) ((TXStateProxyImpl) tx).realDeal;
          if (proxyServer.equals(txstate.getProxyServer())) {
            result.add(entry.getKey());
          }
        }
      }
    }
    return result;
  }

  public void setDistributed(boolean flag) {
    checkClosed();
    TXStateProxy tx = getTXState();
    // Check whether given flag and current flag are different and whether a transaction is in
    // progress
    if (tx != null && flag != isDistributed()) {
      // Cannot change mode in the middle of a transaction
      throw new java.lang.IllegalStateException(
          LocalizedStrings.TXManagerImpl_CANNOT_CHANGE_TRANSACTION_MODE_WHILE_TRANSACTIONS_ARE_IN_PROGRESS
              .toLocalizedString());
    } else {
      isTXDistributed.set(flag);
    }
  }

  /*
   * If explicitly set using setDistributed, this returns that value. If not, it returns the value
   * of gemfire property "distributed-transactions" if set. If this is also not set, it returns the
   * default value of this property.
   */
  public boolean isDistributed() {
    Boolean value = isTXDistributed.get();
    // This can be null if not set in setDistributed().
    if (value == null) {
      InternalDistributedSystem ids = (InternalDistributedSystem) cache.getDistributedSystem();
      return ids.getOriginalConfig().getDistributedTransactions();
    } else {
      return value;
    }
  }

  Map<TXId, TXStateProxy> getHostedTXStates() {
    return hostedTXStates;
  }

}
