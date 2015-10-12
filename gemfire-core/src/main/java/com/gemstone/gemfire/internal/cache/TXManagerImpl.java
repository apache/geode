/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache;

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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.TransactionDataRebalancedException;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.TransactionInDoubtException;
import com.gemstone.gemfire.cache.TransactionListener;
import com.gemstone.gemfire.cache.TransactionWriter;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.distributed.TXManagerCancelledException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.SystemTimer.SystemTimerTask;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap;
import com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;
import com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.MapCallback;

/** <p>The internal implementation of the {@link CacheTransactionManager}
 * interface returned by {@link GemFireCacheImpl#getCacheTransactionManager}.
 * Internal operations 

 </code>TransactionListener</code> invocation, Region synchronization, transaction statistics and

 * transaction logging are handled here 
 * 
 * @author Mitch Thomas
 *
 * @since 4.0
 * 
 * @see CacheTransactionManager
 */
public final class TXManagerImpl implements CacheTransactionManager,
    MembershipListener {

  private static final Logger logger = LogService.getLogger();
  
  // Thread specific context container
  private final ThreadLocal<TXStateProxy> txContext;
  private static TXManagerImpl currentInstance = null;
  // The unique transaction ID for this Manager 
  private final AtomicInteger uniqId;

  private final DM dm;
  private final Cache cache;

  // The DistributionMemberID used to construct TXId's
  private final InternalDistributedMember distributionMgrId;

  private final CachePerfStats cachePerfStats;

  private static final TransactionListener[] EMPTY_LISTENERS =
    new TransactionListener[0];

  /**
   * Default transaction id to indicate no transaction
   */
  public static final int NOTX = -1;
  
  private final ArrayList<TransactionListener> txListeners = new ArrayList<TransactionListener>(8);
  public TransactionWriter writer = null;
  private boolean closed = false;

  private final Map<TXId, TXStateProxy> hostedTXStates;

  /**
   * the number of client initiated transactions to store for client failover
   */
  public final static int FAILOVER_TX_MAP_SIZE = Integer.getInteger("gemfire.transactionFailoverMapSize", 1000);
  
  /**
   * used to store TXCommitMessages for client initiated transactions, so that when a client failsover,
   * (after the delegate dies) the commit message can be sent to client.
   * //TODO we really need to keep around only one msg for each thread on a client
   */
  @SuppressWarnings("unchecked")
  private Map<TXId ,TXCommitMessage> failoverMap = Collections.synchronizedMap(new LinkedHashMap<TXId, TXCommitMessage>() {
    private static final long serialVersionUID = -4156018226167594134L;

    protected boolean removeEldestEntry(Entry eldest) {
      if (logger.isDebugEnabled()) {
        logger.debug("TX: removing client initiated transaction from failover map:{} :{}", eldest.getKey(), (size()>FAILOVER_TX_MAP_SIZE));
      }
      return size() > FAILOVER_TX_MAP_SIZE;
    };
  });

  /**
   * A flag to allow persistent transactions. public for testing.
   */
  public static boolean ALLOW_PERSISTENT_TRANSACTIONS = Boolean.getBoolean("gemfire.ALLOW_PERSISTENT_TRANSACTIONS");

  /**
   * this keeps track of all the transactions that were initiated locally.
   */
  private ConcurrentMap<TXId, TXStateProxy> localTxMap = new ConcurrentHashMap<TXId, TXStateProxy>();

  /**
   * the time in minutes after which any suspended transaction are rolled back. default is 30 minutes
   */
  private volatile long suspendedTXTimeout = Long.getLong("gemfire.suspendedTxTimeout", 30);
  
  /**
   * Thread-specific flag to indicate whether the transactions managed by this
   * CacheTransactionManager for this thread should be distributed
   */
  private final ThreadLocal<Boolean> isTXDistributed;
  

  /** Constructor that implements the {@link CacheTransactionManager}
   * interface. Only only one instance per {@link com.gemstone.gemfire.cache.Cache}
   *
   * @param cachePerfStats 
   */
  public TXManagerImpl(
                       CachePerfStats cachePerfStats,
                       Cache cache) {
    this.cache = cache;
    this.dm = ((InternalDistributedSystem)cache.getDistributedSystem())
        .getDistributionManager();
    this.distributionMgrId = this.dm.getDistributionManagerId();
    this.uniqId = new AtomicInteger(0);
    this.cachePerfStats = cachePerfStats;
    this.hostedTXStates = new HashMap<TXId, TXStateProxy>();
    this.txContext = new ThreadLocal<TXStateProxy>();
    this.isTXDistributed = new ThreadLocal<Boolean>();
    currentInstance = this;
  }

  final Cache getCache() {
    return this.cache;
  }
  
  
  /**
   * Get the TransactionWriter for the cache
   * 
   * @return the current TransactionWriter
   * @see TransactionWriter
   */
  public final TransactionWriter getWriter() {
    return writer;
  }
  

  public final void setWriter(TransactionWriter writer) {
    if (((GemFireCacheImpl)this.cache).isClient()) {
      throw new IllegalStateException(LocalizedStrings.TXManager_NO_WRITER_ON_CLIENT.toLocalizedString());
    }
    this.writer = writer;
  }
  

  public final TransactionListener getListener() {
    synchronized (this.txListeners) {
      if (this.txListeners.isEmpty()) {
      return null;
      } else if (this.txListeners.size() == 1) {
        return this.txListeners.get(0);
      } else {
        throw new IllegalStateException(LocalizedStrings.TXManagerImpl_MORE_THAN_ONE_TRANSACTION_LISTENER_EXISTS.toLocalizedString());
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
      throw new IllegalArgumentException(LocalizedStrings.TXManagerImpl_ADDLISTENER_PARAMETER_WAS_NULL.toLocalizedString());
    }
    synchronized (this.txListeners) {
      if (!this.txListeners.contains(aListener)) {
        this.txListeners.add(aListener);
      }
    }
  }
  public void removeListener(TransactionListener aListener) {
    if (aListener == null) {
      throw new IllegalArgumentException(LocalizedStrings.TXManagerImpl_REMOVELISTENER_PARAMETER_WAS_NULL.toLocalizedString());
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
          throw new IllegalArgumentException(LocalizedStrings.TXManagerImpl_INITLISTENERS_PARAMETER_HAD_A_NULL_ELEMENT.toLocalizedString());
        }
        this.txListeners.addAll(nl);
      }
    }
  }

  final CachePerfStats getCachePerfStats() {
    return this.cachePerfStats;
  }

  /** Build a new {@link TXId}, use it as part of the transaction
   * state and associate with the current thread using a {@link
   * ThreadLocal}.
   */
  public void begin() {
    checkClosed();
    {
      TransactionId tid = getTransactionId();
      if (tid != null) {
        throw new java.lang.IllegalStateException(LocalizedStrings.TXManagerImpl_TRANSACTION_0_ALREADY_IN_PROGRESS.toLocalizedString(tid));
      }
    }
    TXId id = new TXId(this.distributionMgrId, this.uniqId.incrementAndGet());
    TXStateProxyImpl proxy = null;
    if (isDistributed()) {
      proxy = new DistTXStateProxyImplOnCoordinator(this, id, null);  
    } else {
      proxy = new TXStateProxyImpl(this, id, null);  
    }
    setTXState(proxy);
    this.localTxMap.put(id, proxy);
  }


  /** Build a new {@link TXId}, use it as part of the transaction
   * state and associate with the current thread using a {@link
   * ThreadLocal}. Flag the transaction to be enlisted with a JTA
   * Transaction.  Should only be called in a context where we know
   * there is no existing transaction.
   */
  public TXStateProxy beginJTA() {
    checkClosed();
    TXId id = new TXId(this.distributionMgrId, this.uniqId.incrementAndGet());
    TXStateProxy newState = null;
    
    if (isDistributed()) {
      newState = new DistTXStateProxyImplOnCoordinator(this, id, true);
    } else {
      newState = new TXStateProxyImpl(this, id, true);
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
      throw new IllegalStateException(LocalizedStrings.TXManagerImpl_THREAD_DOES_NOT_HAVE_AN_ACTIVE_TRANSACTION.toLocalizedString());
    }
    
    tx.checkJTA(LocalizedStrings.TXManagerImpl_CAN_NOT_COMMIT_THIS_TRANSACTION_BECAUSE_IT_IS_ENLISTED_WITH_A_JTA_TRANSACTION_USE_THE_JTA_MANAGER_TO_PERFORM_THE_COMMIT.toLocalizedString());
  
    tx.precommit();
  }
  
  /** Complete the transaction associated with the current
   *  thread. When this method completes, the thread is no longer
   *  associated with a transaction.
   *
   */
  public void commit() throws CommitConflictException {
    checkClosed();

    final TXStateProxy tx = getTXState();
    if (tx == null) {
      throw new IllegalStateException(LocalizedStrings.TXManagerImpl_THREAD_DOES_NOT_HAVE_AN_ACTIVE_TRANSACTION.toLocalizedString());
    }

    tx.checkJTA(LocalizedStrings.TXManagerImpl_CAN_NOT_COMMIT_THIS_TRANSACTION_BECAUSE_IT_IS_ENLISTED_WITH_A_JTA_TRANSACTION_USE_THE_JTA_MANAGER_TO_PERFORM_THE_COMMIT.toLocalizedString());

    final long opStart = CachePerfStats.getStatTime();
    final long lifeTime = opStart - tx.getBeginTime();
    try {
      setTXState(null);
      tx.commit();
    } catch (CommitConflictException ex) {
      saveTXStateForClientFailover(tx, TXCommitMessage.CMT_CONFLICT_MSG); //fixes #43350
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

  final void noteCommitFailure(long opStart, long lifeTime, TXStateInterface tx) {
    long opEnd = CachePerfStats.getStatTime();
    this.cachePerfStats.txFailure(opEnd - opStart,
                                  lifeTime, tx.getChanges());
    TransactionListener[] listeners = getListeners();
    if (tx.isFireCallbacks() && listeners.length > 0) {
      final TXEvent e = tx.getEvent();
      try {
      for (int i=0; i < listeners.length; i++) {
        try {
          listeners[i].afterFailedCommit(e);
        } 
        catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.error(LocalizedMessage.create(LocalizedStrings.TXManagerImpl_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER), t);
        }
      }
      } finally {
        e.release();
      }
    }
  }

  final void noteCommitSuccess(long opStart, long lifeTime, TXStateInterface tx) {
    long opEnd = CachePerfStats.getStatTime();
    this.cachePerfStats.txSuccess(opEnd - opStart,
                                  lifeTime, tx.getChanges());
    TransactionListener[] listeners = getListeners();
    if (tx.isFireCallbacks() && listeners.length > 0) {
      final TXEvent e = tx.getEvent();
      try {
      for (final TransactionListener listener : listeners) {
        try {
          listener.afterCommit(e);
        } 
        catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.error(LocalizedMessage.create(LocalizedStrings.TXManagerImpl_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER), t);
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
    TXStateProxyImpl tx = (TXStateProxyImpl)getTXState();
    assert tx != null : "expected a transaction to be in progress";
    TXId id = new TXId(this.distributionMgrId, this.uniqId.incrementAndGet());
    tx.setTXIDForReplay(id);
  }


  /** Roll back the transaction associated with the current
   *  thread. When this method completes, the thread is no longer
   *  associated with a transaction.
   */
  public void rollback() {
    checkClosed();
    TXStateProxy tx = getTXState();
    if (tx == null) {
      throw new IllegalStateException(LocalizedStrings.TXManagerImpl_THREAD_DOES_NOT_HAVE_AN_ACTIVE_TRANSACTION.toLocalizedString());
    }

    tx.checkJTA(LocalizedStrings.TXManagerImpl_CAN_NOT_ROLLBACK_THIS_TRANSACTION_IS_ENLISTED_WITH_A_JTA_TRANSACTION_USE_THE_JTA_MANAGER_TO_PERFORM_THE_ROLLBACK.toLocalizedString());

    final long opStart = CachePerfStats.getStatTime();
    final long lifeTime = opStart - tx.getBeginTime();
    setTXState(null);
    tx.rollback();
    saveTXStateForClientFailover(tx);
    cleanup(tx.getTransactionId());
    noteRollbackSuccess(opStart, lifeTime, tx);
  }
  
  final void noteRollbackSuccess(long opStart, long lifeTime, TXStateInterface tx) {
    long opEnd = CachePerfStats.getStatTime();
    this.cachePerfStats.txRollback(opEnd - opStart,
                                   lifeTime, tx.getChanges());
    TransactionListener[] listeners = getListeners();
    if (tx.isFireCallbacks() && listeners.length > 0) {
      final TXEvent e = tx.getEvent();
      try {
      for (int i = 0; i < listeners.length; i++) {
        try {
          listeners[i].afterRollback(e);
        } 
        catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.error(LocalizedMessage.create(LocalizedStrings.TXManagerImpl_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER), t);
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
  
  /** Reports the existance of a Transaction for this thread
   *
   */
  public boolean exists() {
    return null != getTXState();
  }

  /** Gets the current transaction identifier or null if no transaction exists
   *
   */
  public TransactionId getTransactionId() {
    TXStateProxy t = getTXState();
    TransactionId ret = null;
    if (t!=null) {
      ret = t.getTransactionId();
    } 
    return ret;
  }

  /** 
   * Returns the TXStateProxyInterface of the current thread; null if no transaction.
   */
  public final TXStateProxy getTXState() {
    TXStateProxy tsp = txContext.get();
    if (tsp != null && !tsp.isInProgress()) {
      this.txContext.set(null);
      tsp = null;
    }
    return tsp;
  }

  /**
   * sets {@link TXStateProxy#setInProgress(boolean)} when a txContext is present.
   * This method must only be used in fail-over scenarios.
   * @param progress value of the progress flag to be set
   * @return the previous value of inProgress flag
   * @see TXStateProxy#setInProgress(boolean)
   */
  public boolean setInProgress(boolean progress) {
    boolean retVal = false;
    TXStateProxy tsp = txContext.get();
    if (tsp != null) {
      retVal = tsp.isInProgress();
      tsp.setInProgress(progress);
    }
    return retVal;
  }

  public final void setTXState(TXStateProxy val) {
    txContext.set(val);
  }


  public void close() {
    if (isClosed()) {
      return;
    }
    this.closed = true;
    for (TXStateProxy proxy: this.hostedTXStates.values()) {
      proxy.close();
    }
    for (TXStateProxy proxy: this.localTxMap.values()) {
      proxy.close();
    }
    {
      TransactionListener[] listeners = getListeners();
      for (int i=0; i < listeners.length; i++) {
        closeListener(listeners[i]);
      }
    }
  }
  private void closeListener(TransactionListener tl) {
    try {
      tl.close();
    } 
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(LocalizedMessage.create(LocalizedStrings.TXManagerImpl_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER), t);
    }
  }
  /**
   * If the current thread is in a transaction then suspend will
   * cause it to no longer be in a transaction.
   * @return the state of the transaction or null. Pass this value
   *  to {@link TXManagerImpl#resume} to reactivate the suspended transaction.
   */
  public final TXStateProxy internalSuspend() {
    TXStateProxy result = getTXState();
    if (result != null) {
      result.suspend();
      setTXState(null);
    }
    return result;
  }
  /**
   * Activates the specified transaction on the calling thread.
   * @param tx the transaction to activate.
   * @throws IllegalStateException if this thread already has an active transaction
   */
  public final void resume(TXStateProxy tx) {
    if (tx != null) {
      TransactionId tid = getTransactionId();
      if (tid != null) {
        throw new java.lang.IllegalStateException(LocalizedStrings.TXManagerImpl_TRANSACTION_0_ALREADY_IN_PROGRESS.toLocalizedString(tid));
      }
      if (tx instanceof TXState) {
        throw new java.lang.IllegalStateException("Found instance of TXState: " + tx);
      }
      setTXState(tx);
      tx.resume();
      SystemTimerTask task = this.expiryTasks.remove(tx.getTransactionId());
      if (task != null) {
        task.cancel();
      }
    }
  }

  private final boolean isClosed() {
    return this.closed;
  }
  private final void checkClosed() {
    cache.getCancelCriterion().checkCancelInProgress(null);
    if (this.closed) {
      throw new TXManagerCancelledException("This transaction manager is closed.");
    }
  }

  final DM getDM() {
    return this.dm;
  }

  
  public static int getCurrentTXUniqueId() {
    if(currentInstance==null) {
      return NOTX;
    }
    return currentInstance.getMyTXUniqueId();
  }
  
  
  
  
  public final static TXStateProxy getCurrentTXState() {
    if(currentInstance==null) {
      return null;
    }
    return currentInstance.getTXState();
  }
  
  public static void incrementTXUniqueIDForReplay() {
    if(currentInstance != null) {
      currentInstance._incrementTXUniqueIDForReplay();
    }
  }
  
  public int getMyTXUniqueId() {
    TXStateProxy t = txContext.get();
    if (t != null) {
      return t.getTxId().getUniqId();
    } else {
      return NOTX;
    }
  }

  /**
   * Associate the remote txState with the thread processing this message. Also,
   * we acquire a lock on the txState, on which this thread operates.
   * Some messages like SizeMessage should not create a new txState.
   * @param msg
   * @return {@link TXStateProxy} the txProxy for the transactional message
   * @throws InterruptedException 
   */
  public TXStateProxy masqueradeAs(TransactionMessage msg) throws InterruptedException {
    if (msg.getTXUniqId() == NOTX || !msg.canParticipateInTransaction()) {
      return null;
    }
    TXId key = new TXId(msg.getMemberToMasqueradeAs(), msg.getTXUniqId());
    TXStateProxy val;
    val = this.hostedTXStates.get(key);
    if (val == null) {
      synchronized(this.hostedTXStates) {
        val = this.hostedTXStates.get(key);
        if (val == null && msg.canStartRemoteTransaction()) {
          if (msg.isTransactionDistributed()) {
            val = new DistTXStateProxyImplOnDatanode(this, key, msg.getTXOriginatorClient());
            val.setLocalTXState(new DistTXState(val,true));
          } else {
            val = new TXStateProxyImpl(this, key, msg.getTXOriginatorClient());
            val.setLocalTXState(new TXState(val,true));
          }
          this.hostedTXStates.put(key, val);
        }
      }
    }
    if (val != null) {
      if (!val.getLock().isHeldByCurrentThread()) {
        val.getLock().lock();
      }
    }

    setTXState(val);
    return val;
  }
  
  /**
   * Associate the remote txState with the thread processing this message. Also,
   * we acquire a lock on the txState, on which this thread operates.
   * Some messages like SizeMessage should not create a new txState.
   * @param msg
   * @param memberId
   * @param probeOnly - do not masquerade; just look up the TX state
   * @return {@link TXStateProxy} the txProxy for the transactional message
   * @throws InterruptedException 
   */
  public TXStateProxy masqueradeAs(Message msg,InternalDistributedMember memberId, boolean probeOnly) throws InterruptedException {
    if (msg.getTransactionId() == NOTX) {
      return null;
    }
    TXId key = new TXId(memberId, msg.getTransactionId());
    TXStateProxy val;
    val = this.hostedTXStates.get(key);
    if (val == null) {
      synchronized(this.hostedTXStates) {
        val = this.hostedTXStates.get(key);
        if (val == null && msg.canStartRemoteTransaction()) {
          // [sjigyasu] TODO: Conditionally create object based on distributed or non-distributed tx mode 
          if (msg instanceof TransactionMessage && ((TransactionMessage)msg).isTransactionDistributed()) {
            val = new DistTXStateProxyImplOnDatanode(this, key, memberId);
            //val.setLocalTXState(new DistTXState(val,true));
          } else {
            val = new TXStateProxyImpl(this, key, memberId);
            //val.setLocalTXState(new TXState(val,true));
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
   * @param tx
   */
  public void unmasquerade(TXStateProxy tx) {
    if (tx != null) {
      setTXState(null);
      tx.getLock().unlock();
    }
  }

  /**
   * Cleanup the remote txState after commit and rollback
   * @param txId  
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
   * Called when the CacheServer is shutdown.
   * Removes txStates hosted on client's behalf
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
   * @param txId
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

  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    synchronized (this.hostedTXStates) {
      Iterator<Map.Entry<TXId,TXStateProxy>> iterator = this.hostedTXStates.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<TXId,TXStateProxy> me = iterator.next();
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
  }

  public void memberJoined(InternalDistributedMember id) {
  }

  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
  }

  public void memberSuspect(InternalDistributedMember id,
      InternalDistributedMember whoSuspected) {
  }
  

  /**
   * retrieve the transaction states for the given client
   * @param id the client's membership ID
   * @return a set of the currently open transaction states
   */
  public Set<TXId> getTransactionsForClient(InternalDistributedMember id) {
    Set<TXId> result = new HashSet<TXId>();
    synchronized (this.hostedTXStates) {
      for (Map.Entry<TXId, TXStateProxy> entry: this.hostedTXStates.entrySet()) {
        if (entry.getKey().getMemberId().equals(id)) {
          result.add(entry.getKey());
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
        Map.Entry<TXId,TXStateProxy> entry = iterator.next();
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

  private void saveTXStateForClientFailover(TXStateProxy tx) {
    if (tx.isOnBehalfOfClient() && tx.isRealDealLocal()) {
      failoverMap.put(tx.getTxId(), tx.getCommitMessage());
      if (logger.isDebugEnabled()) {
        logger.debug("TX: storing client initiated transaction:{}; now there are {} entries in the failoverMap",
            tx.getTxId(), failoverMap.size());
      }
    }
  }

  private void saveTXStateForClientFailover(TXStateProxy tx, TXCommitMessage msg) {
    if (tx.isOnBehalfOfClient() && tx.isRealDealLocal()) {
      failoverMap.put(tx.getTxId(), msg);
      if (logger.isDebugEnabled()) {
        logger.debug("TX: storing client initiated transaction:{}; now there are {} entries in the failoverMap",
            tx.getTxId(), failoverMap.size());
      }
    }
  }

  public void saveTXCommitMessageForClientFailover(TXId txId, TXCommitMessage msg) {
    failoverMap.put(txId, msg);
  }
  
  public boolean isHostedTxRecentlyCompleted(TXId txId) {
    // if someone is asking to see if we have the txId, they will come
    // back and ask for the commit message, this could take a long time
    // specially when called from TXFailoverCommand, so we move
    // the txId to the front of the queue
    TXCommitMessage msg = failoverMap.remove(txId);
    if (msg != null) {
      failoverMap.put(txId, msg);
      return true;
    }
    return false;
  }
  
  
  /**
   * If the given transaction is already being completed by another thread
   * this will wait for that completion to finish and will ensure that
   * the result is saved in the client failover map.
   * @param txId
   * @return true if a wait was performed
   */
  public boolean waitForCompletingTransaction(TXId txId) {
    TXStateProxy val;
    val = this.hostedTXStates.get(txId);
    if (val == null) {
      synchronized(this.hostedTXStates) {
        val = this.hostedTXStates.get(txId);
      }
    }
    if (val != null && val.isRealDealLocal()) {
      TXStateProxyImpl impl = (TXStateProxyImpl)val;
      TXState state = impl.getLocalRealDeal();
      if (state.waitForPreviousCompletion()) {
        // the thread we were waiting for would have put a TXCommitMessage
        // in the failover map, doing so here may replace an existing token
        // like TXCommitMessage.REBALANCE_MSG with null. fixes bug 42661
        //saveTXStateForClientFailover(impl);
        return true;
      }
    }
    return false;
  }
  
  /**
   * Returns the TXCommitMessage for a transaction that has been
   * successfully completed.
   * @param txId
   * @return the commit message or an exception token e.g 
   * {@link TXCommitMessage#CMT_CONFLICT_MSG} if the transaction
   * threw an exception
   * @see #isExceptionToken(TXCommitMessage)
   */
  public TXCommitMessage getRecentlyCompletedMessage(TXId txId) {
    return failoverMap.get(txId);
  }

  /**
   * @param msg
   * @return true if msg is an exception token, false otherwise
   */
  public boolean isExceptionToken(TXCommitMessage msg) {
    if (msg == TXCommitMessage.CMT_CONFLICT_MSG
        || msg == TXCommitMessage.REBALANCE_MSG
        || msg == TXCommitMessage.EXCEPTION_MSG) {
      return true;
    }
    return false;
  }
  
  /**
   * Generates exception messages for the three TXCommitMessage tokens that represent
   * exceptions during transaction execution. 
   * @param msg the token that represents the exception
   * @param txId
   * @return the exception
   */
  public RuntimeException getExceptionForToken(TXCommitMessage msg, TXId txId) {
    if (msg == TXCommitMessage.CMT_CONFLICT_MSG) {
      return new CommitConflictException(LocalizedStrings.
            TXState_CONFLICT_DETECTED_IN_GEMFIRE_TRANSACTION_0.toLocalizedString(txId));
    }
    if (msg == TXCommitMessage.REBALANCE_MSG) {
      return new TransactionDataRebalancedException(LocalizedStrings.
          PartitionedRegion_TRANSACTIONAL_DATA_MOVED_DUE_TO_REBALANCING.toLocalizedString());
    }
    if (msg == TXCommitMessage.EXCEPTION_MSG) {
      return new TransactionInDoubtException(LocalizedStrings.
          ClientTXStateStub_COMMIT_FAILED_ON_SERVER.toLocalizedString());
    }
    throw new InternalGemFireError("the parameter TXCommitMessage is not an exception token");
  }
  
  public static class TXRemovalMessage extends HighPriorityDistributionMessage {

    Set<TXId> txIds;

    /** for deserialization */
    public TXRemovalMessage() {
    }

    static void send(DM dm, Set recipients, Set<TXId> txIds) {
      TXRemovalMessage msg = new TXRemovalMessage();
      msg.txIds = txIds;
      msg.setRecipients(recipients);
      dm.putOutgoing(msg);
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeHashSet((HashSet<TXId>)this.txIds, out);
    }
    
    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.txIds = DataSerializer.readHashSet(in);
    }

    public int getDSFID() {
      return TX_MANAGER_REMOVE_TRANSACTIONS;
    }

    @Override
    protected void process(DistributionManager dm) {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache != null) {
        TXManagerImpl mgr = cache.getTXMgr();
        mgr.removeTransactions(this.txIds, false);
      }
    }
    
  }

  private ConcurrentMap<TransactionId, TXStateProxy> suspendedTXs = new ConcurrentHashMap<TransactionId, TXStateProxy>();
  
  public TransactionId suspend() {
    return suspend(TimeUnit.MINUTES);
  }
  
  TransactionId suspend(TimeUnit expiryTimeUnit) {
    TXStateProxy result = getTXState();
    if (result != null) {
      TransactionId txId = result.getTransactionId();
      internalSuspend();
      this.suspendedTXs.put(txId, result);
      // wake up waiting threads
      Queue<Thread> waitingThreads = this.waitMap.get(txId);
      if (waitingThreads != null) {
        Thread waitingThread = null;
        while (true) {
          waitingThread = waitingThreads.poll();
          if (waitingThread == null
              || !Thread.currentThread().equals(waitingThread)) {
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
          LocalizedStrings.TXManagerImpl_UNKNOWN_TRANSACTION_OR_RESUMED
              .toLocalizedString());
    }
    if (getTXState() != null) {
      throw new IllegalStateException(
          LocalizedStrings.TXManagerImpl_TRANSACTION_ACTIVE_CANNOT_RESUME
              .toLocalizedString());
    }
    TXStateProxy txProxy = this.suspendedTXs.remove(transactionId);
    if (txProxy == null) {
      throw new IllegalStateException(
          LocalizedStrings.TXManagerImpl_UNKNOWN_TRANSACTION_OR_RESUMED
              .toLocalizedString());
    }
    resume(txProxy);
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
      resume(txProxy);
      return true;
    }
    return false;
  }

  /**
   * this map keeps track of all the threads that are waiting in
   * {@link #tryResume(TransactionId, long, TimeUnit)} for a particular
   * transactionId
   */
  private ConcurrentMap<TransactionId, Queue<Thread>> waitMap = new ConcurrentHashMap<TransactionId, Queue<Thread>>();
  
  public boolean tryResume(TransactionId transactionId, long time, TimeUnit unit) {
    if (transactionId == null || getTXState() != null || !exists(transactionId)) {
      return false;
    }
    Thread currentThread = Thread.currentThread();
    long timeout = unit.toNanos(time);
    long startTime = System.nanoTime();
    Queue<Thread> threadq = null;

    try {
      while (true) {
        threadq = waitMap.get(transactionId);
        if (threadq == null) {
          threadq = new ConcurrentLinkedQueue<Thread>();
          Queue<Thread> oldq = waitMap.putIfAbsent(transactionId, threadq);
          if (oldq != null) {
            threadq = oldq;
          }
        }
        threadq.add(currentThread);
        // after putting this thread in waitMap, we should check for
        // an entry in suspendedTXs. if no entry is found in suspendedTXs
        // next invocation of suspend() will unblock this thread
        if (tryResume(transactionId)) {
          return true;
        } else if (!exists(transactionId)) {
          return false;
        }
        LockSupport.parkNanos(timeout);
        long nowTime = System.nanoTime();
        timeout -= nowTime - startTime;
        startTime = nowTime;
        if (timeout <= 0) {
          break;
        }
      }
    } finally {
      threadq = waitMap.get(transactionId);
      if (threadq != null) {
        threadq.remove(currentThread);
        // the queue itself will be removed at commit/rollback
      }
    }
    return false;
  }

  public boolean exists(TransactionId transactionId) {
    return isHostedTxInProgress((TXId) transactionId)
        || isSuspended(transactionId)
        || this.localTxMap.containsKey(transactionId);
  }

  /**
   * The timeout after which any suspended transactions are
   * rolled back if they are not resumed. If a negative
   * timeout is passed, suspended transactions will never expire.
   * @param timeout the timeout in minutes
   */
  public void setSuspendedTransactionTimeout(long timeout) {
    this.suspendedTXTimeout = timeout;
  }

  /**
   * Return the timeout after which suspended transactions
   * are rolled back.
   * @return the timeout in minutes
   * @see #setSuspendedTransactionTimeout(long)
   */
  public long getSuspendedTransactionTimeout() {
    return this.suspendedTXTimeout;
  }
  
  /**
   * map to track the scheduled expiry tasks of suspended transactions.
   */
  private ConcurrentMap<TransactionId, SystemTimerTask> expiryTasks = new ConcurrentHashMap<TransactionId, SystemTimerTask>();
  
  /**
   * schedules the transaction to expire after {@link #suspendedTXTimeout}
   * @param txId
   * @param expiryTimeUnit the time unit to use when scheduling the expiration
   */
  private void scheduleExpiry(TransactionId txId, TimeUnit expiryTimeUnit) {
    final GemFireCacheImpl cache = (GemFireCacheImpl) this.cache;
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
    cache.getCCPTimer().schedule(task, TimeUnit.MILLISECONDS.convert(suspendedTXTimeout, expiryTimeUnit));
    this.expiryTasks.put(txId, task);
  }

  /**
   * Task scheduled to expire a transaction when it is suspended.
   * This task gets canceled if the transaction is resumed.
   * @author sbawaska
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
          logger.warn(LocalizedMessage.create(LocalizedStrings.TXManagerImpl_EXCEPTION_IN_TRANSACTION_TIMEOUT, txId), e);
        }
      }
    }
  }
  private static class RefCountMapEntryCreator implements CustomEntryConcurrentHashMap.HashEntryCreator<AbstractRegionEntry, RefCountMapEntry> {
    @Override
    public HashEntry<AbstractRegionEntry, RefCountMapEntry> newEntry(AbstractRegionEntry key, int hash,
        HashEntry<AbstractRegionEntry, RefCountMapEntry> next, RefCountMapEntry value) {
      value.setNextEntry(next);
      return value;
    }

    @Override
    public int keyHashCode(Object key, boolean compareValues) {
      // key will always be an AbstractRegionEntry because our map is strongly typed.
      return ((AbstractRegionEntry) key).getEntryHash();
    }
  }
  private static class RefCountMapEntry implements HashEntry<AbstractRegionEntry, RefCountMapEntry> {
    private final AbstractRegionEntry key;
    private HashEntry<AbstractRegionEntry, RefCountMapEntry> next;
    private volatile int refCount;
    private static final AtomicIntegerFieldUpdater<RefCountMapEntry> refCountUpdater
      = AtomicIntegerFieldUpdater.newUpdater(RefCountMapEntry.class, "refCount");    
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
  
  private final CustomEntryConcurrentHashMap<AbstractRegionEntry, RefCountMapEntry> refCountMap
    = new CustomEntryConcurrentHashMap<AbstractRegionEntry, RefCountMapEntry>(
        CustomEntryConcurrentHashMap.DEFAULT_INITIAL_CAPACITY, 
        CustomEntryConcurrentHashMap.DEFAULT_LOAD_FACTOR,
        CustomEntryConcurrentHashMap.DEFAULT_CONCURRENCY_LEVEL,
        true,
        new RefCountMapEntryCreator());
  
  private static final MapCallback<AbstractRegionEntry, RefCountMapEntry, Object, Object> incCallback = new MapCallback<AbstractRegionEntry, RefCountMapEntry, Object, Object>() {
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
    public boolean doRemoveValue(RefCountMapEntry value, Object context,
        Object removeParams) {
      throw new IllegalStateException("doRemoveValue should not be called from create");
    }
  };
  
  private static final MapCallback<AbstractRegionEntry, RefCountMapEntry, Object, Object> decCallback = new MapCallback<AbstractRegionEntry, RefCountMapEntry, Object, Object>() {
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
    public boolean doRemoveValue(RefCountMapEntry value, Object context,
        Object removeParams) {
      return value.decRefCount();
    }
  };
  
  public static final void incRefCount(AbstractRegionEntry re) {
    TXManagerImpl mgr = currentInstance;
    if (mgr != null) {
      mgr.refCountMap.create(re, incCallback, null, null, true);
    }
  }
  /**
   * Return true if refCount went to zero.
   */
  public static final boolean decRefCount(AbstractRegionEntry re) {
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
  
  public void setDistributed(boolean flag) {
    checkClosed();
    TXStateProxy tx = getTXState();
    // Check whether given flag and current flag are different and whether a transaction is in progress
    if (tx != null && flag != isDistributed()) {
      // Cannot change mode in the middle of a transaction
      throw new java.lang.IllegalStateException(
          LocalizedStrings.TXManagerImpl_CANNOT_CHANGE_TRANSACTION_MODE_WHILE_TRANSACTIONS_ARE_IN_PROGRESS
              .toLocalizedString());
    } else {
      isTXDistributed.set(new Boolean(flag));
    }
  }

  /*
   * If explicitly set using setDistributed, this returns that value.
   * If not, it returns the value of gemfire property "distributed-transactions" if set.
   * If this is also not set, it returns the default value of this property.
   */
  public boolean isDistributed() {
    
     Boolean value = isTXDistributed.get();
    // This can be null if not set in setDistributed().
    if (value == null) {
      return InternalDistributedSystem.getAnyInstance().getOriginalConfig().getDistributedTransactions();
    } else {
      return value.booleanValue();
    }
  }
  
}
