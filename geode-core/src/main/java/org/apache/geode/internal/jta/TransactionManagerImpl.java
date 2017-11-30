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
package org.apache.geode.internal.jta;

/**
 * <p>
 * TransactionManager: A JTA compatible Transaction Manager.
 * </p>
 *
 * @since GemFire 4.1.1
 *
 * @deprecated as of Geode 1.2.0 user should use a third party JTA transaction manager instead.
 */
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;

@Deprecated
public class TransactionManagerImpl implements TransactionManager, Serializable {
  private static final long serialVersionUID = 5033392316185449821L;

  private static final Logger logger = LogService.getLogger();
  /**
   * A mapping of Thread - Transaction Objects
   */
  private Map transactionMap = new ConcurrentHashMap();
  /**
   * A mapping of Transaction - Global Transaction
   */
  private Map globalTransactionMap = Collections.synchronizedMap(new HashMap());
  /**
   * Ordered set of active global transactions - Used for timeOut
   */
  protected SortedSet gtxSet =
      Collections.synchronizedSortedSet(new TreeSet(new GlobalTransactionComparator()));
  /**
   * Transaction TimeOut Class
   */
  private transient TransactionTimeOutThread cleaner;
  /**
   * Singleton transactionManager
   */
  private static TransactionManagerImpl transactionManager = null;
  /**
   * Transaction TimeOut thread
   */
  private transient Thread cleanUpThread = null;
  /**
   * Default Transaction Time Out
   */
  static final int DEFAULT_TRANSACTION_TIMEOUT =
      Integer.getInteger("jta.defaultTimeout", 600).intValue();
  /**
   * Asif: The integers identifying the cause of Rollback
   */
  private static final int MARKED_ROLLBACK = 1;
  private static final int EXCEPTION_IN_NOTIFY_BEFORE_COMPLETION = 2;
  private static final int COMMIT_FAILED_SO_ROLLEDBAK = 3;
  private static final int COMMIT_FAILED_ROLLBAK_ALSO_FAILED = 4;
  private static final int ROLLBAK_FAILED = 5;
  // TODO:Asif .Not yet used this exception code
  // private static final int EXCEPTION_IN_NOTIFY_AFTER_COMPLETION = 6;
  /*
   * to enable VERBOSE = true pass System parameter jta.VERBOSE = true while running the test.
   */
  private static boolean VERBOSE = Boolean.getBoolean("jta.VERBOSE");
  /*
   * checks if the TransactionManager is active
   */
  private boolean isActive = true;

  private final transient AtomicBoolean loggedJTATransactionManagerDeprecatedWarning =
      new AtomicBoolean(false);

  /**
   * Constructs a new TransactionManagerImpl
   */
  private TransactionManagerImpl() {
    cleaner = this.new TransactionTimeOutThread();
    ThreadGroup group = LoggingThreadGroup.createThreadGroup(
        LocalizedStrings.TransactionManagerImpl_CLEAN_UP_THREADS.toLocalizedString());
    cleanUpThread = new Thread(group, cleaner, "GlobalTXTimeoutMonitor");
    cleanUpThread.setDaemon(true);
    cleanUpThread.start();
  }

  /**
   * Returns the singleton TransactionManagerImpl Object
   */
  public static TransactionManagerImpl getTransactionManager() {
    if (transactionManager == null) {
      createTransactionManager();
    }
    return transactionManager;
  }

  /**
   * Creates an instance of TransactionManagerImpl if none exists
   */
  private static synchronized void createTransactionManager() {
    if (transactionManager == null)
      transactionManager = new TransactionManagerImpl();
  }

  /**
   * Create a new transaction and associate it with the current thread if none exists with the
   * current thread else throw an exception since nested transactions are not supported
   *
   * Create a global transaction and associate the transaction created with the global transaction
   *
   * @throws NotSupportedException - Thrown if the thread is already associated with a transaction
   *         and the Transaction Manager implementation does not support nested transactions.
   * @throws SystemException - Thrown if the transaction manager encounters an unexpected error
   *         condition.
   *
   * @see javax.transaction.TransactionManager#begin()
   */
  public void begin() throws NotSupportedException, SystemException {
    if (loggedJTATransactionManagerDeprecatedWarning.compareAndSet(false, true)) {
      logger.warn(
          "Geode JTA transaction manager is deprecated since 1.2.0, please use a third party JTA transaction manager instead");
    }
    if (!isActive) {
      throw new SystemException(
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGER_INVALID.toLocalizedString());
    }
    LogWriterI18n log = TransactionUtils.getLogWriterI18n();
    if (log.fineEnabled()) {
      log.fine("TransactionManager.begin() invoked");
    }
    Thread thread = Thread.currentThread();
    if (transactionMap.get(thread) != null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_BEGIN_NESTED_TRANSACTION_IS_NOT_SUPPORTED
              .toLocalizedString();
      if (VERBOSE)
        log.fine(exception);
      throw new NotSupportedException(exception);
    }
    try {
      TransactionImpl transaction = new TransactionImpl();
      transactionMap.put(thread, transaction);
      GlobalTransaction globalTransaction = new GlobalTransaction();
      globalTransactionMap.put(transaction, globalTransaction);
      globalTransaction.addTransaction(transaction);
      globalTransaction.setStatus(Status.STATUS_ACTIVE);
    } catch (Exception e) {
      String exception = LocalizedStrings.TransactionManagerImpl_BEGIN__SYSTEMEXCEPTION_DUE_TO_0
          .toLocalizedString(new Object[] {e});
      if (log.severeEnabled())
        log.severe(LocalizedStrings.TransactionManagerImpl_BEGIN__SYSTEMEXCEPTION_DUE_TO_0,
            new Object[] {e});
      throw new SystemException(exception);
    }
  }

  /**
   * Complete the transaction associated with the current thread by calling the
   * GlobalTransaction.commit(). When this method completes, the thread is no longer associated with
   * a transaction.
   *
   * @throws RollbackException - Thrown to indicate that the transaction has been rolled back rather
   *         than committed.
   * @throws HeuristicMixedException - Thrown to indicate that a heuristic decision was made and
   *         that some relevant updates have been committed while others have been rolled back.
   * @throws HeuristicRollbackException - Thrown to indicate that a heuristic decision was made and
   *         that all relevant updates have been rolled back.
   * @throws java.lang.SecurityException - Thrown to indicate that the thread is not allowed to
   *         commit the transaction.
   * @throws java.lang.IllegalStateException - Thrown if the current thread is not associated with a
   *         transaction.
   * @throws SystemException - Thrown if the transaction manager encounters an unexpected error
   *         condition.
   *
   * @see javax.transaction.TransactionManager#commit()
   */
  public void commit() throws HeuristicRollbackException, RollbackException,
      HeuristicMixedException, SystemException {
    if (!isActive) {
      throw new SystemException(
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGER_INVALID.toLocalizedString());
    }
    int cozOfException = -1;
    Transaction transactionImpl = getTransaction();
    if (transactionImpl == null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_IS_NULL_CANNOT_COMMIT_A_NULL_TRANSACTION
              .toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE)
        writer.fine(exception);
      throw new IllegalStateException(exception);
    }
    GlobalTransaction gtx = getGlobalTransaction(transactionImpl);
    if (gtx == null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_GLOBAL_TRANSACTION_IS_NULL_CANNOT_COMMIT_A_NULL_GLOBAL_TRANSACTION
              .toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE)
        writer.fine(exception);
      throw new SystemException(exception);
    }
    boolean isCommit = false;
    // ensure only one thread can commit. Use a synchronized block
    // Asif
    int status = -1;
    if (((status = gtx.getStatus()) == Status.STATUS_ACTIVE)
        || status == Status.STATUS_MARKED_ROLLBACK) {
      synchronized (gtx) {
        if ((status = gtx.getStatus()) == Status.STATUS_ACTIVE) {
          gtx.setStatus(Status.STATUS_COMMITTING);
          isCommit = true;
        } else if (status == Status.STATUS_MARKED_ROLLBACK) {
          gtx.setStatus(Status.STATUS_ROLLING_BACK);
          cozOfException = MARKED_ROLLBACK;
        } else {
          String exception =
              LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_NOT_ACTIVE_CANNOT_BE_COMMITTED_TRANSACTION_STATUS_0
                  .toLocalizedString(Integer.valueOf(status));
          LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
          if (VERBOSE)
            writer.fine(exception);
          throw new IllegalStateException(exception);
        }
      }
    } else {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_IS_NOT_ACTIVE_AND_CANNOT_BE_COMMITTED
              .toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE)
        writer.fine(exception);
      throw new IllegalStateException(exception);
    }
    // Only one thread can call commit (the first thread to do reach the block
    // above).
    // Before commiting the notifications to be done before the done are called
    // the global transaction is called and then the after completion
    // notifications
    // are taken care of. The transactions associated to the global
    // transactions are
    // removed from the map and also the tread to transaction.
    //
    // Asif : Store the thrown Exception in case of commit .
    // Reuse it for thrwing later.
    // Asif TODO:Verify if it is a good practise
    boolean isClean = false;
    Exception e = null;
    try {
      ((TransactionImpl) transactionImpl).notifyBeforeCompletion();
      isClean = true;
    } catch (Exception ge) {
      // Asif : Just mark the Tranxn to setRollbackOnly to ensure Rollback
      setRollbackOnly();
      cozOfException = EXCEPTION_IN_NOTIFY_BEFORE_COMPLETION;
      e = ge;
    }
    // TODO:Asif In case the status of transaction is marked as
    // ROLLING_BACK , then we don't have to take a synch block
    // As once the status is marked for ROLLING_BACK , setRollnbackonly
    // will be harmless
    if (isCommit) {
      synchronized (gtx) {
        if ((status = gtx.getStatus()) == Status.STATUS_COMMITTING) {
          // Asif: Catch any exception encountered during commit
          // and appropriately mark the exception code
          try {
            gtx.commit();
          } catch (RollbackException rbe) {
            e = rbe;
            cozOfException = COMMIT_FAILED_SO_ROLLEDBAK;
          } catch (SystemException se) {
            e = se;
            cozOfException = COMMIT_FAILED_ROLLBAK_ALSO_FAILED;
          }
        } else if (status == Status.STATUS_ROLLING_BACK) {
          try {
            gtx.rollback();
            if (isClean)
              cozOfException = MARKED_ROLLBACK;
          } catch (SystemException se) {
            e = se;
            cozOfException = ROLLBAK_FAILED;
          }
        }
      }
    } else {
      try {
        gtx.rollback();
      } catch (SystemException se) {
        e = se;
        cozOfException = ROLLBAK_FAILED;
      }
    }
    try {
      ((TransactionImpl) transactionImpl).notifyAfterCompletion(status = gtx.getStatus());
    } catch (Exception ge) {
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.infoEnabled())
        writer.info(
            LocalizedStrings.TransactionManagerImpl_EXCEPTION_IN_NOTIFY_AFTER_COMPLETION_DUE_TO__0,
            ge.getMessage(), ge);
    }
    Thread thread = Thread.currentThread();
    transactionMap.remove(thread);
    this.gtxSet.remove(gtx);
    if (status != Status.STATUS_COMMITTED) {
      switch (cozOfException) {
        case EXCEPTION_IN_NOTIFY_BEFORE_COMPLETION: {
          String exception =
              LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_ROLLED_BACK_BECAUSE_OF_EXCEPTION_IN_NOTIFYBEFORECOMPLETION_FUNCTION_CALL_ACTUAL_EXCEPTION_0
                  .toLocalizedString();
          LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
          if (VERBOSE)
            writer.fine(exception, e);
          RollbackException re = new RollbackException(exception);
          re.initCause(e);
          throw re;
        }
        case MARKED_ROLLBACK: {
          String exception =
              LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_ROLLED_BACK_BECAUSE_A_USER_MARKED_IT_FOR_ROLLBACK
                  .toLocalizedString();
          LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
          if (VERBOSE)
            writer.fine(exception, e);
          throw new RollbackException(exception);
        }
        case COMMIT_FAILED_SO_ROLLEDBAK: {
          LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
          if (VERBOSE)
            writer.fine(e);
          throw (RollbackException) e;
        }
        case COMMIT_FAILED_ROLLBAK_ALSO_FAILED:
        case ROLLBAK_FAILED: {
          LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
          if (VERBOSE)
            writer.fine(e);
          throw (SystemException) e;
        }
      }
    }
    gtx.setStatus(Status.STATUS_NO_TRANSACTION);
  }

  /**
   * Rolls back the transaction associated with the current thread by calling the
   * GlobalTransaction.rollback(). When this method completes, the thread is no longer associated
   * with a transaction.
   *
   * @throws java.lang.SecurityException - Thrown to indicate that the thread is not allowed to
   *         commit the transaction.
   * @throws java.lang.IllegalStateException - Thrown if the current thread is not associated with a
   *         transaction.
   * @throws SystemException - Thrown if the transaction manager encounters an unexpected error
   *         condition.
   *
   * @see javax.transaction.TransactionManager#commit()
   */
  public void rollback() throws IllegalStateException, SecurityException, SystemException {
    if (!isActive) {
      throw new SystemException(
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGER_INVALID.toLocalizedString());
    }
    // boolean isRollingBack = false;
    LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
    Transaction transactionImpl = getTransaction();
    if (transactionImpl == null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_NO_TRANSACTION_EXISTS
              .toLocalizedString();
      if (VERBOSE)
        writer.fine(exception);
      throw new IllegalStateException(exception);
    }
    GlobalTransaction gtx = getGlobalTransaction(transactionImpl);
    if (gtx == null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_NO_GLOBAL_TRANSACTION_EXISTS
              .toLocalizedString();
      if (VERBOSE)
        writer.fine(exception);
      throw new SystemException(exception);
    }
    int status = gtx.getStatus();
    if (!(status == Status.STATUS_ACTIVE || status == Status.STATUS_MARKED_ROLLBACK)) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_TRANSACTION_STATUS_DOES_NOT_ALLOW_ROLLBACK_TRANSACTIONAL_STATUS_0
              .toLocalizedString(Integer.valueOf(status));
      if (VERBOSE)
        writer.fine(exception);
      throw new IllegalStateException(exception);
    }
    // ensure only one thread proceeds from here
    status = -1;
    synchronized (gtx) {
      if ((status = gtx.getStatus()) == Status.STATUS_ACTIVE
          || status == Status.STATUS_MARKED_ROLLBACK)
        gtx.setStatus(Status.STATUS_ROLLING_BACK);
      else if (gtx.getStatus() == Status.STATUS_ROLLING_BACK) {
        String exception =
            LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_TRANSACTION_ALREADY_IN_A_ROLLING_BACK_STATE_TRANSACTIONAL_STATUS_0
                .toLocalizedString(Integer.valueOf(status));
        if (VERBOSE)
          writer.fine(exception);
        throw new IllegalStateException(exception);
      } else {
        String exception =
            LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_TRANSACTION_STATUS_DOES_NOT_ALLOW_ROLLBACK
                .toLocalizedString();
        if (VERBOSE)
          writer.fine(exception);
        throw new IllegalStateException(exception);
      }
    }
    // Only one thread can call rollback (the first thread to do reach the
    // block above).
    // Before rollback the notifications to be done before the done are called
    // the global transaction is called and then the after completion
    // notifications
    // are taken care of. The transactions associated to the global
    // transactions are
    // removed from the map and also the tread to transaction.
    //
    // TODO remove all threads-transactions (from the map)
    // for transactions participating in the global transaction
    //
    SystemException se = null;
    try {
      gtx.rollback();
    } catch (SystemException se1) {
      se = se1;
    }
    try {
      ((TransactionImpl) transactionImpl).notifyAfterCompletion(gtx.getStatus());
    } catch (Exception e1) {
      if (writer.infoEnabled())
        writer.info(
            LocalizedStrings.TransactionManagerImpl_EXCEPTION_IN_NOTIFY_AFTER_COMPLETION_DUE_TO__0,
            e1.getMessage(), e1);
    }
    Thread thread = Thread.currentThread();
    transactionMap.remove(thread);
    this.gtxSet.remove(gtx);
    if (se != null) {
      if (VERBOSE)
        writer.fine(se);
      throw se;
    }
    gtx.setStatus(Status.STATUS_NO_TRANSACTION);
  }

  /**
   * Set the Global Transaction status (Associated with the current thread) to be RollBackOnly
   *
   * Becauce we are using one phase commit, we are not considering Prepared and preparing states.
   *
   * @see javax.transaction.TransactionManager#setRollbackOnly()
   */
  public void setRollbackOnly() throws IllegalStateException, SystemException {
    if (!isActive) {
      throw new SystemException(
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGER_INVALID.toLocalizedString());
    }
    GlobalTransaction gtx = getGlobalTransaction();
    if (gtx == null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_SETROLLBACKONLY_NO_GLOBAL_TRANSACTION_EXISTS
              .toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE)
        writer.fine(exception);
      throw new SystemException(exception);
    }
    synchronized (gtx) {
      int status = gtx.getStatus();
      if (status == Status.STATUS_ACTIVE)
        gtx.setRollbackOnly();
      else if (status == Status.STATUS_COMMITTING)
        gtx.setStatus(Status.STATUS_ROLLING_BACK);
      else if (status == Status.STATUS_ROLLING_BACK)
        ; // Dont do anything
      else {
        String exception =
            LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_SETROLLBACKONLY_TRANSACTION_CANNOT_BE_MARKED_FOR_ROLLBACK_TRANSCATION_STATUS_0
                .toLocalizedString(Integer.valueOf(status));
        LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
        if (VERBOSE)
          writer.fine(exception);
        throw new IllegalStateException(exception);
      }
    }
    // Asif : Log after exiting synch block
    LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
    if (VERBOSE)
      writer.fine("Transaction Set to Rollback only");
  }

  /**
   * Get the status of the global transaction associated with this thread
   *
   * @see javax.transaction.TransactionManager#getStatus()
   */
  public int getStatus() throws SystemException {
    if (!isActive) {
      throw new SystemException(
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGER_INVALID.toLocalizedString());
    }
    GlobalTransaction gtx = getGlobalTransaction();
    if (gtx == null) {
      return Status.STATUS_NO_TRANSACTION;
    }
    return gtx.getStatus();
  }

  /**
   * not supported
   *
   * @see javax.transaction.TransactionManager#setTransactionTimeout(int)
   */
  public void setTransactionTimeout(int seconds) throws SystemException {
    if (!isActive) {
      throw new SystemException(
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGER_INVALID.toLocalizedString());
    }
    GlobalTransaction gtx = getGlobalTransaction();
    if (gtx == null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_SETTRANSACTIONTIMEOUT_NO_GLOBAL_TRANSACTION_EXISTS
              .toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE)
        writer.fine(exception);
      throw new SystemException(exception);
    }
    long newExpiry = gtx.setTransactionTimeoutForXARes(seconds);
    if (newExpiry > 0) {
      // long expirationTime = System.currentTimeMillis() + (seconds * 1000);
      gtxSet.remove(gtx);
      // Asif :Lets blindly remove the current gtx from the TreeMap &
      // Add only if status is neither Rolledback, Unknown , committed or no
      // transaction or GTX not
      // expired, which gurantees that the client thread will be returning &
      // cleaning up .so we
      // don't add it
      int status = gtx.getStatus();
      if (status != Status.STATUS_NO_TRANSACTION && status != Status.STATUS_COMMITTED
          && status != Status.STATUS_ROLLEDBACK && !gtx.isExpired()) {
        // Asif : Take a lock on GTX while setting the new Transaction timeout
        // value,
        // so that cleaner thread sees the new value immediately else due to
        // volatility issue
        // we may have inconsistent values of time out
        boolean toAdd = false;
        synchronized (gtx) {
          if (!gtx.isExpired()) {
            gtx.setTimeoutValue(newExpiry);
            toAdd = true;
          }
        }
        // Asif : It is possible that in the window between we set the new
        // timeout value in current GTX & add it to the gtxSet , the currentGtx
        // is expired by the cleaner thread as there is no safeguard for it.
        // We allow it to happen that is add the expired GTx to the TreeSet.
        // Since a notify will be issued , the cleaner thread wil take care
        // of it.
        if (toAdd) {
          synchronized (gtxSet) {
            gtxSet.add(gtx);
            if (gtxSet.first() == gtx) {
              gtxSet.notify();
            }
          }
        }
      } else {
        String exception =
            LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_SETTRANSACTIONTIMEOUT_TRANSACTION_HAS_EITHER_EXPIRED_OR_ROLLEDBACK_OR_COMITTED
                .toLocalizedString();
        LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
        if (VERBOSE)
          writer.fine(exception);
        throw new SystemException(exception);
      }
    }
  }

  /**
   * @see javax.transaction.TransactionManager#suspend()
   */
  public Transaction suspend() throws SystemException {
    if (!isActive) {
      throw new SystemException(
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGER_INVALID.toLocalizedString());
    }
    Transaction txn = getTransaction();

    if (null != txn) {
      GlobalTransaction gtx = getGlobalTransaction(txn);
      gtx.suspend();
      transactionMap.remove(Thread.currentThread());
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.infoEnabled())
        writer.info(
            LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPLSUSPENDTRANSACTION_SUSPENDED);
    }

    return txn;
  }

  /**
   * @see javax.transaction.TransactionManager#resume(javax.transaction.Transaction)
   */
  public void resume(Transaction txn)
      throws InvalidTransactionException, IllegalStateException, SystemException {
    if (!isActive) {
      throw new SystemException(
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGER_INVALID.toLocalizedString());
    }
    if (txn == null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_RESUME_CANNOT_RESUME_A_NULL_TRANSACTION
              .toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE)
        writer.fine(exception);
      throw new InvalidTransactionException(exception);
    }
    GlobalTransaction gtx = getGlobalTransaction(txn);
    if (gtx == null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_RESUME_CANNOT_RESUME_A_NULL_TRANSACTION
              .toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE)
        writer.fine(exception);
      throw new InvalidTransactionException(exception);
    }
    gtx.resume();
    try {
      Thread thread = Thread.currentThread();
      transactionMap.put(thread, txn);
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.infoEnabled())
        writer.info(
            LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPLRESUMETRANSACTION_RESUMED);
    } catch (Exception e) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_RESUME_ERROR_IN_LISTING_THREAD_TO_TRANSACTION_MAP_DUE_TO_0
              .toLocalizedString(e);
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE)
        writer.fine(exception);
      throw new SystemException(exception);
    }
  }

  /**
   * Get the transaction associated with the calling thread
   *
   * @see javax.transaction.TransactionManager#getTransaction()
   */
  public Transaction getTransaction() throws SystemException {
    if (!isActive) {
      throw new SystemException(
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGER_INVALID.toLocalizedString());
    }
    Thread thread = Thread.currentThread();
    Transaction txn = (Transaction) transactionMap.get(thread);
    return txn;
  }

  /**
   * Get the Global Transaction associated with the calling thread
   */
  GlobalTransaction getGlobalTransaction() throws SystemException {
    Transaction txn = getTransaction();
    if (txn == null) {
      return null;
    }
    GlobalTransaction gtx = (GlobalTransaction) globalTransactionMap.get(txn);
    return gtx;
  }

  /**
   * Get the Global Transaction associated with the calling thread
   */
  GlobalTransaction getGlobalTransaction(Transaction txn) throws SystemException {
    if (txn == null) {
      String exception =
          LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPL_GETGLOBALTRANSACTION_NO_TRANSACTION_EXISTS
              .toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE)
        writer.fine(exception);
      throw new SystemException(exception);
    }
    GlobalTransaction gtx = (GlobalTransaction) globalTransactionMap.get(txn);
    return gtx;
  }

  // Asif : This method is used only for testing purposes
  Map getGlobalTransactionMap() {
    return globalTransactionMap;
  }

  // Asif : This method is used only for testing purposes
  Map getTransactionMap() {
    return transactionMap;
  }

  // Asif : Remove the mapping of tranxn to Global Tranxn.
  // Previously thsi task was being done in GlobalTranxn
  void cleanGlobalTransactionMap(List tranxns) {
    synchronized (tranxns) {
      Iterator iterator = tranxns.iterator();
      while (iterator.hasNext()) {
        globalTransactionMap.remove(iterator.next());
      }
    }
  }

  void removeTranxnMappings(List tranxns) {
    Object[] threads = transactionMap.keySet().toArray();
    int len = threads.length;
    Object tx = null;
    boolean removed = false;
    Object temp = null;
    for (int i = 0; i < len; ++i) {
      tx = transactionMap.get((temp = threads[i]));
      removed = tranxns.remove(tx);
      if (removed) {
        transactionMap.remove(temp);
        globalTransactionMap.remove(tx);
      }
    }
  }

  class TransactionTimeOutThread implements Runnable {

    protected volatile boolean toContinueRunning = true;

    public void run() {
      GlobalTransaction currentGtx = null;
      long lag = 0;
      LogWriterI18n logger = TransactionUtils.getLogWriterI18n();

      while (toContinueRunning) { // Asif :Ensure that we do not get out of
        try { // wait
          // without a GTX object
          synchronized (gtxSet) {
            while (gtxSet.isEmpty() && toContinueRunning) {
              gtxSet.wait();
            }
            if (!toContinueRunning)
              continue;
            currentGtx = (GlobalTransaction) gtxSet.first();
          }
          // Asif : Check whether current GTX has timed out or not
          boolean continueInner = true;
          do {
            synchronized (currentGtx) {
              lag = System.currentTimeMillis() - currentGtx.getExpirationTime();
              if (lag >= 0) {
                // Asif: Expire the GTX .Do not worry if some GTX comes
                // before it in Map , ie
                // even if tehre is a GTX earlier than this one to expire ,
                // it is OK
                // to first take care of this one
                // TODO: Do the clean up from all Maps
                currentGtx.expireGTX();
                gtxSet.remove(currentGtx);
              }
            }
            synchronized (gtxSet) {
              if (gtxSet.isEmpty()) {
                continueInner = false;
              } else {
                currentGtx = (GlobalTransaction) gtxSet.first();
                boolean isGTXExp = false;
                // Asif: There will not be any need for synchronizing
                // on currentGTX as we are already taking lokc on gtxSet.
                // Since the GTXSEt is locked that implies no GTX can be
                // either removed or added ( if already removed)
                // if the thread is in this block . thus we are safe
                //
                // synchronized (currentGtx) {
                lag = System.currentTimeMillis() - currentGtx.getExpirationTime();
                if (lag < 0) {
                  // Asif : Make the thread wait for stipluated
                  // time
                  isGTXExp = false;
                } else {
                  isGTXExp = true;
                  // Asif It is possibel that GTX is already expired but
                  // just got added in TreeSet bcoz of the small window
                  // in setTimeOut function of TranxnManager.
                  if (!currentGtx.isExpired()) {
                    currentGtx.expireGTX();
                  }
                  gtxSet.remove(currentGtx);
                  // Asif Clean the objects
                  if (gtxSet.isEmpty()) {
                    continueInner = false;
                  } else {
                    currentGtx = (GlobalTransaction) gtxSet.first();
                  }
                }
                // }
                // Asif : It is OK if we release the lock on Current GTX
                // for sleep
                // bcoz as we have still the the lock on gtxSet, even if
                // the
                // transaction set time out gets modified by other client
                // thread,
                // it will notbe added to the set as the lock is still
                // with us.
                // So in case of new tiemout value, if it is such that
                // the GTX is
                /// at the beginning , notified will be issued & the
                // cleaner thread
                // wil awake.
                // the set as the lock is
                if (!isGTXExp && toContinueRunning) {
                  gtxSet.wait(-(lag));
                  if (gtxSet.isEmpty()) {
                    continueInner = false;
                  } else {
                    currentGtx = (GlobalTransaction) gtxSet.first();
                  }
                }
                if (!toContinueRunning) {
                  continueInner = false;
                }
              }
            } // synchronized
          } while (continueInner);
        } catch (InterruptedException e) {
          // No need to reset the bit; we'll exit.
          if (toContinueRunning) {
            logger.fine("TransactionTimeOutThread: unexpected exception", e);
          }
          return;
        } catch (CancelException e) {
          // this thread is shutdown by doing an interrupt so this is expected
          // logger.fine("TransactionTimeOutThread: encountered exception", e);
          return;
        } catch (Exception e) {
          if (logger.severeEnabled() && toContinueRunning) {
            logger.severe(
                LocalizedStrings.TransactionManagerImpl_TRANSACTIONTIMEOUTTHREAD__RUN_EXCEPTION_OCCURRED_WHILE_INSPECTING_GTX_FOR_EXPIRY,
                e);
          }
        }
      }
    }
  }

  static class GlobalTransactionComparator implements Comparator, Serializable {

    /**
     * Sort the array in ascending order of expiration times
     */
    public int compare(Object obj1, Object obj2) {
      GlobalTransaction gtx1 = (GlobalTransaction) obj1;
      GlobalTransaction gtx2 = (GlobalTransaction) obj2;
      return gtx1.compare(gtx2);
    }

    /**
     * Overwrite default equals implementation
     */
    @Override
    public boolean equals(Object o1) {
      return this == o1;
    }
  }

  /**
   * Shutdown the transactionManager and threads associated with this.
   */
  public static void refresh() {
    getTransactionManager();
    transactionManager.isActive = false;
    transactionManager.cleaner.toContinueRunning = false;
    try {
      transactionManager.cleanUpThread.interrupt();
    } catch (Exception e) {
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.infoEnabled())
        writer.info(
            LocalizedStrings.TransactionManagerImpl_TRANSACTIONMANAGERIMPLCLEANUPEXCEPTION_WHILE_CLEANING_THREAD_BEFORE_RE_STATRUP);
    }
    /*
     * try { transactionManager.cleanUpThread.join(); } catch (Exception e) { e.printStackTrace(); }
     */
    transactionManager = null;
  }
}
