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
package org.apache.geode.internal.jta;

/**
 * TransactionImpl implements the JTA Transaction interface.
 * 
 */
import javax.transaction.xa.*;
import javax.transaction.*;

import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.util.*;

public class TransactionImpl implements Transaction {

  /**
   * The Global Transaction to which this Transaction belongs.
   */
  private GlobalTransaction gtx = null;
  /**
   * A Synchronization object.
   */
  private Synchronization sync = null;
  /**
   * The transaction manager that owns this transaction.
   */
  private TransactionManagerImpl tm = TransactionManagerImpl
      .getTransactionManager();

  /**
   * List of registered Synchronization objects.
   */
  private List syncList = new ArrayList();

  /**
   * Constructs an instance of a Transaction
   */
  public TransactionImpl() {
  }

  /**
   * Calls the commit() of the TransactionManager that owns the current
   * Transaction
   * 
   * @throws RollbackException - Thrown to indicate that the transaction has
   *             been rolled back rather than committed.
   * @throws HeuristicMixedException - Thrown to indicate that a heuristic
   *             decision was made and that some relevant updates have been
   *             committed while others have been rolled back.
   * @throws HeuristicRollbackException - Thrown to indicate that a heuristic
   *             decision was made and that all relevant updates have been
   *             rolled back.
   * @throws java.lang.SecurityException - Thrown to indicate that the thread
   *             is not allowed to commit the transaction.
   * @throws java.lang.IllegalStateException - Thrown if the current thread is
   *             not associated with a transaction.
   * @throws SystemException - Thrown if the transaction manager encounters an
   *             unexpected error condition.
   * 
   * @see javax.transaction.Transaction#commit()
   */
  public void commit() throws RollbackException, HeuristicMixedException,
      HeuristicRollbackException, SecurityException, SystemException {
    tm.commit();
  }

  /**
   * Calls the rollback() of the TransactionManager that owns the current
   * Transaction
   * 
   * @throws java.lang.SecurityException - Thrown to indicate that the thread
   *             is not allowed to commit the transaction.
   * @throws java.lang.IllegalStateException - Thrown if the current thread is
   *             not associated with a transaction.
   * @throws SystemException - Thrown if the transaction manager encounters an
   *             unexpected error condition.
   * 
   * @see javax.transaction.Transaction#rollback()
   */
  public void rollback() throws IllegalStateException, SystemException {
    tm.rollback();
  }

  /**
   * Sets the status of the Global Transaction associated with this transaction
   * to be RollBack only
   * 
   * @see javax.transaction.Transaction#setRollbackOnly()
   */
  public void setRollbackOnly() throws IllegalStateException, SystemException {
    gtx = tm.getGlobalTransaction();
    if (gtx == null) {
      String exception = LocalizedStrings.TransactionImpl_TRANSACTIONIMPL_SETROLLBACKONLY_NO_GLOBAL_TRANSACTION_EXISTS.toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.fineEnabled()) writer.fine(exception);
      throw new SystemException(exception);
    }
    gtx.setRollbackOnly();
  }

  /**
   * Get the status of the Global Transaction associated with this local
   * transaction
   * 
   * @see javax.transaction.Transaction#getStatus()
   */
  public int getStatus() throws SystemException {
    gtx = tm.getGlobalTransaction();
    if (gtx == null) {
      return Status.STATUS_NO_TRANSACTION;
    }
    return gtx.getStatus();
  }

  /**
   * not supported
   */
  public void setTransactionTimeout(int seconds) throws SystemException {
    String exception = LocalizedStrings.TransactionImpl_SETTRANSACTIONTIMEOUT_IS_NOT_SUPPORTED.toLocalizedString(); 
    LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
    if (writer.fineEnabled()) writer.fine(exception);
    throw new SystemException(exception);
  }

  /**
   * Enlist the XAResource specified to the Global Transaction associated with
   * this transaction.
   * 
   * @param xaRes XAResource to be enlisted
   * @return true, if resource was enlisted successfully, otherwise false.
   * @throws SystemException Thrown if the transaction manager encounters an
   *             unexpected error condition.
   * @throws IllegalStateException Thrown if the transaction in the target
   *             object is in the prepared state or the transaction is
   *             inactive.
   * @throws RollbackException Thrown to indicate that the transaction has been
   *             marked for rollback only.
   * 
   * @see javax.transaction.Transaction#enlistResource(javax.transaction.xa.XAResource)
   */
  public boolean enlistResource(XAResource xaRes) throws RollbackException,
      IllegalStateException, SystemException {
    gtx = tm.getGlobalTransaction();
    if (gtx == null) {
      String exception = LocalizedStrings.TransactionImpl_TRANSACTIONIMPL_ENLISTRESOURCE_NO_GLOBAL_TRANSACTION_EXISTS.toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.fineEnabled()) writer.fine(exception);
      throw new SystemException(exception);
    }
    return gtx.enlistResource(xaRes);
  }

  /**
   * Disassociate the resource specified from the global transaction.
   * associated with this transaction
   * 
   * @param xaRes XAResource to be delisted
   * @param flag One of the values of TMSUCCESS, TMSUSPEND, or TMFAIL.
   * @return true, if resource was delisted successfully, otherwise false.
   * @throws SystemException Thrown if the transaction manager encounters an
   *             unexpected error condition.
   * @throws IllegalStateException Thrown if the transaction in the target
   *             object is not active.
   * 
   * @see javax.transaction.Transaction#delistResource(javax.transaction.xa.XAResource,
   *      int)
   */
  public boolean delistResource(XAResource xaRes, int flag)
      throws IllegalStateException, SystemException {
    gtx = tm.getGlobalTransaction();
    if (gtx == null) {
      String exception = LocalizedStrings.TransactionImpl_TRANSACTIONIMPL_DELISTRESOURCE_NO_GLOBAL_TRANSACTION_EXISTS.toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.fineEnabled()) writer.fine(exception);
      throw new SystemException(exception);
    }
    return gtx.delistResource(xaRes, flag);
  }

  /**
   * Register the Synchronizations by adding the synchronization object to a
   * list of synchronizations
   * 
   * @param synchronisation Synchronization the Synchronization which needs to
   *            be registered
   * 
   * @see javax.transaction.Transaction#registerSynchronization(javax.transaction.Synchronization)
   */
  public void registerSynchronization(Synchronization synchronisation)
      throws SystemException, IllegalStateException, RollbackException {
    {
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.fineEnabled()) {
        writer.fine("registering JTA synchronization: " + synchronisation);
      }
    }
    if (synchronisation == null)
        throw new SystemException(LocalizedStrings.TransactionImpl_TRANSACTIONIMPLREGISTERSYNCHRONIZATIONSYNCHRONIZATION_IS_NULL.toLocalizedString());
    gtx = tm.getGlobalTransaction();
    if (gtx == null) {
      throw new SystemException(LocalizedStrings.TransactionManagerImpl_NO_TRANSACTION_PRESENT.toLocalizedString());
    }
    synchronized (gtx) {
      int status = -1;
      if ((status = gtx.getStatus()) == Status.STATUS_MARKED_ROLLBACK) {
        String exception = LocalizedStrings.TransactionImpl_TRANSACTIONIMPL_REGISTERSYNCHRONIZATION_SYNCHRONIZATION_CANNOT_BE_REGISTERED_BECAUSE_THE_TRANSACTION_HAS_BEEN_MARKED_FOR_ROLLBACK.toLocalizedString();
        LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
        if (writer.fineEnabled()) writer.fine(exception);
        throw new RollbackException(exception);
      } else if (status != Status.STATUS_ACTIVE) {
        String exception = LocalizedStrings.TransactionImpl_TRANSACTIONIMPL_REGISTERSYNCHRONIZATION_SYNCHRONIZATION_CANNOT_BE_REGISTERED_ON_A_TRANSACTION_WHICH_IS_NOT_ACTIVE.toLocalizedString();
        LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
        if (writer.fineEnabled()) writer.fine(exception);
        throw new IllegalStateException(exception);
      }
      syncList.add(synchronisation);
    }
  }

  /**
   * Iterate over the list of Synchronizations to complete all the methods to
   * be performed before completion
   */
  boolean notifyBeforeCompletion() {
    Iterator iterator = syncList.iterator();
    boolean result = true;
    while (iterator.hasNext()) {
      sync = ((Synchronization) iterator.next());
      sync.beforeCompletion();
    }
    return result;
  }

  /**
   * Iterate over the list of Synchronizations to complete all the methods to
   * be performed after completion
   * 
   * @param status int The status of the Global transaction associated with the
   *            transaction
   */
  void notifyAfterCompletion(int status) throws SystemException {
    Iterator iterator = syncList.iterator();
    while (iterator.hasNext()) {
      sync = ((Synchronization) iterator.next());
      sync.afterCompletion(status);
    }
  }

  //This method is to be used only for test validation
  List getSyncList() {
    return syncList;
  }
}
