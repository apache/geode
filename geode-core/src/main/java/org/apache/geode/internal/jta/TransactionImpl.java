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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.apache.geode.LogWriter;

/**
 * TransactionImpl implements the JTA Transaction interface.
 *
 * @deprecated as of Geode 1.2.0 user should use a third party JTA transaction manager to manage JTA
 *             transactions.
 */
@Deprecated
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
  private TransactionManagerImpl tm = TransactionManagerImpl.getTransactionManager();

  /**
   * List of registered Synchronization objects.
   */
  private List syncList = new ArrayList();

  /**
   * Constructs an instance of a Transaction
   */
  public TransactionImpl() {}

  /**
   * Calls the commit() of the TransactionManager that owns the current Transaction
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
   * @see javax.transaction.Transaction#commit()
   */
  @Override
  public void commit() throws RollbackException, HeuristicMixedException,
      HeuristicRollbackException, SecurityException, SystemException {
    tm.commit();
  }

  /**
   * Calls the rollback() of the TransactionManager that owns the current Transaction
   *
   * @throws java.lang.SecurityException - Thrown to indicate that the thread is not allowed to
   *         commit the transaction.
   * @throws java.lang.IllegalStateException - Thrown if the current thread is not associated with a
   *         transaction.
   * @throws SystemException - Thrown if the transaction manager encounters an unexpected error
   *         condition.
   *
   * @see javax.transaction.Transaction#rollback()
   */
  @Override
  public void rollback() throws IllegalStateException, SystemException {
    tm.rollback();
  }

  /**
   * Sets the status of the Global Transaction associated with this transaction to be RollBack only
   *
   * @see javax.transaction.Transaction#setRollbackOnly()
   */
  @Override
  public void setRollbackOnly() throws IllegalStateException, SystemException {
    gtx = tm.getGlobalTransaction();
    if (gtx == null) {
      String exception =
          "TransactionImpl::setRollbackOnly: No global transaction exists.";
      LogWriter writer = TransactionUtils.getLogWriter();
      if (writer.fineEnabled())
        writer.fine(exception);
      throw new SystemException(exception);
    }
    gtx.setRollbackOnly();
  }

  /**
   * Get the status of the Global Transaction associated with this local transaction
   *
   * @see javax.transaction.Transaction#getStatus()
   */
  @Override
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
    String exception =
        "setTransactionTimeout is not supported.";
    LogWriter writer = TransactionUtils.getLogWriter();
    if (writer.fineEnabled())
      writer.fine(exception);
    throw new SystemException(exception);
  }

  /**
   * Enlist the XAResource specified to the Global Transaction associated with this transaction.
   *
   * @param xaRes XAResource to be enlisted
   * @return true, if resource was enlisted successfully, otherwise false.
   * @throws SystemException Thrown if the transaction manager encounters an unexpected error
   *         condition.
   * @throws IllegalStateException Thrown if the transaction in the target object is in the prepared
   *         state or the transaction is inactive.
   * @throws RollbackException Thrown to indicate that the transaction has been marked for rollback
   *         only.
   *
   * @see javax.transaction.Transaction#enlistResource(javax.transaction.xa.XAResource)
   */
  @Override
  public boolean enlistResource(XAResource xaRes)
      throws RollbackException, IllegalStateException, SystemException {
    gtx = tm.getGlobalTransaction();
    if (gtx == null) {
      String exception =
          "TransactionImpl::enlistResource: No global transaction exists";
      LogWriter writer = TransactionUtils.getLogWriter();
      if (writer.fineEnabled())
        writer.fine(exception);
      throw new SystemException(exception);
    }
    return gtx.enlistResource(xaRes);
  }

  /**
   * Disassociate the resource specified from the global transaction. associated with this
   * transaction
   *
   * @param xaRes XAResource to be delisted
   * @param flag One of the values of TMSUCCESS, TMSUSPEND, or TMFAIL.
   * @return true, if resource was delisted successfully, otherwise false.
   * @throws SystemException Thrown if the transaction manager encounters an unexpected error
   *         condition.
   * @throws IllegalStateException Thrown if the transaction in the target object is not active.
   *
   * @see javax.transaction.Transaction#delistResource(javax.transaction.xa.XAResource, int)
   */
  @Override
  public boolean delistResource(XAResource xaRes, int flag)
      throws IllegalStateException, SystemException {
    gtx = tm.getGlobalTransaction();
    if (gtx == null) {
      String exception =
          "TransactionImpl::delistResource: No global transaction exists";
      LogWriter writer = TransactionUtils.getLogWriter();
      if (writer.fineEnabled())
        writer.fine(exception);
      throw new SystemException(exception);
    }
    return gtx.delistResource(xaRes, flag);
  }

  /**
   * Register the Synchronizations by adding the synchronization object to a list of
   * synchronizations
   *
   * @param synchronisation Synchronization the Synchronization which needs to be registered
   *
   * @see javax.transaction.Transaction#registerSynchronization(javax.transaction.Synchronization)
   */
  @Override
  public void registerSynchronization(Synchronization synchronisation)
      throws SystemException, IllegalStateException, RollbackException {
    {
      LogWriter writer = TransactionUtils.getLogWriter();
      if (writer.fineEnabled()) {
        writer.fine("registering JTA synchronization: " + synchronisation);
      }
    }
    if (synchronisation == null)
      throw new SystemException(
          "TransactionImpl::registerSynchronization:Synchronization is null");
    gtx = tm.getGlobalTransaction();
    if (gtx == null) {
      throw new SystemException(
          "no transaction present");
    }
    synchronized (gtx) {
      int status = -1;
      if ((status = gtx.getStatus()) == Status.STATUS_MARKED_ROLLBACK) {
        String exception =
            "TransactionImpl::registerSynchronization: Synchronization cannot be registered because the transaction has been marked for rollback";
        LogWriter writer = TransactionUtils.getLogWriter();
        if (writer.fineEnabled())
          writer.fine(exception);
        throw new RollbackException(exception);
      } else if (status != Status.STATUS_ACTIVE) {
        String exception =
            "TransactionImpl::registerSynchronization: Synchronization cannot be registered on a transaction which is not active";
        LogWriter writer = TransactionUtils.getLogWriter();
        if (writer.fineEnabled())
          writer.fine(exception);
        throw new IllegalStateException(exception);
      }
      syncList.add(synchronisation);
    }
  }

  /**
   * Iterate over the list of Synchronizations to complete all the methods to be performed before
   * completion
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
   * Iterate over the list of Synchronizations to complete all the methods to be performed after
   * completion
   *
   * @param status int The status of the Global transaction associated with the transaction
   */
  void notifyAfterCompletion(int status) throws SystemException {
    Iterator iterator = syncList.iterator();
    while (iterator.hasNext()) {
      sync = ((Synchronization) iterator.next());
      sync.afterCompletion(status);
    }
  }

  // This method is to be used only for test validation
  List getSyncList() {
    return syncList;
  }

  public boolean notifyBeforeCompletionForTest() {
    return notifyBeforeCompletion();
  }
}
