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


import java.io.Serializable;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

/**
 * <p>
 * <code> UserTransactionImpl </code> is an implementation of UserTransaction interface. It is
 * hard-coded to <code> TransactionManagerImpl
 * </code>.
 * </p>
 *
 *
 * @since GemFire 4.0
 *
 * @deprecated as of Geode 1.2.0 user should use a third party JTA transaction manager to manage JTA
 *             transactions.
 */
@Deprecated
public class UserTransactionImpl implements UserTransaction, Serializable {
  private static final long serialVersionUID = 2994652455204901910L;

  /**
   * The TransactionManager which will manage this UserTransaction.
   */
  private TransactionManager tm = null;

  /**
   * Construct a UserTransactionImpl Object
   */
  public UserTransactionImpl() throws SystemException {
    tm = TransactionManagerImpl.getTransactionManager();
  }

  /**
   * timeOut which is stored in case timeOut is called before begin
   */
  private int storedTimeOut = TransactionManagerImpl.DEFAULT_TRANSACTION_TIMEOUT;

  /**
   * Calls begin() of the transaction manager owning this user transaction
   *
   * @see javax.transaction.UserTransaction#begin()
   */
  @Override
  public synchronized void begin() throws NotSupportedException, SystemException {
    LogWriter log = InternalDistributedSystem.getLogger();
    if (log.fineEnabled()) {
      log.fine("UserTransactionImpl starting JTA transaction");
    }
    int temp = storedTimeOut;
    storedTimeOut = TransactionManagerImpl.DEFAULT_TRANSACTION_TIMEOUT;
    tm.begin();
    tm.setTransactionTimeout(temp);
  }

  /**
   * Calls commit() of the transaction manager owning this user transaction
   *
   * @see javax.transaction.UserTransaction#commit()
   */
  @Override
  public void commit() throws RollbackException, HeuristicMixedException,
      HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
    tm.commit();
  }

  /**
   * Calls rollback() of the transaction manager owning this user transaction
   *
   * @see javax.transaction.UserTransaction#rollback()
   */
  @Override
  public void rollback() throws IllegalStateException, SecurityException, SystemException {
    tm.rollback();
  }

  /**
   * Calls setRollbackOnly() of the transaction manager owning this user transaction
   *
   * @see javax.transaction.UserTransaction#setRollbackOnly()
   */
  @Override
  public void setRollbackOnly() throws IllegalStateException, SystemException {
    tm.setRollbackOnly();
  }

  /**
   * Calls getStatus() of the transaction manager owning this user transaction
   *
   * @see javax.transaction.UserTransaction#getStatus()
   */
  @Override
  public int getStatus() throws SystemException {
    return tm.getStatus();
  }

  /**
   * Checks if transaction has begun. If yes, then call the tm.setTransactionTimeOut else stores and
   *
   * @see javax.transaction.UserTransaction#setTransactionTimeout
   */
  @Override
  public void setTransactionTimeout(int timeOut) throws SystemException {
    if (timeOut < 0) {
      String exception =
          "Cannot set a negative Time Out for transactions";
      LogWriter writer = TransactionUtils.getLogWriter();
      if (writer.fineEnabled())
        writer.fine(exception);
      throw new SystemException(exception);
    } else if (timeOut == 0) {
      timeOut = TransactionManagerImpl.DEFAULT_TRANSACTION_TIMEOUT;
    }
    if (tm.getTransaction() != null) {
      tm.setTransactionTimeout(timeOut);
    } else {
      storedTimeOut = timeOut;
    }
  }
}
