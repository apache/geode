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
package com.gemstone.gemfire.internal.ra.spi;

import javax.resource.ResourceException;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.LocalTransactionException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.TXStateProxy;

import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * 
 *
 */
public class JCALocalTransaction implements LocalTransaction
{
  private volatile GemFireCacheImpl cache;

  private volatile TXManagerImpl gfTxMgr;

  private volatile TransactionId tid;

  private static final boolean DEBUG = false;

  private volatile boolean initDone = false;

  JCALocalTransaction(GemFireCacheImpl cache, TXManagerImpl tm) {
    this.cache = cache;
    this.gfTxMgr = tm;
    this.initDone = true;
    // System.out.println("Asif:JCALocalTransaction:Param contrcutr for tx ="+
    // this );
  }

  JCALocalTransaction() {
    this.cache = null;
    this.gfTxMgr = null;
    this.initDone = false;
    // System.out.println("Asif:JCALocalTransaction:Empty constructor for tx ="+
    // this );
  }

  public void begin() throws ResourceException
  {
    if (DEBUG) {
      try {
        throw new NullPointerException("Asif:JCALocalTransaction:begin");
      }
      catch (NullPointerException npe) {
        npe.printStackTrace();
      }
    }
    try {
      if (!initDone || this.cache.isClosed()) {
        this.init();
      }
      // System.out.println("JCALocalTransaction:Asif: cache is ="+cache +
      // " for tx ="+this);
      LogWriter logger = cache.getLogger();
      if (logger.fineEnabled()) {
        logger.fine("JCALocalTransaction::begin:");
      }
      TransactionManager tm = cache.getJTATransactionManager();
      if (this.tid != null) {
        throw new LocalTransactionException(
            " A transaction is already in progress");
      }
      if (tm != null && tm.getTransaction() != null) {
        if (logger.fineEnabled()) {
          logger.fine("JCAManagedConnection: JTA transaction is on");
        }
        // This is having a JTA transaction. Assuming ignore jta flag is true,
        // explicitly being a gemfire transaction.
        TXStateProxy tsp = this.gfTxMgr.getTXState();
        if (tsp == null) {
          this.gfTxMgr.begin();
          tsp = this.gfTxMgr.getTXState();
          tsp.setJCATransaction();
          this.tid = tsp.getTransactionId();
          if (logger.fineEnabled()) {
            logger.fine("JCALocalTransaction:begun GFE transaction");
          }
        }
        else {
          throw new LocalTransactionException(
              "GemFire is already associated with a transaction");
        }
      }
      else {
        if (logger.fineEnabled()) {
          logger.fine("JCAManagedConnection: JTA Transaction does not exist.");
        }
      }
    }
    catch (SystemException e) {
      // this.onError();
      throw new ResourceException(e);
    }
    // Not to be invoked for local transactions managed by the container
    // Iterator<ConnectionEventListener> itr = this.listeners.iterator();
    // ConnectionEvent ce = new ConnectionEvent(this,
    // ConnectionEvent.LOCAL_TRANSACTION_STARTED);
    // while (itr.hasNext()) {
    // itr.next().localTransactionStarted(ce);
    // }

  }

  public void commit() throws ResourceException
  {
    if (DEBUG) {
      try {
        throw new NullPointerException("Asif:JCALocalTransaction:commit");
      }
      catch (NullPointerException npe) {
        npe.printStackTrace();
      }
    }
    LogWriter logger = cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCALocalTransaction:invoked commit");
    }
    TXStateProxy tsp = this.gfTxMgr.getTXState();
    if (tsp != null && this.tid != tsp.getTransactionId()) {
      throw new IllegalStateException(
          "Local Transaction associated with Tid = " + this.tid
              + " attempting to commit a different transaction");
    }
    try {
      this.gfTxMgr.commit();
      this.tid = null;
    }
    catch (Exception e) {
      throw new LocalTransactionException(e.toString());
    }
    // Iterator<ConnectionEventListener> itr = this.listeners.iterator();
    // ConnectionEvent ce = new
    // ConnectionEvent(this,ConnectionEvent.LOCAL_TRANSACTION_COMMITTED);
    // while( itr.hasNext()) {
    // itr.next().localTransactionCommitted(ce);
    // }

  }

  public void rollback() throws ResourceException
  {
    if (DEBUG) {
      try {
        throw new NullPointerException("Asif:JJCALocalTransaction:rollback");
      }
      catch (NullPointerException npe) {
        npe.printStackTrace();
      }
    }
    TXStateProxy tsp = this.gfTxMgr.getTXState();
    if (tsp != null && this.tid != tsp.getTransactionId()) {
      throw new IllegalStateException(
          "Local Transaction associated with Tid = " + this.tid
              + " attempting to commit a different transaction");
    }
    LogWriter logger = cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCALocalTransaction:invoked rollback");
    }
    try {
      this.gfTxMgr.rollback();
    }
    catch (IllegalStateException ise) {
      // It is possible that the GFE transaction has already been rolled back.
      if (ise
          .getMessage()
          .equals(
              LocalizedStrings.TXManagerImpl_THREAD_DOES_NOT_HAVE_AN_ACTIVE_TRANSACTION
                  .toLocalizedString())) {
        // /ignore;
      }
      else {
        throw new ResourceException(ise);
      }
    }
    catch (Exception e) {
      throw new ResourceException(e);
    }
    finally {
      this.tid = null;
    }
    // Iterator<ConnectionEventListener> itr = this.listeners.iterator();
    // ConnectionEvent ce = new ConnectionEvent(this,
    // ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK);
    // while (itr.hasNext()) {
    // itr.next().localTransactionRolledback(ce);
    // }

  }

  private void init() throws SystemException
  {
    this.cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
    LogWriter logger = this.cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCAManagedConnection:init. Inside init");
    }
    gfTxMgr = cache.getTxManager();
    this.initDone = true;
  }

  boolean transactionInProgress()
  {
    return this.tid != null;
  }

}
