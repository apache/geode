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
package org.apache.geode.internal.ra.spi;

import javax.resource.ResourceException;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.LocalTransactionException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;

public class JCALocalTransaction implements LocalTransaction {
  private volatile InternalCache cache;

  private volatile TXManagerImpl gfTxMgr;

  private volatile TransactionId tid;

  private volatile boolean initDone = false;

  JCALocalTransaction(InternalCache cache, TXManagerImpl tm) {
    this.cache = cache;
    this.gfTxMgr = tm;
    this.initDone = true;
  }

  JCALocalTransaction() {
    this.cache = null;
    this.gfTxMgr = null;
    this.initDone = false;
  }

  @Override
  public void begin() throws ResourceException {
    try {
      if (!this.initDone || this.cache.isClosed()) {
        this.init();
      }
      LogWriter logger = this.cache.getLogger();
      if (logger.fineEnabled()) {
        logger.fine("JCALocalTransaction::begin:");
      }
      TransactionManager tm = this.cache.getJTATransactionManager();
      if (this.tid != null) {
        throw new LocalTransactionException(" A transaction is already in progress");
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
        } else {
          throw new LocalTransactionException("GemFire is already associated with a transaction");
        }
      } else {
        if (logger.fineEnabled()) {
          logger.fine("JCAManagedConnection: JTA Transaction does not exist.");
        }
      }
    } catch (SystemException e) {
      throw new ResourceException(e);
    }
  }

  @Override
  public void commit() throws ResourceException {
    LogWriter logger = this.cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCALocalTransaction:invoked commit");
    }
    TXStateProxy tsp = this.gfTxMgr.getTXState();
    if (tsp != null && this.tid != tsp.getTransactionId()) {
      throw new IllegalStateException("Local Transaction associated with Tid = " + this.tid
          + " attempting to commit a different transaction");
    }
    try {
      this.gfTxMgr.commit();
      this.tid = null;
    } catch (Exception e) {
      // TODO: consider wrapping the cause
      throw new LocalTransactionException(e.toString());
    }
  }

  @Override
  public void rollback() throws ResourceException {
    TXStateProxy tsp = this.gfTxMgr.getTXState();
    if (tsp != null && this.tid != tsp.getTransactionId()) {
      throw new IllegalStateException("Local Transaction associated with Tid = " + this.tid
          + " attempting to commit a different transaction");
    }
    LogWriter logger = this.cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCALocalTransaction:invoked rollback");
    }
    try {
      this.gfTxMgr.rollback();
    } catch (IllegalStateException ise) {
      // It is possible that the GFE transaction has already been rolled back.
      if (ise.getMessage()
          .equals("Thread does not have an active transaction")) {
        // ignore
      } else {
        throw new ResourceException(ise);
      }
    } catch (RuntimeException e) {
      throw new ResourceException(e);
    } finally {
      this.tid = null;
    }
  }

  private void init() {
    this.cache = (InternalCache) CacheFactory.getAnyInstance();
    LogWriter logger = this.cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCAManagedConnection:init. Inside init");
    }
    this.gfTxMgr = this.cache.getTxManager();
    this.initDone = true;
  }

  boolean transactionInProgress() {
    return this.tid != null;
  }

}
