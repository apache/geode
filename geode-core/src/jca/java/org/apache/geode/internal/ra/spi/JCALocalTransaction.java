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
    gfTxMgr = tm;
    initDone = true;
  }

  JCALocalTransaction() {
    cache = null;
    gfTxMgr = null;
    initDone = false;
  }

  @Override
  public void begin() throws ResourceException {
    if (!initDone || cache.isClosed()) {
      init();
    }
    if (cache.getLogger().fineEnabled()) {
      cache.getLogger().fine("JCALocalTransaction:begin invoked");
    }
    if (tid != null) {
      throw new LocalTransactionException(
          "Transaction with id=" + tid + " is already in progress");
    }
    TXStateProxy tsp = gfTxMgr.getTXState();
    if (tsp == null) {
      gfTxMgr.begin();
      tsp = gfTxMgr.getTXState();
      tsp.setJCATransaction();
      tid = tsp.getTransactionId();
      if (cache.getLogger().fineEnabled()) {
        cache.getLogger()
            .fine("JCALocalTransaction:begin completed transactionId=" + tid);
      }
    } else {
      throw new LocalTransactionException(
          "Transaction with state=" + tsp + " is already in progress");
    }
  }

  @Override
  public void commit() throws ResourceException {
    LogWriter logger = cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCALocalTransaction:invoked commit");
    }
    TXStateProxy tsp = gfTxMgr.getTXState();
    if (tsp != null && tid != tsp.getTransactionId()) {
      throw new IllegalStateException("Local Transaction associated with Tid = " + tid
          + " attempting to commit a different transaction");
    }
    try {
      gfTxMgr.commit();
      tid = null;
    } catch (Exception e) {
      throw new LocalTransactionException(e);
    }
  }

  @Override
  public void rollback() throws ResourceException {
    TXStateProxy tsp = gfTxMgr.getTXState();
    if (tsp != null && tid != tsp.getTransactionId()) {
      throw new IllegalStateException("Local Transaction associated with Tid = " + tid
          + " attempting to commit a different transaction");
    }
    LogWriter logger = cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCALocalTransaction:invoked rollback");
    }
    try {
      gfTxMgr.rollback();
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
      tid = null;
    }
  }

  private void init() {
    cache = (InternalCache) CacheFactory.getAnyInstance();
    LogWriter logger = cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCAManagedConnection:init. Inside init");
    }
    gfTxMgr = cache.getTxManager();
    initDone = true;
  }

  boolean transactionInProgress() {
    return tid != null;
  }

}
