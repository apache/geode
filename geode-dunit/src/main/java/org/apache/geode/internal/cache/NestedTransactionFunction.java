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

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.logging.LogService;

/**
 * This function is used by {@link CommitFunction} to commit existing transaction. A
 * {@link TransactionId} corresponding to the transaction to be committed must be provided as an
 * argument while invoking this function.<br />
 *
 * When executed this function commits a transaction if it exists locally.<br />
 *
 * This function returns a single Boolean as result, whose value is <code>Boolean.TRUE</code> if the
 * transaction committed successfully otherwise the return value is <code>Boolean.FALSE</code><br />
 *
 * This function is <b>not</b> registered on the cache servers by default, and it is the user's
 * responsibility to register this function. see {@link FunctionService#registerFunction(Function)}
 *
 * @see CommitFunction
 * @since GemFire 6.6.1
 *
 */
public class NestedTransactionFunction implements Function, DataSerializable {
  private static final Logger logger = LogService.getLogger();

  public static final int COMMIT = 1;
  public static final int ROLLBACK = 2;

  private static final long serialVersionUID = 1400965724856341543L;

  public NestedTransactionFunction() {}

  public boolean hasResult() {
    return true;
  }

  public void execute(FunctionContext context) {
    Cache cache = CacheFactory.getAnyInstance();
    ArrayList args = (ArrayList) context.getArguments();
    TXId txId = null;
    int action = 0;
    try {
      txId = (TXId) args.get(0);
      action = (Integer) args.get(1);
    } catch (ClassCastException e) {
      logger.info(
          "CommitFunction should be invoked with a TransactionId as an argument i.e. setArguments(txId).execute(function)");
      throw e;
    }
    CacheTransactionManager txMgr = cache.getCacheTransactionManager();
    Boolean result = false;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (txMgr.tryResume(txId)) {
      if (isDebugEnabled) {
        logger.debug("CommitFunction: resumed transaction: {}", txId);
      }
      if (action == COMMIT) {
        if (isDebugEnabled) {
          logger.debug("CommitFunction: committing transaction: {}", txId);
        }
        txMgr.commit();
      } else if (action == ROLLBACK) {
        if (isDebugEnabled) {
          logger.debug("CommitFunction: rolling back transaction: {}", txId);
        }
        txMgr.rollback();
      } else {
        throw new IllegalStateException("unknown transaction termination action");
      }
      result = true;
    }
    if (isDebugEnabled) {
      logger.debug("CommitFunction: for transaction: {} sending result: {}", txId, result);
    }
    context.getResultSender().lastResult(result);
  }

  public String getId() {
    return getClass().getName();
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public boolean isHA() {
    return false;
  }

  @Override
  public void toData(DataOutput out) throws IOException {

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

  }
}
