/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute.util;

import java.util.ArrayList;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.cache.TXId;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * This function is used by {@link CommitFunction} to commit existing transaction.
 * A {@link TransactionId} corresponding to the transaction to be
 * committed must be provided as an argument while invoking this function.<br />
 * 
 * When executed this function commits a transaction if it exists locally.<br />
 * 
 * This function returns a single Boolean as result, whose value is <code>Boolean.TRUE</code>
 * if the transaction committed successfully otherwise the return value is
 * <code>Boolean.FALSE</code><br />
 * 
 * This function is <b>not</b> registered on the cache servers by default, and
 * it is the user's responsibility to register this function. see
 * {@link FunctionService#registerFunction(Function)}
 * 
 * @see CommitFunction
 * @since 6.6.1
 * @author sbawaska
 *
 */
public class NestedTransactionFunction implements Function {
  private static final Logger logger = LogService.getLogger();

  public static final int COMMIT = 1;
  public static final int ROLLBACK = 2;
  
  private static final long serialVersionUID = 1400965724856341543L;

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
      logger.info("CommitFunction should be invoked with a TransactionId as an argument i.e. withArgs(txId).execute(function)");
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

}
