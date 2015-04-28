/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.TransactionDataNodeHasDepartedException;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.TXId;
import com.gemstone.gemfire.internal.cache.execute.TransactionFunctionService;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * This function can be used by GemFire clients and peers to rollback an existing
 * transaction. A {@link TransactionId} corresponding to the transaction to be
 * rolledback must be provided as an argument while invoking this function.<br />
 * 
 * This function should execute only on one server. If the transaction is not
 * hosted on the server where the function is invoked then this function decides
 * to invoke a {@link NestedTransactionFunction} which executes on the member where
 * transaction is hosted.<br />
 * 
 * This function returns a single Boolean as result, whose value is <code>Boolean.TRUE</code>
 * if the transaction rolled back successfully otherwise the return value is
 * <code>Boolean.FALSE</code>.<br />
 * 
 * To execute this function, it is recommended to use the {@link Execution} obtained by
 * using {@link TransactionFunctionService}. <br />
 * 
 * To summarize, this function should be used as follows:
 * 
 * <pre>
 * Execution exe = TransactionFunctionService.onTransaction(txId);
 * List l = (List) exe.execute(rollbackFunction).getResult();
 * Boolean result = (Boolean) l.get(0);
 * </pre>
 * 
 * This function is <b>not</b> registered on the cache servers by default, and
 * it is the user's responsibility to register this function. see
 * {@link FunctionService#registerFunction(Function)}
 * 
 * @see TransactionFunctionService
 * @since 6.6.1
 * @author sbawaska
 */
public class RollbackFunction implements Function {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1377183180063184795L;

  public boolean hasResult() {
    return true;
  }

  public void execute(FunctionContext context) {
    Cache cache = CacheFactory.getAnyInstance();
    TXId txId = null;
    try {
      txId = (TXId) context.getArguments();
    } catch (ClassCastException e) {
      logger.info("RollbackFunction should be invoked with a TransactionId as an argument i.e. withArgs(txId).execute(function)");
      throw e;
    }
    DistributedMember member = txId.getMemberId();
    Boolean result = false;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (cache.getDistributedSystem().getDistributedMember().equals(member)) {
      if (isDebugEnabled) {
        logger.debug("RollbackFunction: for transaction: {} rolling back locally", txId);
      }
      CacheTransactionManager txMgr = cache.getCacheTransactionManager();
      if (txMgr.tryResume(txId)) {
        if (isDebugEnabled) {
          logger.debug("RollbackFunction: resumed transaction: {}", txId);
        }
        txMgr.rollback();
        result = true;
      }
    } else {
      ArrayList args = new ArrayList();
      args.add(txId);
      args.add(NestedTransactionFunction.ROLLBACK);
      Execution ex = FunctionService.onMember(cache.getDistributedSystem(),
          member).withArgs(args);
      if (isDebugEnabled) {
        logger.debug("RollbackFunction: for transaction: {} executing NestedTransactionFunction on member: {}", txId, member);
      }
      try {
        List list = (List) ex.execute(new NestedTransactionFunction()).getResult();
        result = (Boolean) list.get(0);
      } catch (FunctionException fe) {
        throw new TransactionDataNodeHasDepartedException("Could not Rollback on member:"+member);
      }
    }
    if (isDebugEnabled) {
      logger.debug("RollbackFunction: for transaction: {} returning result: {}", txId, result);
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
