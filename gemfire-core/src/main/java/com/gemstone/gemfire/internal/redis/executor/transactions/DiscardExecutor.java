package com.gemstone.gemfire.internal.redis.executor.transactions;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;

public class DiscardExecutor extends TransactionExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {

    CacheTransactionManager txm = context.getCacheTransactionManager();

    if (context.hasTransaction()) {
      TransactionId transactionId = context.getTransactionID();
      txm.resume(transactionId);
      txm.rollback();
      context.clearTransaction();
    }

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), "OK"));
  }

}
