package com.gemstone.gemfire.internal.redis.executor.transactions;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;

public class MultiExecutor extends TransactionExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {

    CacheTransactionManager txm = context.getCacheTransactionManager();

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), "OK"));

    if (context.hasTransaction()) {
      throw new IllegalStateException(RedisConstants.ERROR_NESTED_MULTI);
    }

    txm.begin();

    TransactionId id = txm.suspend();

    context.setTransactionID(id);

  }

}
