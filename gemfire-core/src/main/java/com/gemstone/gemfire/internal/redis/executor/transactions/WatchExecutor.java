package com.gemstone.gemfire.internal.redis.executor.transactions;

import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;

public class WatchExecutor extends TransactionExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_WATCH));
  }

}
