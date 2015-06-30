package com.gemstone.gemfire.internal.redis.executor.list;

import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;

public class LInsertExecutor extends ListExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), "Unfortunately GemFireRedis server does not support LINSERT"));
  }
}
