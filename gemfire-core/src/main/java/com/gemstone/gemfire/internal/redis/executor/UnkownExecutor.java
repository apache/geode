package com.gemstone.gemfire.internal.redis.executor;

import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;

public class UnkownExecutor extends AbstractExecutor {
  
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_UNKOWN_COMMAND));
  }

}
