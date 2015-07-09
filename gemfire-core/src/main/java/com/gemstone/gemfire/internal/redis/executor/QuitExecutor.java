package com.gemstone.gemfire.internal.redis.executor;

import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;
import com.gemstone.gemfire.internal.redis.Coder;

public class QuitExecutor extends AbstractExecutor {
  
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), RedisConstants.QUIT_RESPONSE));
  }

}
