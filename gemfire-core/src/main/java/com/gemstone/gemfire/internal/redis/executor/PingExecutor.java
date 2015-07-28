package com.gemstone.gemfire.internal.redis.executor;

import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;

public class PingExecutor extends AbstractExecutor {

  private final String PING_RESPONSE = "PONG";
  
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), PING_RESPONSE));
  }
}
