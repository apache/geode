package com.gemstone.gemfire.internal.redis.executor;

import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;

public class DBSizeExecutor extends AbstractExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    int size = context.getRegionProvider().getMetaSize();
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), size));
  }

}
