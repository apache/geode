package com.gemstone.gemfire.internal.redis.executor;

import java.util.List;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class ExistsExecutor extends AbstractExecutor {
  private final int EXISTS = 1;

  private final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.EXISTS));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    boolean exists = context.getRegionProvider().existsKey(key);

    if (exists)
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), EXISTS));
    else
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));

  }
}
