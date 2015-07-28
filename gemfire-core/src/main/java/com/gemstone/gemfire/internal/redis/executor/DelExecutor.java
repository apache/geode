package com.gemstone.gemfire.internal.redis.executor;

import java.util.List;

import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class DelExecutor extends AbstractExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    if (context.hasTransaction())
      throw new UnsupportedOperationInTransactionException();

    List<byte[]> commandElems = command.getProcessedCommand();
    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.DEL));
      return;
    }

    int numRemoved = 0;

    for (int i = 1; i < commandElems.size(); i++) {
      byte[] byteKey = commandElems.get(i);
      ByteArrayWrapper key = new ByteArrayWrapper(byteKey);
      RedisDataType type = context.getRegionProvider().getRedisDataType(key); 
      if (removeEntry(key, type, context))
        numRemoved++;
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
  }

}
