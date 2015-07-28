package com.gemstone.gemfire.internal.redis.executor;

import java.util.List;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class TypeExecutor extends AbstractExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.TYPE));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    RedisDataType type = context.getRegionProvider().getRedisDataType(key);

    if (type == null)
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), "none"));
    else 
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), type.toString()));
  }

}
