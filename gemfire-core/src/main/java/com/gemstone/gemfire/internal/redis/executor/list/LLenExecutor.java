package com.gemstone.gemfire.internal.redis.executor.list;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class LLenExecutor extends ListExecutor {

  private final int NOT_EXISTS = 0;
  
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LLEN));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    int listSize = 0;
    
    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region<Integer, ByteArrayWrapper> keyRegion = getRegion(context, key);
    
    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
      return;
    }
    
    listSize = keyRegion.size() - LIST_EMPTY_SIZE;

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), listSize));
  }
}
