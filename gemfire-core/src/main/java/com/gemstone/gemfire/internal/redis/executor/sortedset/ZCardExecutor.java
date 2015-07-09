package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class ZCardExecutor extends SortedSetExecutor {

  private final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZCARD));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);
    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);

    if (keyRegion == null)
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
    else
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), keyRegion.size()));

  }
}
