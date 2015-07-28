package com.gemstone.gemfire.internal.redis.executor.list;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public abstract class PushXExecutor extends ListExecutor implements Extendable {

  private final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<Integer, ByteArrayWrapper> keyRegion = getRegion(context, key);
    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
      return;
    }
    checkDataType(key, RedisDataType.REDIS_LIST, context);    
    pushElements(key, commandElems, 2, 3, keyRegion, pushType(), context);
    
    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), listSize));
  }

  protected abstract ListDirection pushType();

}
