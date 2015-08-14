package com.gemstone.gemfire.internal.redis.executor.list;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.Coder;

public abstract class PushExecutor extends PushXExecutor implements Extendable {

  private final int START_VALUES_INDEX = 2;
  static volatile AtomicInteger puts = new AtomicInteger(0);

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<Integer, ByteArrayWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_LIST);
    pushElements(key, commandElems, START_VALUES_INDEX, commandElems.size(), keyRegion, pushType(), context);
    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), listSize));
  }

  protected abstract ListDirection pushType();

}
