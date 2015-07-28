package com.gemstone.gemfire.internal.redis.executor.set;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class SMoveExecutor extends SetExecutor {

  private final int MOVED = 1;

  private final int NOT_MOVED = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SMOVE));
      return;
    }

    ByteArrayWrapper source = command.getKey();
    ByteArrayWrapper destination = new ByteArrayWrapper(commandElems.get(2));
    ByteArrayWrapper mem = new ByteArrayWrapper(commandElems.get(3));

    checkDataType(source, RedisDataType.REDIS_SET, context);
    checkDataType(destination, RedisDataType.REDIS_SET, context);
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, Boolean> sourceRegion = (Region<ByteArrayWrapper, Boolean>) context.getRegionProvider().getRegion(source);

    if (sourceRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_MOVED));
      return;
    }

    Object oldVal = sourceRegion.get(mem); sourceRegion.remove(mem);

    if (oldVal == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_MOVED));
      return;
    }

    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, Boolean> destinationRegion = (Region<ByteArrayWrapper, Boolean>) getOrCreateRegion(context, destination, RedisDataType.REDIS_SET);
    destinationRegion.put(mem, true);

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), MOVED));
  }

}
