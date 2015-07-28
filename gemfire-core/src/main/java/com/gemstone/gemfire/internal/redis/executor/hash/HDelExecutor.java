package com.gemstone.gemfire.internal.redis.executor.hash;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class HDelExecutor extends HashExecutor {

  private final int START_FIELDS_INDEX = 2;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HDEL));
      return;
    }

    int numDeleted = 0;
    
    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_HASH, context);
    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numDeleted));
      return;
    }

    
    for (int i = START_FIELDS_INDEX; i < commandElems.size(); i++) {
      ByteArrayWrapper field = new ByteArrayWrapper(commandElems.get(i));
      Object oldValue = keyRegion.remove(field);
      if (oldValue != null)
        numDeleted++;
    }
    if (keyRegion.isEmpty()) {
      context.getRegionProvider().removeKey(key, RedisDataType.REDIS_HASH);
    }
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numDeleted));
  }

}
