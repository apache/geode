package com.gemstone.gemfire.internal.redis.executor.set;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class SRemExecutor extends SetExecutor {

  private final int NONE_REMOVED = 0;
  
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SREM));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_SET, context);
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, Boolean> keyRegion = (Region<ByteArrayWrapper, Boolean>) context.getRegionProvider().getRegion(key);
    
    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NONE_REMOVED));
      return;
    }
    
    int numRemoved = 0;
    
    for (int i = 2; i < commandElems.size(); i++) {
      Object oldVal;
      oldVal = keyRegion.remove(new ByteArrayWrapper(commandElems.get(i)));
      if (oldVal != null)
        numRemoved++;
    }
    
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
  }
}
