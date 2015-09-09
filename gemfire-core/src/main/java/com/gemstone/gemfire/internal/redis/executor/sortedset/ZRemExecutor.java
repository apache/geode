package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class ZRemExecutor extends SortedSetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZREM));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      return;
    }

    int numDeletedMembers = 0;
    
    for (int i = 2; i < commandElems.size(); i++) {
      byte[] memberArray = commandElems.get(i);
      ByteArrayWrapper member = new ByteArrayWrapper(memberArray);
      Object oldVal = keyRegion.remove(member);
      if (oldVal != null)
        numDeletedMembers++;
    }
    if (keyRegion.isEmpty())
      context.getRegionProvider().removeKey(key);
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numDeletedMembers));
  }
}
