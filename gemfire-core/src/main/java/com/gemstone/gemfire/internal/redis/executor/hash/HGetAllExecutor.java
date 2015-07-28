package com.gemstone.gemfire.internal.redis.executor.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class HGetAllExecutor extends HashExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HGETALL));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_HASH, context);
    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }

    Collection<Map.Entry<ByteArrayWrapper,ByteArrayWrapper>> entries = new ArrayList(keyRegion.entrySet()); // This creates a CopyOnRead behavior
   
   if (entries.isEmpty()) {
     command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
     return;
   }

   command.setResponse(Coder.getKeyValArrayResponse(context.getByteBufAllocator(), entries));
  }

}
