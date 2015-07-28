package com.gemstone.gemfire.internal.redis.executor.hash;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class HKeysExecutor extends HashExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HKEYS));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_HASH, context);
    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }

   Set<ByteArrayWrapper> keys = new HashSet(keyRegion.keySet());
   
   if (keys.isEmpty()) {
     command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
     return;
   }
   
   // String response = getBulkStringArrayResponse(keys);

   command.setResponse(Coder.getBulkStringArrayResponse(context.getByteBufAllocator(), keys));
  }
}
