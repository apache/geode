package com.gemstone.gemfire.internal.redis.executor.hash;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class HMSetExecutor extends HashExecutor {

  private final String SUCCESS = "OK";
  
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3 || commandElems.size() % 2 == 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HMSET));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_HASH);
    
    Map<ByteArrayWrapper, ByteArrayWrapper> map = new HashMap<ByteArrayWrapper, ByteArrayWrapper>();
    for (int i = 2; i < commandElems.size(); i += 2) {
      byte[] fieldArray = commandElems.get(i);
      ByteArrayWrapper field = new ByteArrayWrapper(fieldArray);
      byte[] value = commandElems.get(i +1);
      map.put(field, new ByteArrayWrapper(value));
    }
    
    keyRegion.putAll(map);

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));

  }

}
