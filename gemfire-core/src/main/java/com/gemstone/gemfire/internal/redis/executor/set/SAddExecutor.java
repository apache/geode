package com.gemstone.gemfire.internal.redis.executor.set;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class SAddExecutor extends SetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SADD));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, Boolean> keyRegion = (Region<ByteArrayWrapper, Boolean>) context.getRegionProvider().getOrCreateRegion(key, RedisDataType.REDIS_SET, context);

    if (commandElems.size() >= 4) {
      Map<ByteArrayWrapper, Boolean> entries = new HashMap<ByteArrayWrapper, Boolean>();
      for (int i = 2; i < commandElems.size(); i++)
        entries.put(new ByteArrayWrapper(commandElems.get(i)), true);

      keyRegion.putAll(entries);
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), entries.size()));
    } else {
      Object v = keyRegion.put(new ByteArrayWrapper(commandElems.get(2)), true);
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), v == null ? 1 : 0));
    }

  }

}
