package com.gemstone.gemfire.internal.redis.executor.string;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class GetExecutor extends StringExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    Region<ByteArrayWrapper, ByteArrayWrapper> r= context.getRegionProvider().getStringsRegion();

    if (command.getProcessedCommand().size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.GETEXECUTOR));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_STRING, context);
    ByteArrayWrapper wrapper = r.get(key);

    if (wrapper == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    } else {
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), wrapper.toBytes()));
    }

  }

}
