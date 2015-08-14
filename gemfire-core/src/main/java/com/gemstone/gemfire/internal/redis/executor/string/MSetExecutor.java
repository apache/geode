package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataTypeMismatchException;
import com.gemstone.gemfire.internal.redis.Coder;

public class MSetExecutor extends StringExecutor {

  private final String SUCCESS = "OK";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 3 || commandElems.size() % 2 == 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.MSET));
      return;
    }

    Map<ByteArrayWrapper, ByteArrayWrapper> map = new HashMap<ByteArrayWrapper, ByteArrayWrapper>();
    for (int i = 1; i < commandElems.size(); i += 2) {
      byte[] keyArray = commandElems.get(i);
      ByteArrayWrapper key = new ByteArrayWrapper(keyArray);
      try {
        checkAndSetDataType(key, context);
      } catch (RedisDataTypeMismatchException e) {
        continue;
      }
      byte[] value = commandElems.get(i + 1);
      map.put(key, new ByteArrayWrapper(value));
    }
    r.putAll(map);

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));

  }

}
