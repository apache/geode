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

public class MSetNXExecutor extends StringExecutor {

  private final int SET = 1;

  private final int NOT_SET = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 3 || commandElems.size() % 2 == 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.MSETNX));
      return;
    }

    boolean hasEntry = false;

    Map<ByteArrayWrapper, ByteArrayWrapper> map = new HashMap<ByteArrayWrapper, ByteArrayWrapper>();
    for (int i = 1; i < commandElems.size(); i += 2) {
      byte[] keyArray = commandElems.get(i);
      ByteArrayWrapper key = new ByteArrayWrapper(keyArray);
      try {
        checkDataType(key, context);
      } catch (RedisDataTypeMismatchException e) {
        hasEntry = true;
        break;
      }
      byte[] value = commandElems.get(i + 1);
      map.put(key, new ByteArrayWrapper(value));
      if (r.containsKey(key)) {
        hasEntry = true;
        break;
      }
    }
    boolean successful = false;
    if (!hasEntry) {
      successful = true;
      for (ByteArrayWrapper k : map.keySet()) {
        try {
          checkAndSetDataType(k, context);
        } catch (RedisDataTypeMismatchException e) {
          successful = false;
          break;
        }
      }
      r.putAll(map);
    }
    if (successful) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), SET));
    } else {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
    }

  }

}
