package com.gemstone.gemfire.internal.redis.executor.string;

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

public class MGetExecutor extends StringExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.MGET));
      return;
    }

    Collection<ByteArrayWrapper> keys = new ArrayList<ByteArrayWrapper>();
    for (int i = 1; i < commandElems.size(); i++) {
      byte[] keyArray = commandElems.get(i);
      ByteArrayWrapper key = new ByteArrayWrapper(keyArray);
      /*
      try {
        checkDataType(key, RedisDataType.REDIS_STRING, context);
      } catch (RedisDataTypeMismatchException e) {
        keys.ad
        continue;
      }
      */
      keys.add(key);
    }

    Map<ByteArrayWrapper, ByteArrayWrapper> results = r.getAll(keys);

    Collection<ByteArrayWrapper> values = new ArrayList<ByteArrayWrapper>();

    /*
     * This is done to preserve order in the output
     */
    for (ByteArrayWrapper key : keys)
      values.add(results.get(key));

    command.setResponse(Coder.getBulkStringArrayResponse(context.getByteBufAllocator(), values));

  }

}
