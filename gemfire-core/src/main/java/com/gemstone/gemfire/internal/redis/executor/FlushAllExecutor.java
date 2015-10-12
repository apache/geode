package com.gemstone.gemfire.internal.redis.executor;

import java.util.Map.Entry;

import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class FlushAllExecutor extends AbstractExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    if (context.hasTransaction())
      throw new UnsupportedOperationInTransactionException();

    for (Entry<String, RedisDataType> e: context.getRegionProvider().metaEntrySet()) {
      try {
        String skey = e.getKey();
        RedisDataType type = e.getValue();
        removeEntry(Coder.stringToByteWrapper(skey), type, context);
      } catch (EntryDestroyedException e1) {
        continue;
      }

    }

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), "OK"));
  }

}
