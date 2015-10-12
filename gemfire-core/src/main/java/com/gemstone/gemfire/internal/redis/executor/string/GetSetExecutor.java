package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class GetSetExecutor extends StringExecutor {

  private final int VALUE_INDEX = 2;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.GETSET));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);

    byte[] newCharValue = commandElems.get(VALUE_INDEX);
    ByteArrayWrapper newValueWrapper = new ByteArrayWrapper(newCharValue);

    ByteArrayWrapper oldValueWrapper = r.get(key);
    r.put(key, newValueWrapper);

    if (oldValueWrapper == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
    } else {
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), oldValueWrapper.toBytes()));
    }

  }
}
