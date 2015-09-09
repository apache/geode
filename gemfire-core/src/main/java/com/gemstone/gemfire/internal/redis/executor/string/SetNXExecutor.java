package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class SetNXExecutor extends StringExecutor {

  private final int SET = 1;

  private final int NOT_SET = 0;

  private final int VALUE_INDEX = 2;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SETNX));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    byte[] value = commandElems.get(VALUE_INDEX);

    Object oldValue = r.putIfAbsent(key, new ByteArrayWrapper(value));

    if (oldValue != null)
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
    else
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), SET));

  }

}
