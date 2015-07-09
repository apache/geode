package com.gemstone.gemfire.internal.redis.executor.hash;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class HSetExecutor extends HashExecutor implements Extendable {

  private final int EXISTING_FIELD = 0;

  private final int NEW_FIELD = 1;

  private final int VALUE_INDEX = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_HASH);

    byte[] byteField = commandElems.get(FIELD_INDEX);
    ByteArrayWrapper field = new ByteArrayWrapper(byteField);

    byte[] value = commandElems.get(VALUE_INDEX);

    Object oldValue;

    if (onlySetOnAbsent())
      oldValue = keyRegion.putIfAbsent(field, new ByteArrayWrapper(value));
    else
      oldValue = keyRegion.put(field, new ByteArrayWrapper(value));

    if (oldValue == null)
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NEW_FIELD));
    else
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), EXISTING_FIELD));

  }

  protected boolean onlySetOnAbsent() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.HSET;
  }
}
