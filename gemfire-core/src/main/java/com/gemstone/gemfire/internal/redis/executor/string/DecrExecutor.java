package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RegionProvider;

public class DecrExecutor extends StringExecutor {

  private final String ERROR_VALUE_NOT_USABLE = "The value at this key cannot be decremented numerically";

  private final String ERROR_OVERFLOW = "This decrementation cannot be performed due to overflow";

  private final byte[] INIT_VALUE_BYTES = Coder.stringToBytes("-1");

  private final int INIT_VALUE_INT = -1;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    RegionProvider rC = context.getRegionProvider();
    Region<ByteArrayWrapper, ByteArrayWrapper> r = rC.getStringsRegion();;

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.DECR));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper valueWrapper = r.get(key);

    /*
     * Value does not exist
     */

    if (valueWrapper == null) {
      byte[] newValue = INIT_VALUE_BYTES;
      r.put(key, new ByteArrayWrapper(newValue));
      rC.metaPut(key, RedisDataType.REDIS_STRING);
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), INIT_VALUE_INT));
      return;
    }

    /*
     * Value exists
     */

    String stringValue = valueWrapper.toString();
    Long value;
    try {
      value = Long.parseLong(stringValue);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_VALUE_NOT_USABLE));
      return;
    }

    if (value == Long.MIN_VALUE) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_OVERFLOW));
      return;
    }

    value--;

    stringValue = "" + value;

    r.put(key, new ByteArrayWrapper(Coder.stringToBytes(stringValue)));
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), value));

  }

}
