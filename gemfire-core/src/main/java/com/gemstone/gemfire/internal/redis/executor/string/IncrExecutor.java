package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class IncrExecutor extends StringExecutor {

  private final String ERROR_VALUE_NOT_USABLE = "The value at this key cannot be incremented numerically";

  private final String ERROR_OVERFLOW = "This incrementation cannot be performed due to overflow";

  private final int INIT_VALUE_INT = 1;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.INCR));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper valueWrapper = r.get(key);

    /*
     * Value does not exist
     */

    if (valueWrapper == null) {
      byte[] newValue = {Coder.NUMBER_1_BYTE};
      r.put(key, new ByteArrayWrapper(newValue));
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

    if (value == Long.MAX_VALUE) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_OVERFLOW));
      return;
    }

    value++;

    stringValue = "" + value;
    r.put(key, new ByteArrayWrapper(Coder.stringToBytes(stringValue)));


    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), value));

  }

}
