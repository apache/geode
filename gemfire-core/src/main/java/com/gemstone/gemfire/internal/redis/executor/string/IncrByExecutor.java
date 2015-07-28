package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class IncrByExecutor extends StringExecutor {

  private final String ERROR_VALUE_NOT_USABLE = "The value at this key cannot be incremented numerically";

  private final String ERROR_INCREMENT_NOT_USABLE = "The increment on this key must be numeric";

  private final String ERROR_OVERFLOW = "This incrementation cannot be performed due to overflow";

  private final int INCREMENT_INDEX = 2;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.INCRBY));
      return;
    }
    
    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper valueWrapper = r.get(key);

    /*
     * Try increment
     */

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    Long increment;

    try {
      increment = Coder.bytesToLong(incrArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INCREMENT_NOT_USABLE));
      return;
    }

    /*
     * Value does not exist
     */

    if (valueWrapper == null) {
      r.put(key, new ByteArrayWrapper(incrArray));
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), increment));
      return;
    }

    /*
     * Value exists
     */

    String stringValue = Coder.bytesToString(valueWrapper.toBytes());
    Long value;
    try {
      value = Long.parseLong(stringValue);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_VALUE_NOT_USABLE));
      return;
    }

    /*
     * Check for overflow
     */
    if (value >= 0 && increment > (Long.MAX_VALUE - value)) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_OVERFLOW));
      return;
    }

    value += increment;

    stringValue = "" + value;
    r.put(key, new ByteArrayWrapper(Coder.stringToBytes(stringValue)));

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), value));

  }

}
