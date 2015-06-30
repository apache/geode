package com.gemstone.gemfire.internal.redis.executor.hash;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class HIncrByExecutor extends HashExecutor {

  private final String ERROR_FIELD_NOT_USABLE = "The value at this field is not an integer";

  private final String ERROR_INCREMENT_NOT_USABLE = "The increment on this key must be numeric";

  private final String ERROR_OVERFLOW = "This incrementation cannot be performed due to overflow";

  private final int FIELD_INDEX = 2;

  private final int INCREMENT_INDEX = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HINCRBY));
      return;
    }

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    long increment;

    try {
      increment = Coder.bytesToLong(incrArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INCREMENT_NOT_USABLE));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_HASH);

    byte[] byteField = commandElems.get(FIELD_INDEX);
    ByteArrayWrapper field = new ByteArrayWrapper(byteField);

    /*
     * Put incrememnt as value if field doesn't exist
     */

    ByteArrayWrapper oldValue = keyRegion.get(field);

    if (oldValue == null) {
      keyRegion.put(field, new ByteArrayWrapper(incrArray));
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), increment));
      return;
    }

    /*
     * If the field did exist then increment the field
     */

    long value;

    try {
      value = Long.parseLong(oldValue.toString());
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_FIELD_NOT_USABLE));
      return;
    }

    /*
     * Check for overflow
     */
    if ((value >= 0 && increment > (Long.MAX_VALUE - value)) || (value <= 0 && increment < (Long.MIN_VALUE - value))) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_OVERFLOW));
      return;
    }

    value += increment;
    //String newValue = String.valueOf(value);

    keyRegion.put(field, new ByteArrayWrapper(Coder.longToBytes(value)));

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), value));

  }

}
