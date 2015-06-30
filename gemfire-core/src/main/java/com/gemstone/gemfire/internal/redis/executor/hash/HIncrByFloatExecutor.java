package com.gemstone.gemfire.internal.redis.executor.hash;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class HIncrByFloatExecutor extends HashExecutor {

  private final String ERROR_FIELD_NOT_USABLE = "The value at this field cannot be incremented numerically because it is not a float";

  private final String ERROR_INCREMENT_NOT_USABLE = "The increment on this key must be floating point numeric";

  private final int FIELD_INDEX = 2;

  private final int INCREMENT_INDEX = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HINCRBYFLOAT));
      return;
    }

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    Double increment;

    try {
      increment = Coder.bytesToDouble(incrArray);
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
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), increment));
      return;
    }

    /*
     * If the field did exist then increment the field
     */
    String valueS = oldValue.toString();
    if (valueS.contains(" ")) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_FIELD_NOT_USABLE));
      return;
    }
    Double value;

    try {
      value = Coder.stringToDouble(valueS);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_FIELD_NOT_USABLE));
      return;
    }

    value += increment;
    keyRegion.put(field, new ByteArrayWrapper(Coder.doubleToBytes(value)));
    command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), value));
  }

}
