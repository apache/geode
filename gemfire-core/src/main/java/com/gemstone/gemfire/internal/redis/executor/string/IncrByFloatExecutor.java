package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class IncrByFloatExecutor extends StringExecutor {

  private final String ERROR_VALUE_NOT_USABLE = "Invalid value at this key and cannot be incremented numerically";

  private final String ERROR_INCREMENT_NOT_USABLE = "The increment on this key must be a valid floating point numeric";

  private final String ERROR_OVERFLOW = "This incrementation cannot be performed due to overflow";

  private final int INCREMENT_INDEX = 2;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();
    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.INCRBYFLOAT));
      return;
    }
    
    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper valueWrapper = r.get(key);

    /*
     * Try increment
     */

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    String doub = Coder.bytesToString(incrArray).toLowerCase();
    if (doub.contains("inf") || doub.contains("nan")) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), "Increment would produce NaN or infinity"));
      return;
    } else if (valueWrapper != null && valueWrapper.toString().contains(" ")) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_VALUE_NOT_USABLE));
      return;
    }
    
    
    Double increment;

    try {
      increment = Coder.stringToDouble(doub);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INCREMENT_NOT_USABLE));
      return;
    }

    /*
     * Value does not exist
     */

    if (valueWrapper == null) {
      r.put(key, new ByteArrayWrapper(incrArray));
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), increment));
      return;
    }

    /*
     * Value exists
     */

    String stringValue = Coder.bytesToString(valueWrapper.toBytes());

    Double value;
    try {
      value = Coder.stringToDouble(stringValue);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_VALUE_NOT_USABLE));
      return;
    }

    /*
     * Check for overflow
     */
    if (value >= 0 && increment > (Double.MAX_VALUE - value)) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_OVERFLOW));
      return;
    }

    double result = value + increment;
    if (Double.isNaN(result) || Double.isInfinite(result)) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_NAN_INF_INCR));
      return;
    }
    value += increment;

    stringValue = "" + value;
    r.put(key, new ByteArrayWrapper(Coder.stringToBytes(stringValue)));

    command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), value));
  }

}
