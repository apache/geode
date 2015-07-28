package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class BitCountExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The indexes provided must be numeric values";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() != 2 && commandElems.size() != 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.BITCOUNT));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper wrapper = r.get(key);
    if (wrapper == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      return;
    }
    byte[] value = wrapper.toBytes();

    long startL = 0;
    long endL = value.length - 1;

    if (commandElems.size() == 4) {
      try {
        startL = Coder.bytesToLong(commandElems.get(2));
        endL = Coder.bytesToLong(commandElems.get(3));
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
        return;
      }
    }
    if (startL > Integer.MAX_VALUE || endL > Integer.MAX_VALUE) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_OUT_OF_RANGE));
      return;
    }

    int start = (int) startL;
    int end = (int) endL;
    if (start < 0)
      start += value.length;
    if (end < 0)
      end += value.length;

    if (start < 0)
      start = 0;
    if (end < 0)
      end = 0;

    if (end > value.length - 1)
      end = value.length - 1;

    if (end < start || start >= value.length) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      return;
    }

    long setBits = 0;
    for (int j = start; j <= end; j++)
      setBits += Integer.bitCount(0xFF & value[j]); // 0xFF keeps same bit sequence as the byte as opposed to keeping the same value

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), setBits));
  }

}
