package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.Arrays;
import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class GetRangeExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The indexes provided must be numeric values";

  private final int startIndex = 2;

  private final int stopIndex = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.GETRANGE));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_STRING, context);
    ByteArrayWrapper valueWrapper = r.get(key);

    if (valueWrapper == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    byte[] value = valueWrapper.toBytes();
    int length = value.length;

    long start;
    long end;


    try {
      byte[] startI = commandElems.get(startIndex);
      byte[] stopI = commandElems.get(stopIndex);
      start = Coder.bytesToLong(startI);
      end = Coder.bytesToLong(stopI);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
      return;
    }
    start = getBoundedStartIndex(start, length);
    end = getBoundedEndIndex(end, length);
    
    /*
     * If the properly formatted indexes are illegal, send nil
     */
    if (start > end || start == length) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }
    /*
     *  1 is added to end because the end in copyOfRange is exclusive
     *  but in Redis it is inclusive
     */
    if (end != length) end++;
    byte[] returnRange = Arrays.copyOfRange(value, (int) start, (int) end);
    if (returnRange == null || returnRange.length == 0) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), returnRange));

  }
}
