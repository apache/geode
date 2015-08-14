package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class BitPosExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The numbers provided must be numeric values";

  private final String ERROR_BIT = "The bit must either be a 0 or 1";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.BITPOS));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper string = r.get(key);

    int bit;
    int bitPosition = -1;
    boolean endSet = false;

    try {
      byte[] bitAr = commandElems.get(2);
      bit = Coder.bytesToInt(bitAr);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
      return;
    }

    if (bit != 0 && bit != 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_BIT));
      return;
    }

    if (string == null || string.length() == 0) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), -bit)); // Redis returns 0 when key does not exists for this command
      return;
    }
    byte[] bytes = string.toBytes();
    int start = 0;
    int end = bytes.length - 1;
    if (commandElems.size() > 3) {
      try {
        byte[] startAr = commandElems.get(3);
        start = Coder.bytesToInt(startAr);
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
        return;
      }
    }


    if (commandElems.size() > 4) {
      try {
        byte[] endAr = commandElems.get(4);
        end = Coder.bytesToInt(endAr);
        endSet = true;
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
        return;
      }
    }

    if (start < 0)
      start += bytes.length;
    if (end < 0)
      end += bytes.length;

    if (start < 0)
      start = 0;
    if (end < 0)
      end = 0;

    if (start > bytes.length)
      start = bytes.length - 1;
    if (end > bytes.length)
      end = bytes.length - 1;

    if (end < start) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), -1));
      return;
    }

    outerLoop:
      for (int i = start; i <= end; i++) {
        int cBit;
        byte cByte = bytes[i];
        for (int j = 0; j < 8; j++) {
          cBit = (cByte & (0x80 >> j)) >> (7 - j);
    if (cBit ==  bit) {
      bitPosition = 8 * i + j;
      break outerLoop;
    }
        }
      }

    if (bit == 0 && bitPosition == -1 && !endSet)
      bitPosition = bytes.length * 8;

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), bitPosition));
  }

}
