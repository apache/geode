package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class GetBitExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The offset provided must be numeric";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.GETBIT));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper wrapper = r.get(key);
    if (wrapper == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      return;
    }

    int bit = 0;
    byte[] bytes = wrapper.toBytes();
    int offset;
    try {
      byte[] offAr = commandElems.get(2);
      offset = Coder.bytesToInt(offAr);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
      return;
    }
    if (offset < 0)
      offset += bytes.length * 8;

    if (offset < 0 || offset > bytes.length * 8) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      return;
    }

    int byteIndex = offset / 8;
    offset %= 8;
    
    if (byteIndex >= bytes.length) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      return;
    }
    
    bit = (bytes[byteIndex] & (0x80 >> offset)) >> (7 - offset);
    
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), bit));
  }

}
