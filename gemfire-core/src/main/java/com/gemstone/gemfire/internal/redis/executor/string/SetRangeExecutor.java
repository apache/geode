package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class SetRangeExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The number provided must be numeric";

  private final String ERROR_ILLEGAL_OFFSET = "The offset is out of range, must be greater than or equal to 0 and the offset added to the length of the value must be less than 536870911 (512MB), the maximum allowed size";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SETRANGE));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper wrapper = r.get(key);

    int offset;
    byte[] value = commandElems.get(3);
    try {
      byte[] offAr = commandElems.get(2);
      offset = Coder.bytesToInt(offAr);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
      return;
    }

    int totalLength = offset + value.length;
    if (offset < 0 || totalLength > 536870911) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_ILLEGAL_OFFSET));
      return;
    } else if (value.length == 0) {
      int length = wrapper == null ? 0 : wrapper.toBytes().length;
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), length));
      if (wrapper == null)
        context.getRegionProvider().removeKey(key);
      return;
    }

    if (wrapper == null) {
      byte[] bytes = new byte[totalLength];
      System.arraycopy(value, 0, bytes, offset, value.length);
      r.put(key, new ByteArrayWrapper(bytes));
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), bytes.length));
    } else {

      byte[] bytes = wrapper.toBytes();
      int returnLength;
      if (totalLength < bytes.length) {
        System.arraycopy(value, 0, bytes, offset, value.length);
        r.put(key, new ByteArrayWrapper(bytes));
        returnLength = bytes.length;
      } else {
        byte[] newBytes = new byte[totalLength];
        System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
        System.arraycopy(value, 0, newBytes, offset, value.length);
        returnLength = newBytes.length;
        r.put(key, new ByteArrayWrapper(newBytes));
      }

      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), returnLength));
    }
  }

}
