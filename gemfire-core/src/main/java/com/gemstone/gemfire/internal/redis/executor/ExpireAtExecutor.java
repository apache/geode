package com.gemstone.gemfire.internal.redis.executor;

import java.util.List;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RegionProvider;

public class ExpireAtExecutor extends AbstractExecutor implements Extendable {

  private final String ERROR_TIMESTAMP_NOT_USABLE = "The timestamp specified must be numeric";

  private final int TIMESTAMP_INDEX = 2;

  private final int SET = 1;

  private final int NOT_SET = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }
    RegionProvider rC = context.getRegionProvider();
    ByteArrayWrapper wKey = command.getKey();

    byte[] timestampByteArray = commandElems.get(TIMESTAMP_INDEX);
    long timestamp;
    try {
      timestamp = Coder.bytesToLong(timestampByteArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_TIMESTAMP_NOT_USABLE));
      return;
    }

    if (!timeUnitMillis())
      timestamp = timestamp * millisInSecond;

    long currentTimeMillis = System.currentTimeMillis();
    
    if (timestamp <= currentTimeMillis) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
      return;
    }

    long delayMillis = timestamp - currentTimeMillis;

    boolean expirationSet = false;

    if (rC.hasExpiration(wKey))
      expirationSet = rC.modifyExpiration(wKey, delayMillis);
    else
      expirationSet = rC.setExpiration(wKey, delayMillis);

    if (expirationSet)
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), SET));
    else
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
  }

  protected boolean timeUnitMillis() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.EXPIREAT;
  }

}
