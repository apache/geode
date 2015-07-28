package com.gemstone.gemfire.internal.redis.executor;

import java.util.List;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RegionProvider;

public class ExpireExecutor extends AbstractExecutor implements Extendable {

  private final String ERROR_SECONDS_NOT_USABLE = "The number of seconds specified must be numeric";

  private final int SECONDS_INDEX = 2;

  private final int SET = 1;

  private final int NOT_SET = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }
    ByteArrayWrapper wKey = command.getKey();
    RegionProvider rC = context.getRegionProvider();
        byte[] delayByteArray = commandElems.get(SECONDS_INDEX);
    long delay;
    try {
      delay = Coder.bytesToLong(delayByteArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_SECONDS_NOT_USABLE));
      return;
    } 

    if (delay <= 0) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
      return;
    }

    // If time unit given is not in millis convert to millis
    if (!timeUnitMillis())
      delay = delay * millisInSecond;

    boolean expirationSet = false;

    if (rC.hasExpiration(wKey))
      expirationSet = rC.modifyExpiration(wKey, delay);
    else
      expirationSet = rC.setExpiration(wKey, delay);


    if (expirationSet)
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), SET));
    else
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
  }

  /*
   * Overridden by PExpire
   */
  protected boolean timeUnitMillis() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.EXPIRE;
  }

}
