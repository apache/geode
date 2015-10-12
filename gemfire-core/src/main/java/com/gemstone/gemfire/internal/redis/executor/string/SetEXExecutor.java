package com.gemstone.gemfire.internal.redis.executor.string;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class SetEXExecutor extends StringExecutor implements Extendable {

  private final String ERROR_SECONDS_NOT_A_NUMBER = "The expiration argument provided was not a number";

  private final String ERROR_SECONDS_NOT_LEGAL = "The expiration argument must be greater than 0";

  private final String SUCCESS = "OK";

  private final int VALUE_INDEX = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] value = commandElems.get(VALUE_INDEX);

    byte[] expirationArray = commandElems.get(2);
    long expiration;
    try {
      expiration = Coder.bytesToLong(expirationArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_SECONDS_NOT_A_NUMBER));
      return;
    }

    if (expiration <= 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_SECONDS_NOT_LEGAL));
      return;
    }

    if (!timeUnitMillis())
      expiration *= millisInSecond;

    checkAndSetDataType(key, context);
    r.put(key, new ByteArrayWrapper(value));

    context.getRegionProvider().setExpiration(key, expiration);

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));

  }

  protected boolean timeUnitMillis() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.SETEX;
  }

}
