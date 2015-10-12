package com.gemstone.gemfire.internal.redis.executor;

import java.util.List;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RegionProvider;

public class TTLExecutor extends AbstractExecutor implements Extendable {

  private final int NOT_EXISTS = -2;
  
  private final int NO_TIMEOUT = -1;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    RegionProvider rC = context.getRegionProvider();
    boolean exists = false;
    RedisDataType val = rC.getRedisDataType(key);
    if (val != null)
      exists = true;

    if (!exists) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
      return;
    }
    long ttl = rC.getExpirationDelayMillis(key);
    
    if (ttl == 0L) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NO_TIMEOUT));
      return;
    }
    
    if(!timeUnitMillis())
      ttl = ttl / millisInSecond;

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), ttl));
  }

  protected boolean timeUnitMillis() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.TTL;
  }
}
