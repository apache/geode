package com.gemstone.gemfire.internal.redis.executor;

import java.util.List;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class PersistExecutor extends AbstractExecutor {

  private final int TIMEOUT_REMOVED = 1;
  
  private final int KEY_NOT_EXIST_OR_NO_TIMEOUT = 0;
  
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.PERSIST));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    
    boolean canceled = context.getRegionProvider().cancelKeyExpiration(key);
    
    if (canceled)
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), TIMEOUT_REMOVED));
    else
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), KEY_NOT_EXIST_OR_NO_TIMEOUT));
  }

}
