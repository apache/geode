package com.gemstone.gemfire.internal.redis.executor;

import java.util.Arrays;
import java.util.List;

import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Executor;
import com.gemstone.gemfire.internal.redis.RedisConstants;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class AuthExecutor implements Executor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.AUTH));
      return;
    }
    byte[] pwd = context.getAuthPwd();
    if (pwd == null) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_NO_PASS));
      return;
    }
    boolean correct = Arrays.equals(commandElems.get(1), pwd);

    if (correct) {
      context.setAuthenticationVerified();
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), "OK"));
    } else {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_INVALID_PWD));
    }
  }

}
