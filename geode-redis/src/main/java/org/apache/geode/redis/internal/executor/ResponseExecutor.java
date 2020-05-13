package org.apache.geode.redis.internal.executor;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Executor;
import org.apache.geode.redis.internal.RedisResponse;

public interface ResponseExecutor extends Executor {

  default void executeCommand(Command command, ExecutionHandlerContext context) {
    executeCommandWithResponse(command, context);
  }

  RedisResponse executeCommandWithResponse(Command command, ExecutionHandlerContext context);
}
