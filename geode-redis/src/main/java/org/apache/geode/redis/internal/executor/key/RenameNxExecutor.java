package org.apache.geode.redis.internal.executor.key;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NO_SUCH_KEY;

import java.util.List;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class RenameNxExecutor extends AbstractExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    List<ByteArrayWrapper> commandElems = command.getProcessedCommandWrappers();
    ByteArrayWrapper key = command.getKey();
    ByteArrayWrapper newKey = commandElems.get(2);
    RedisKeyCommands redisKeyCommands = getRedisKeyCommands(context);

    if (redisKeyCommands.exists(newKey)) {
      return RedisResponse.integer(0);
    }

    if (!redisKeyCommands.rename(key, newKey)) {
      return RedisResponse.error(ERROR_NO_SUCH_KEY);
    }

    return RedisResponse.integer(1);
  }
}
