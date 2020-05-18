package org.apache.geode.redis.internal.executor;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisData;

public class RedisKeyCommandsFunctionExecutor implements RedisKeyCommands {
  private Region<ByteArrayWrapper, RedisData> region;

  public RedisKeyCommandsFunctionExecutor(
      Region<ByteArrayWrapper, RedisData> region) {
    this.region = region;
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    return (boolean) CommandFunction.execute(RedisCommandType.DEL, key, new Object[] {}, region);
  }

  @Override
  public boolean exists(ByteArrayWrapper key) {
    return (boolean) CommandFunction.execute(RedisCommandType.EXISTS, key, new Object[] {}, region);
  }
}
