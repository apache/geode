package org.apache.geode.redis.internal.executor;

import org.apache.geode.redis.internal.ByteArrayWrapper;

public interface RedisKeyCommands {
  boolean del(ByteArrayWrapper key);

  boolean exists(ByteArrayWrapper key);
}
