package com.gemstone.gemfire.internal.redis.executor;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RegionCache;


public class ExpirationExecutor implements Runnable {
  private final ByteArrayWrapper key;
  private final RedisDataType type;
  private final RegionCache rC;

  public ExpirationExecutor(ByteArrayWrapper k, RedisDataType type, RegionCache rC) {
    this.key = k;
    this.type = type;
    this.rC = rC;
  }

  @Override
  public void run() {
    rC.removeKey(key, type, false);
  }


}
