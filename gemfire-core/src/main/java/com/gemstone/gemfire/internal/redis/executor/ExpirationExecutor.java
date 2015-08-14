package com.gemstone.gemfire.internal.redis.executor;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RegionProvider;


public class ExpirationExecutor implements Runnable {
  private final ByteArrayWrapper key;
  private final RedisDataType type;
  private final RegionProvider rC;

  public ExpirationExecutor(ByteArrayWrapper k, RedisDataType type, RegionProvider rC) {
    this.key = k;
    this.type = type;
    this.rC = rC;
  }

  @Override
  public void run() {
    rC.removeKey(key, type, false);
  }


}
