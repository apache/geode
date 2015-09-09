package com.gemstone.gemfire.internal.redis.executor.hll;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisDataTypeMismatchException;
import com.gemstone.gemfire.internal.redis.executor.AbstractExecutor;

public abstract class HllExecutor extends AbstractExecutor {
  
  public static final Double DEFAULT_HLL_STD_DEV = 0.081;
  public static final Integer DEFAULT_HLL_DENSE = 18;
  public static final Integer DEFAULT_HLL_SPARSE = 32;
  
  protected final void checkAndSetDataType(ByteArrayWrapper key, ExecutionHandlerContext context) {
    Object oldVal = context.getRegionProvider().metaPutIfAbsent(key, RedisDataType.REDIS_HLL);
    if (oldVal == RedisDataType.REDIS_PROTECTED)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is protected");
    if (oldVal != null && oldVal != RedisDataType.REDIS_HLL)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is already used by a " + oldVal.toString());
  }
}
