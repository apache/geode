package com.gemstone.gemfire.internal.redis.executor.string;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisDataTypeMismatchException;
import com.gemstone.gemfire.internal.redis.executor.AbstractExecutor;

public abstract class StringExecutor extends AbstractExecutor {
  
  protected final void checkAndSetDataType(ByteArrayWrapper key, ExecutionHandlerContext context) {
    Object oldVal = context.getRegionProvider().metaPutIfAbsent(key, RedisDataType.REDIS_STRING);
    if (oldVal == RedisDataType.REDIS_PROTECTED)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is protected");
    if (oldVal != null && oldVal != RedisDataType.REDIS_STRING)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is already used by a " + oldVal.toString());
  }
  
  protected void checkDataType(ByteArrayWrapper key, ExecutionHandlerContext context) {
    RedisDataType currentType = context.getRegionProvider().getRedisDataType(key);
    if (currentType == null)
      return;
    if (currentType == RedisDataType.REDIS_PROTECTED)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is protected");
    if (currentType != RedisDataType.REDIS_STRING)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is already used by a " + currentType.toString());
  }
  
}