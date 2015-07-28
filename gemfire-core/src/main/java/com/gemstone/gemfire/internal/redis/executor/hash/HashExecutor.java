package com.gemstone.gemfire.internal.redis.executor.hash;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.executor.AbstractExecutor;

public abstract class HashExecutor extends AbstractExecutor {

  protected final int FIELD_INDEX = 2;
  
  @SuppressWarnings("unchecked")
  protected Region<ByteArrayWrapper, ByteArrayWrapper> getOrCreateRegion(ExecutionHandlerContext context, ByteArrayWrapper key, RedisDataType type) {
   return (Region<ByteArrayWrapper, ByteArrayWrapper>) context.getRegionProvider().getOrCreateRegion(key, type, context);
  }
  
  @SuppressWarnings("unchecked")
  protected Region<ByteArrayWrapper, ByteArrayWrapper> getRegion(ExecutionHandlerContext context, ByteArrayWrapper key) {
   return (Region<ByteArrayWrapper, ByteArrayWrapper>) context.getRegionProvider().getRegion(key);
  }
  
}