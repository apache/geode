package com.gemstone.gemfire.internal.redis.executor.sortedset;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.executor.AbstractExecutor;

public abstract class SortedSetExecutor extends AbstractExecutor {

  @Override
  protected Region<ByteArrayWrapper, DoubleWrapper> getOrCreateRegion(ExecutionHandlerContext context, ByteArrayWrapper key, RedisDataType type) {
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, DoubleWrapper> r = (Region<ByteArrayWrapper, DoubleWrapper>) context.getRegionProvider().getOrCreateRegion(key, type, context);
    return r;
  }
  
  protected Region<ByteArrayWrapper, DoubleWrapper> getRegion(ExecutionHandlerContext context, ByteArrayWrapper key) {
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, DoubleWrapper> r = (Region<ByteArrayWrapper, DoubleWrapper>) context.getRegionProvider().getRegion(key);
    return r;
  }
  
}
