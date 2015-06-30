package com.gemstone.gemfire.internal.redis.executor.list;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;


public class RPopExecutor extends PopExecutor {

  @Override
  protected ListDirection popType() {
    return ListDirection.RIGHT;
  }

  @Override
  public String getArgsError() {
    return ArityDef.RPOP;
  }
  
}
