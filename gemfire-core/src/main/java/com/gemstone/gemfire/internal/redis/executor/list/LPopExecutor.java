package com.gemstone.gemfire.internal.redis.executor.list;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;


public class LPopExecutor extends PopExecutor {

  @Override
  protected ListDirection popType() {
    return ListDirection.LEFT;
  }

  @Override
  public String getArgsError() {
    return ArityDef.LPOP;
  }

}
