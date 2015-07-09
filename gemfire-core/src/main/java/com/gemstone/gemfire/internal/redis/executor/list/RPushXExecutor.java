package com.gemstone.gemfire.internal.redis.executor.list;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;


public class RPushXExecutor extends PushXExecutor {
  
  @Override
  protected ListDirection pushType() {
    return ListDirection.RIGHT;
  }

  @Override
  public String getArgsError() {
    return ArityDef.RPUSHX;
  }

}
