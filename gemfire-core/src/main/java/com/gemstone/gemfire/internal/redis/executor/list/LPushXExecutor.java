package com.gemstone.gemfire.internal.redis.executor.list;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;


public class LPushXExecutor extends PushXExecutor {
  
  @Override
  protected ListDirection pushType() {
    return ListDirection.LEFT;
  }

  @Override
  public String getArgsError() {
    return ArityDef.LPUSHX;
  }

}
