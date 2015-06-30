package com.gemstone.gemfire.internal.redis.executor.list;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;


public class RPushExecutor extends PushExecutor {

  @Override
  protected ListDirection pushType() {
    return ListDirection.RIGHT;
  }

  @Override
  public String getArgsError() {
    return ArityDef.RPUSH;
  } 

}
