package com.gemstone.gemfire.internal.redis.executor.set;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;


public class SInterStoreExecutor extends SInterExecutor {
  
  @Override
  protected boolean isStorage() {
    return true;
  }
  
  @Override
  public String getArgsError() {
    return ArityDef.SINTERSTORE;
  }

}
