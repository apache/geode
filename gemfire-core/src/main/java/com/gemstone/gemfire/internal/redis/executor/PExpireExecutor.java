package com.gemstone.gemfire.internal.redis.executor;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class PExpireExecutor extends ExpireExecutor {
  
  @Override
  protected boolean timeUnitMillis() {
    return true;
  }
  
  @Override
  public String getArgsError() {
    return ArityDef.PEXPIRE;
  }
}
