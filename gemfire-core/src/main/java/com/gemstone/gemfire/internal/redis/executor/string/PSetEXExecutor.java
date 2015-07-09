package com.gemstone.gemfire.internal.redis.executor.string;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;


public class PSetEXExecutor extends SetEXExecutor {

  @Override
  public boolean timeUnitMillis() {
    return true;
  }
  
  @Override
  public String getArgsError() {
    return ArityDef.PSETEX;
  }

}
