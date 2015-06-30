package com.gemstone.gemfire.internal.redis.executor.hash;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;


public class HSetNXExecutor extends HSetExecutor {

  @Override
  protected boolean onlySetOnAbsent() {
    return true;
  }

  @Override
  public String getArgsError() {
    return ArityDef.HSETNX;
  }
}
