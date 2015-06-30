package com.gemstone.gemfire.internal.redis.executor.set;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;


public class SDiffStoreExecutor extends SDiffExecutor {

  @Override
  protected boolean isStorage() {
    return true;
  }

  @Override
  public String getArgsError() {
    return ArityDef.SDIFFSTORE;
  }
}
