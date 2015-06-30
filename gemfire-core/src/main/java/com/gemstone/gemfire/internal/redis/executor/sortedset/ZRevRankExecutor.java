package com.gemstone.gemfire.internal.redis.executor.sortedset;

import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class ZRevRankExecutor extends ZRankExecutor {

  @Override
  protected boolean isReverse() {
    return true;
  }

  @Override
  public String getArgsError() {
    return ArityDef.ZREVRANK;
  }
}
