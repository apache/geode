package com.gemstone.gemfire.internal.redis.executor.set;

import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class SInterExecutor extends SetOpExecutor {
  
  @Override
  protected boolean isStorage() {
    return false;
  }

  @Override
  protected Set<ByteArrayWrapper> setOp(Set<ByteArrayWrapper> firstSet, List<Set<ByteArrayWrapper>> setList) {
    if (firstSet == null)
      return null;
    for (Set<ByteArrayWrapper> set: setList) {
      if (set == null || set.isEmpty())
        return null;
      firstSet.retainAll(set);
    }
    return firstSet;
  }

  @Override
  public String getArgsError() {
    return ArityDef.SINTER;
  }

}
