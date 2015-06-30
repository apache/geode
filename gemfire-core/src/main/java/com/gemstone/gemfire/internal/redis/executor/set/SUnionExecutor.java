package com.gemstone.gemfire.internal.redis.executor.set;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class SUnionExecutor extends SetOpExecutor {

  @Override
  protected boolean isStorage() {
    return false;
  }

  @Override
  protected Set<ByteArrayWrapper> setOp(Set<ByteArrayWrapper> firstSet, List<Set<ByteArrayWrapper>> setList) {
    Set<ByteArrayWrapper> addSet = firstSet;
    for (Set<ByteArrayWrapper> set: setList) {
      if (addSet == null) {
        addSet = new HashSet<ByteArrayWrapper>(set);
        continue;
      }
      addSet.addAll(set);
    }
    return addSet;
  }

  @Override
  public String getArgsError() {
    return ArityDef.SUNION;
  }

}
