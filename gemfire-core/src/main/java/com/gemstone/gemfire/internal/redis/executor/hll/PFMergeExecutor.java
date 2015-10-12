package com.gemstone.gemfire.internal.redis.executor.hll;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class PFMergeExecutor extends HllExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.PFMERGE));
      return;
    }

    ByteArrayWrapper destKey = command.getKey();
    checkAndSetDataType(destKey, context);
    Region<ByteArrayWrapper, HyperLogLogPlus> keyRegion = context.getRegionProvider().gethLLRegion();
    HyperLogLogPlus mergedHLL = keyRegion.get(destKey);
    if (mergedHLL == null)
      mergedHLL = new HyperLogLogPlus(DEFAULT_HLL_DENSE);
    List<HyperLogLogPlus> hlls = new ArrayList<HyperLogLogPlus>();

    for (int i = 2; i < commandElems.size(); i++) {
      ByteArrayWrapper k = new ByteArrayWrapper(commandElems.get(i));
      checkDataType(k, RedisDataType.REDIS_HLL, context);
      HyperLogLogPlus h = keyRegion.get(k);
      if (h != null)
        hlls.add(h);
    }
    if (hlls.isEmpty()) {
      context.getRegionProvider().removeKey(destKey);
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), "OK"));
      return;
    }

    HyperLogLogPlus[] estimators = hlls.toArray(new HyperLogLogPlus[hlls.size()]);
    try {
      mergedHLL = (HyperLogLogPlus) mergedHLL.merge(estimators);
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
    keyRegion.put(destKey, mergedHLL);
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), "OK"));
  }

}
