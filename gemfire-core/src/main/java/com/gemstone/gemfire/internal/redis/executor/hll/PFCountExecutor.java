package com.gemstone.gemfire.internal.redis.executor.hll;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public class PFCountExecutor extends HllExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.PFCOUNT));
      return;
    }

    Region<ByteArrayWrapper, HyperLogLogPlus> keyRegion = context.getRegionProvider().gethLLRegion();

    List<HyperLogLogPlus> hlls = new ArrayList<HyperLogLogPlus>();

    for (int i = 1; i < commandElems.size(); i++) {
      ByteArrayWrapper k = new ByteArrayWrapper(commandElems.get(i));
      checkDataType(k, RedisDataType.REDIS_HLL, context);
      HyperLogLogPlus h = keyRegion.get(k);
      if (h != null)
        hlls.add(h);
    }
    if (hlls.isEmpty()) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      return;
    }

    HyperLogLogPlus tmp = hlls.remove(0);
    HyperLogLogPlus[] estimators = hlls.toArray(new HyperLogLogPlus[hlls.size()]);
    try {
      tmp = (HyperLogLogPlus) tmp.merge(estimators);
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
    long cardinality = tmp.cardinality();
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), cardinality));
  }

}
