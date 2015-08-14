package com.gemstone.gemfire.internal.redis.executor.hll;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class PFAddExecutor extends HllExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.PFADD));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    Region<ByteArrayWrapper, HyperLogLogPlus> keyRegion = context.getRegionProvider().gethLLRegion();

    HyperLogLogPlus hll = keyRegion.get(key);

    boolean changed = false;

    if (hll == null)
      hll = new HyperLogLogPlus(DEFAULT_HLL_DENSE);

    for (int i = 2; i < commandElems.size(); i++) {
      byte[] bytes = commandElems.get(i);
      boolean offerChange = hll.offer(bytes);
      if (offerChange)
        changed = true;
    }

    keyRegion.put(key, hll);

    if (changed)
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 1));
    else
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
  }

}
