package com.gemstone.gemfire.internal.redis.executor.set;

import java.util.List;
import java.util.Random;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class SPopExecutor extends SetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SPOP));
      return;
    }
    
    ByteArrayWrapper key = command.getKey();
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, Boolean> keyRegion = (Region<ByteArrayWrapper, Boolean>) context.getRegionProvider().getRegion(key);
    if (keyRegion == null || keyRegion.isEmpty()) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }
    
    Random rand = new Random();
    
    ByteArrayWrapper[] entries = keyRegion.keySet().toArray(new ByteArrayWrapper[keyRegion.size()]);
    
    ByteArrayWrapper pop = entries[rand.nextInt(entries.length)];
    
    keyRegion.remove(pop);
    if (keyRegion.isEmpty()) {
      context.getRegionProvider().removeKey(key);
    }
    command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), pop.toBytes()));
  }

}
