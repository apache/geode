package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class ZIncrByExecutor extends SortedSetExecutor {

  private final String ERROR_NOT_NUMERIC = "The number provided is not numeric";
  private final String ERROR_NAN = "This increment is illegal because it would result in a NaN";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() != 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZINCRBY));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);

    ByteArrayWrapper member = new ByteArrayWrapper(commandElems.get(3));

    double incr;

    try {
      byte[] incrArray = commandElems.get(2);
      incr = Coder.bytesToDouble(incrArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    DoubleWrapper score = keyRegion.get(member);

    if (score == null) {
      keyRegion.put(member, new DoubleWrapper(incr));
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), incr));
      return;
    }
    double result = score.score + incr;
    if (Double.isNaN(result)) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NAN));
      return;
    }
    score.score = result;
    keyRegion.put(member, score);
    command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), score.score));
  }

}
