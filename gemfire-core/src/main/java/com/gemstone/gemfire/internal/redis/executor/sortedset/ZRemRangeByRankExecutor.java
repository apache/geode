package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.executor.SortedSetQuery;

public class ZRemRangeByRankExecutor extends SortedSetExecutor {

  private final int NONE_REMOVED = 0;

  private final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZREMRANGEBYRANK));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NONE_REMOVED));
      return;
    }

    int startRank;
    int stopRank;

    try {
      startRank = Coder.bytesToInt(commandElems.get(2));
      stopRank = Coder.bytesToInt(commandElems.get(3));
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    int sSetSize = keyRegion.size();

    startRank = getBoundedStartIndex(startRank, sSetSize);
    stopRank = getBoundedEndIndex(stopRank, sSetSize);
    if (stopRank > sSetSize - 1)
      stopRank = sSetSize - 1;

    if (startRank > stopRank) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      return;
    }

    int numRemoved = 0;
    List<?> removeList = null;
    try {
      if (startRank == 0 && stopRank == sSetSize- 1) {
        numRemoved = keyRegion.size();
        context.getRegionProvider().removeKey(key);
      } else {
        removeList = getRemoveKeys(context, key, startRank, stopRank);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (removeList != null) {
      for (Object entry: removeList) {
        ByteArrayWrapper removeKey;
        if (entry instanceof Entry)
          removeKey = (ByteArrayWrapper) ((Entry<?, ?>) entry).getKey();
        else
          removeKey = (ByteArrayWrapper) ((Struct) entry).getFieldValues()[0];
        Object oldVal = keyRegion.remove(removeKey);
        if (oldVal != null)
          numRemoved++;
      }
      if (keyRegion.isEmpty())
        context.getRegionProvider().removeKey(key);
    }
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
  }

  private List<?> getRemoveKeys(ExecutionHandlerContext context, ByteArrayWrapper key, int startRank, int stopRank) throws Exception {
    Query query = getQuery(key, SortedSetQuery.ZREMRANGEBYRANK, context);
    Object[] params = {stopRank + 1};

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    return results.asList().subList(startRank, stopRank + 1);
  }
}
