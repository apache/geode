package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.executor.SortedSetQuery;

public class ZRangeExecutor extends SortedSetExecutor implements Extendable {

  private final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    boolean withScores = false;

    if (commandElems.size() >= 5) {
      byte[] fifthElem = commandElems.get(4);
      withScores = Coder.bytesToString(fifthElem).equalsIgnoreCase("WITHSCORES");

    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }


    int start;
    int stop;
    int sSetSize = keyRegion.size();

    try {
      byte[] startArray = commandElems.get(2);
      byte[] stopArray = commandElems.get(3);
      start = Coder.bytesToInt(startArray);
      stop = Coder.bytesToInt(stopArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    start = getBoundedStartIndex(start, sSetSize);
    stop = getBoundedEndIndex(stop, sSetSize);

    if (start > stop || start == sSetSize) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }
    if (stop == sSetSize)
      stop--;
    List<?> list;
    try {
      list = getRange(context, key, start, stop);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    command.setResponse(Coder.zRangeResponse(context.getByteBufAllocator(), list, withScores));
  }

  private List<?> getRange(ExecutionHandlerContext context, ByteArrayWrapper key, int start, int stop) throws Exception {
    Query query;

    if (isReverse())
      query = getQuery(key, SortedSetQuery.ZRANGE, context);
    else
      query = getQuery(key, SortedSetQuery.ZREVRANGE, context);

    Object[] params = {stop + 1};

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    List<?> list = results.asList();

    return list.subList(start, stop + 1);

  }

  protected boolean isReverse() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.ZRANGE;
  }
}
