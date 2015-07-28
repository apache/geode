package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import com.gemstone.gemfire.cache.Region;
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

public class ZRemRangeByScoreExecutor extends SortedSetExecutor {

  private final String ERROR_NOT_NUMERIC = "The number provided is not numeric";

  private final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZREMRANGEBYSCORE));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
      return;
    }

    boolean startInclusive = true;
    boolean stopInclusive = true;
    double start;
    double stop;

    byte[] startArray = commandElems.get(2);
    byte[] stopArray = commandElems.get(3);
    String startString = Coder.bytesToString(startArray);
    String stopString = Coder.bytesToString(stopArray);
    if (startArray[0] == Coder.OPEN_BRACE_ID) {
      startString = startString.substring(1);
      startInclusive = false;
    }
    if (stopArray[0] == Coder.OPEN_BRACE_ID) {
      stopString = stopString.substring(1);
      stopInclusive = false;
    }

    try {
      start = Coder.stringToDouble(startString);
      stop = Coder.stringToDouble(stopString);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    int numRemoved = 0;

    Collection<?> removeList = null;
    try {
      if (start == Double.NEGATIVE_INFINITY && stop == Double.POSITIVE_INFINITY && startInclusive && stopInclusive) {
        numRemoved = keyRegion.size();
        context.getRegionProvider().removeKey(key);
      } else {
        removeList = getKeys(context, key, keyRegion, start, stop, startInclusive, stopInclusive);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (removeList != null) {
      for (Object entry: removeList) {
        ByteArrayWrapper remove = null;
        if (entry instanceof Entry)
          remove = (ByteArrayWrapper) ((Entry<?, ?>) entry).getKey();
        else if (entry instanceof Struct)
          remove = (ByteArrayWrapper) ((Struct) entry).getFieldValues()[0];
        Object oldVal = keyRegion.remove(remove);
        if (oldVal != null)
          numRemoved++;
        if (keyRegion.isEmpty())
          context.getRegionProvider().removeKey(key);
      }
    }
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
  }

  private Collection<?> getKeys(ExecutionHandlerContext context, ByteArrayWrapper key, Region<ByteArrayWrapper, DoubleWrapper> keyRegion, double start, double stop, boolean startInclusive, boolean stopInclusive) throws Exception {
    if (start == Double.POSITIVE_INFINITY || stop == Double.NEGATIVE_INFINITY || (start > stop))
      return null;

    Query query;
    Object[] params;
    if (startInclusive) {
      if(stopInclusive) {
        query = getQuery(key, SortedSetQuery.ZRBSSTISI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZRBSSTI, context);
      }
    } else {
      if (stopInclusive) {
        query = getQuery(key, SortedSetQuery.ZRBSSI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZRBS, context);
      }
    }
    params = new Object[]{start, stop, INFINITY_LIMIT};

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    return (Collection<?>) results.asList();
  }
}
