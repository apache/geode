package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.executor.SortedSetQuery;

public class ZCountExecutor extends SortedSetExecutor {

  private final String ERROR_NOT_NUMERIC = "The number provided is not numeric";

  private final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZCOUNT));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);
    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);

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


    int count;
    try {
      count = getCount(key, keyRegion, context, start, stop, startInclusive, stopInclusive);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }


    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), count));
  }

  private int getCount(ByteArrayWrapper key, Region<ByteArrayWrapper, DoubleWrapper> keyRegion, ExecutionHandlerContext context, double start, double stop, boolean startInclusive, boolean stopInclusive) throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    if (start == Double.NEGATIVE_INFINITY && stop == Double.POSITIVE_INFINITY)
      return keyRegion.size();
    else if (start == Double.POSITIVE_INFINITY || stop == Double.NEGATIVE_INFINITY)
      return 0;

    Query query;
    Object[] params;
    if (start == Double.NEGATIVE_INFINITY) {
      if (stopInclusive) {
        query = getQuery(key, SortedSetQuery.ZCOUNTNINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZCOUNTNINF, context);
      }
      params = new Object[]{stop};
    } else if (stop == Double.POSITIVE_INFINITY) {
      if (startInclusive) {
        query = getQuery(key, SortedSetQuery.ZCOUNTPINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZCOUNTPINF, context);
      }
      params = new Object[]{start};
    } else {
      if (startInclusive) {
        if(stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZCOUNTSTISI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZCOUNTSTI, context);
        }
      } else {
        if (stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZCOUNTSI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZCOUNT, context);
        }
      }
      params = new Object[]{start, stop};
    }

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    return (Integer) results.asList().get(0);
  }

}
