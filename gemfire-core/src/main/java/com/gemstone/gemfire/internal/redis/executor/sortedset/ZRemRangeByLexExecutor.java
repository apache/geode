package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.executor.SortedSetQuery;

public class ZRemRangeByLexExecutor extends SortedSetExecutor {

  private final int ERROR_NOT_EXISTS = 0;

  private final String ERROR_ILLEGAL_SYNTAX = "The min and max strings must either start with a (, [ or be - or +";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZREMRANGEBYLEX));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), ERROR_NOT_EXISTS));
      return;
    }

    boolean minInclusive = false;
    boolean maxInclusive = false;

    byte[] minArray = commandElems.get(2);
    byte[] maxArray = commandElems.get(3);
    String startString = Coder.bytesToString(minArray);
    String stopString = Coder.bytesToString(maxArray);

    if (minArray[0] == Coder.OPEN_BRACE_ID) {
      startString = startString.substring(1);
      minInclusive = false;
    } else if (minArray[0] == Coder.OPEN_BRACKET_ID) {
      startString = startString.substring(1);
      minInclusive = true;
    } else if (minArray[0] != Coder.HYPHEN_ID) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_ILLEGAL_SYNTAX));
      return;
    }

    if (maxArray[0] == Coder.OPEN_BRACE_ID) {
      stopString = stopString.substring(1);
      maxInclusive = false;
    } else if (maxArray[0] == Coder.OPEN_BRACKET_ID) {
      stopString = stopString.substring(1);
      maxInclusive = true;
    } else if (maxArray[0] != Coder.PLUS_ID) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_ILLEGAL_SYNTAX));
      return;
    }

    Collection<ByteArrayWrapper> removeList;
    try {
      removeList = getRange(key, keyRegion, context, Coder.stringToByteArrayWrapper(startString), Coder.stringToByteArrayWrapper(stopString), minInclusive, maxInclusive);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    int numRemoved = 0;

    for (ByteArrayWrapper entry: removeList) {
      Object oldVal = keyRegion.remove(entry);
      if (oldVal != null)
        numRemoved++;
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
  }

  private Collection<ByteArrayWrapper> getRange(ByteArrayWrapper key, Region<ByteArrayWrapper, DoubleWrapper> keyRegion, ExecutionHandlerContext context, ByteArrayWrapper start, ByteArrayWrapper stop, boolean startInclusive, boolean stopInclusive) throws Exception {
    if (start.equals("-") && stop.equals("+"))
      return new ArrayList<ByteArrayWrapper>(keyRegion.keySet());
    else if (start.equals("+") || stop.equals("-"))
      return null;

    Query query;
    Object[] params;
    if (start.equals("-")) {
      if (stopInclusive) {
        query = getQuery(key, SortedSetQuery.ZRANGEBYLEXNINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZRANGEBYLEXNINF, context);
      }
      params = new Object[]{stop, INFINITY_LIMIT};
    } else if (stop.equals("+")) {
      if (startInclusive) {
        query = getQuery(key, SortedSetQuery.ZRANGEBYLEXPINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZRANGEBYLEXPINF, context);
      }
      params = new Object[]{start, INFINITY_LIMIT};
    } else {
      if (startInclusive) {
        if(stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZRANGEBYLEXSTISI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZRANGEBYLEXSTI, context);
        }
      } else {
        if (stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZRANGEBYLEXSI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZRANGEBYLEX, context);
        }
      }
      params = new Object[]{start, stop, INFINITY_LIMIT};
    }

    @SuppressWarnings("unchecked")
    SelectResults<ByteArrayWrapper> results = (SelectResults<ByteArrayWrapper>) query.execute(params);

    return results.asList();
  }

}
