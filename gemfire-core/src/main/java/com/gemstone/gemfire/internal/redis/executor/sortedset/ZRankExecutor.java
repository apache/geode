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

public class ZRankExecutor extends SortedSetExecutor implements Extendable {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    ByteArrayWrapper member = new ByteArrayWrapper(commandElems.get(2));

    DoubleWrapper value = keyRegion.get(member);

    if (value == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    int rank;
    try {
      rank = getRange(context, key, member, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), rank));
  }

  private int getRange(ExecutionHandlerContext context, ByteArrayWrapper key, ByteArrayWrapper member, DoubleWrapper valueWrapper) throws Exception {
    Query query;
    if (isReverse())
      query = getQuery(key, SortedSetQuery.ZREVRANK, context);
    else
      query = getQuery(key, SortedSetQuery.ZRANK, context);

    Object[] params = {valueWrapper.score, valueWrapper.score, member};

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    return (Integer) results.asList().get(0);

  }

  protected boolean isReverse() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.ZRANK;
  }

}
