package com.gemstone.gemfire.internal.redis.executor.list;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.executor.ListQuery;

public class LRangeExecutor extends ListExecutor {

  private final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LRANGE));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] startArray = commandElems.get(2);
    byte[] stopArray = commandElems.get(3);

    int redisStart;
    int redisStop;


    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region<Integer, ByteArrayWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }

    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;
    if (listSize == 0) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }

    try {
      redisStart = Coder.bytesToInt(startArray);
      redisStop =  Coder.bytesToInt(stopArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }


    redisStart = getBoundedStartIndex(redisStart, listSize);
    redisStop = getBoundedEndIndex(redisStop, listSize);
    if (redisStart > redisStop) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }
    redisStart = Math.min(redisStart, listSize - 1);
    redisStop = Math.min(redisStop, listSize - 1);
   
    
    List<Struct> range;
    try {
      range = getRange(context, key, redisStart, redisStop, keyRegion);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (range == null)
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
    else
      command.setResponse(Coder.getBulkStringArrayResponseOfValues(context.getByteBufAllocator(), range));
  }

  private List<Struct> getRange(ExecutionHandlerContext context, ByteArrayWrapper key, int start, int stop, Region r) throws Exception {

    Query query = getQuery(key, ListQuery.LRANGE, context);

    Object[] params = {Integer.valueOf(stop + 1)};
    SelectResults<Struct> results = (SelectResults<Struct>) query.execute(params);
    int size = results.size();
    if (results == null || size <= start) {
      return null;
    }

    return results.asList().subList(start, size);
  }
}
