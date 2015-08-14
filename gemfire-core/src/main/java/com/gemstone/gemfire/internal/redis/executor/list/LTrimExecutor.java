package com.gemstone.gemfire.internal.redis.executor.list;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisCommandType;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.executor.ListQuery;

public class LTrimExecutor extends ListExecutor {

  private final String ERROR_KEY_NOT_EXISTS = "The key does not exists on this server";

  private final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  private final String SUCCESS = "OK";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LTRIM));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] startArray = commandElems.get(2);
    byte[] stopArray = commandElems.get(3);

    int redisStart;
    int redisStop;


    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_KEY_NOT_EXISTS));
      return;
    }

    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;
    if (listSize == 0) {
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
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
    redisStart = Math.min(redisStart, listSize - 1);
    redisStop = Math.min(redisStop, listSize - 1);

    if (redisStart == 0 && redisStop == listSize - 1) {
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
      return;
    } else if (redisStart == 0 && redisStop < redisStart) {
      context.getRegionProvider().removeKey(key, RedisDataType.REDIS_LIST);
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
      return;
    }

    List<Integer> keepList;
    try {
      keepList = getRange(context, key, redisStart, redisStop, keyRegion);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    for (Object keyElement: keyRegion.keySet()) {
      if (!keepList.contains(keyElement) && keyElement instanceof Integer)
        keyRegion.remove(keyElement);
    }
    
    // Reset indexes in meta data region
    keyRegion.put("head", keepList.get(0));
    keyRegion.put("tail", keepList.get(keepList.size() - 1));
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
  }

  private List<Integer> getRange(ExecutionHandlerContext context, ByteArrayWrapper key, int start, int stop, Region r) throws Exception {
    Query query = getQuery(key, ListQuery.LTRIM, context);

    Object[] params = {Integer.valueOf(stop + 1)};
    
    SelectResults<Integer> results = (SelectResults<Integer>) query.execute(params);
    if (results == null || results.size() <= start) {
      return null;
    }

    return results.asList().subList(start, results.size());
  }
}
