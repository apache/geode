package com.gemstone.gemfire.internal.redis.executor.list;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.executor.ListQuery;

public class LSetExecutor extends ListExecutor {

  private final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  private final String ERROR_INDEX = "The index provided is not within range of this list or the key does not exist";

  private final String SUCCESS = "OK";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LSET));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] indexArray = commandElems.get(2);
    byte[] value = commandElems.get(3);

    int index;


    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region<Integer, ByteArrayWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INDEX));
      return;
    }

    try {
      index = Coder.bytesToInt(indexArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;
    if (index < 0)
      index += listSize;
    if (index < 0 || index > listSize) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INDEX));
      return;
    }

    Integer indexKey;
    try {
      indexKey = getIndexKey(context, key, index);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (indexKey == null) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INDEX));
      return;
    }
    if (index == listSize)
      indexKey++;
    keyRegion.put(indexKey, new ByteArrayWrapper(value));
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
  }

  private Integer getIndexKey(ExecutionHandlerContext context, ByteArrayWrapper key, int index) throws Exception {
    Query query = getQuery(key, ListQuery.LSET, context);

    Object[] params = {Integer.valueOf(index + 1)};
    
    SelectResults<Integer> results = (SelectResults<Integer>) query.execute(params);
    int size = results.size();
    if (results == null || size == 0) {
      return null;
    }

    return results.asList().get(size - 1);
  }
}
