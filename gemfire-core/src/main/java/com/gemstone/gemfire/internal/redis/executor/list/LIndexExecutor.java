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

public class LIndexExecutor extends ListExecutor {

  private final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LINDEX));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] indexArray = commandElems.get(2);

    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region<Integer, ByteArrayWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;

    Integer redisIndex;

    try {
      redisIndex = Coder.bytesToInt(indexArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    } 

    /*
     * Now the fun part, converting the redis index into our index.
     * The redis index is 0 based but negative values count from the tail
     */

    if (redisIndex < 0)
      // Since the redisIndex is negative here, this will reset it to be a standard 0 based index
      redisIndex = listSize + redisIndex;

    /*
     * If the index is still less than 0 that means the index has shot off
     * back past the beginning, which means the index isn't real and a nil is returned
     */
    if (redisIndex < 0) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    /*
     * Now we must get that element from the region
     */
    Struct entry;
    try {
      entry = getEntryAtIndex(context, key, redisIndex);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (entry == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    Object[] entryArray = entry.getFieldValues();
    ByteArrayWrapper valueWrapper = (ByteArrayWrapper) entryArray[1];
    command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), valueWrapper.toBytes()));
  }

  private Struct getEntryAtIndex(ExecutionHandlerContext context, ByteArrayWrapper key, int index) throws Exception {

    Query query = getQuery(key, ListQuery.LINDEX, context);

    Object[] params = {Integer.valueOf(index + 1)};

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    if (results == null || results.size() == 0 || results.size() <= index)
      return null;
    else
      return (Struct) results.asList().get(index);
  }
}
