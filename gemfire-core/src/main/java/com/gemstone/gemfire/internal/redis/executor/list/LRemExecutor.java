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
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.executor.ListQuery;

public class LRemExecutor extends ListExecutor {

  private final String ERROR_NOT_NUMERIC = "The count provided is not numeric";

  private final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LREM));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] countArray = commandElems.get(2);
    byte[] value = commandElems.get(3);

    int count;


    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region<Integer, ByteArrayWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
      return;
    }

    try {
      count = Coder.bytesToInt(countArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }
    
    List<Struct> removeList;
    try {
      removeList = getRemoveList(context, key, new ByteArrayWrapper(value), count);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    int numRemoved = 0;
    
    if (removeList ==  null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
      return;
    }

    for (Struct entry: removeList) {
      Integer removeKey = (Integer) entry.getFieldValues()[0];
      Object oldVal = keyRegion.remove(removeKey);
      if (oldVal != null)
        numRemoved++;
    }
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
  }

  private List<Struct> getRemoveList(ExecutionHandlerContext context, ByteArrayWrapper key, ByteArrayWrapper value, int count) throws Exception {
    Object[] params;
    Query query;
    if (count > 0) {
      query = getQuery(key, ListQuery.LREMG, context);
      params = new Object[]{value, Integer.valueOf(count)};
    } else if (count < 0) {
      query = getQuery(key, ListQuery.LREML, context);
      params = new Object[]{value, Integer.valueOf(-count)};
    } else {
      query = getQuery(key, ListQuery.LREME, context);
      params = new Object[]{value};
    }

    
    SelectResults<Struct> results = (SelectResults<Struct>) query.execute(params);

    if (results == null || results.isEmpty()) {
      return null;
    }

    return results.asList();
  }
}
