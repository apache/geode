package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class ZAddExecutor extends SortedSetExecutor {

  private final String ERROR_NOT_NUMERICAL = "The inteded score is not a float";


  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4 || commandElems.size() % 2 == 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZADD));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    int numberOfAdds = 0;

    if (commandElems.size() > 4) {
      Map<ByteArrayWrapper, DoubleWrapper> map = new HashMap<ByteArrayWrapper, DoubleWrapper>();
      for (int i = 2; i < commandElems.size(); i++) {
        byte[] scoreArray = commandElems.get(i++);
        byte[] memberArray = commandElems.get(i);

        Double score;
        try {
          score = Coder.bytesToDouble(scoreArray);
        } catch (NumberFormatException e) {
          command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERICAL));
          return;
        }

        map.put(new ByteArrayWrapper(memberArray), new DoubleWrapper(score));
        numberOfAdds++;
      }
      Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);
      keyRegion.putAll(map);
    } else {
      byte[] scoreArray = commandElems.get(2);
      byte[] memberArray = commandElems.get(3);
      Double score;
      try {
        score = Coder.bytesToDouble(scoreArray);
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERICAL));
        return;
      }
      Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);
      Object oldVal = keyRegion.put(new ByteArrayWrapper(memberArray), new DoubleWrapper(score));

      if (oldVal == null)
        numberOfAdds = 1;
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numberOfAdds));
  }

}
