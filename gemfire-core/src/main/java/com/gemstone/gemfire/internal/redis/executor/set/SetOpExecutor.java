package com.gemstone.gemfire.internal.redis.executor.set;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RegionProvider;

public abstract class SetOpExecutor extends SetExecutor implements Extendable {

  @SuppressWarnings("unchecked")
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    int setsStartIndex = isStorage() ? 2 : 1;
    if (commandElems.size() < setsStartIndex + 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }
    RegionProvider rC = context.getRegionProvider();
    ByteArrayWrapper destination = null;
    if (isStorage())
      destination = command.getKey();

    ByteArrayWrapper firstSetKey = new ByteArrayWrapper(commandElems.get(setsStartIndex++));
    if (!isStorage())
      checkDataType(firstSetKey, RedisDataType.REDIS_SET, context);
    Region<ByteArrayWrapper, Boolean> region = (Region<ByteArrayWrapper, Boolean>) rC.getRegion(firstSetKey);
    Set<ByteArrayWrapper> firstSet = null;
    if (region != null) {
      firstSet = new HashSet<ByteArrayWrapper>(region.keySet());
    }
    ArrayList<Set<ByteArrayWrapper>> setList = new ArrayList<Set<ByteArrayWrapper>>();
    for (int i = setsStartIndex; i < commandElems.size(); i++) {
      ByteArrayWrapper key = new ByteArrayWrapper(commandElems.get(i));
      checkDataType(key, RedisDataType.REDIS_SET, context);
      region = (Region<ByteArrayWrapper, Boolean>) rC.getRegion(key);
      if (region != null)
        setList.add(region.keySet());
      else if (this instanceof SInterExecutor)
        setList.add(null);
    }
    if (setList.isEmpty()) {
      if (isStorage()) {
          command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
          context.getRegionProvider().removeKey(destination);
      } else {
        if (firstSet == null)
          command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
        else
          command.setResponse(Coder.getBulkStringArrayResponse(context.getByteBufAllocator(), firstSet));
      }
      return;
    }

    Set<ByteArrayWrapper> resultSet = setOp(firstSet, setList);
    if (isStorage()) {
      Region<ByteArrayWrapper, Boolean> newRegion = null; // (Region<ByteArrayWrapper, Boolean>) rC.getRegion(destination);
      rC.removeKey(destination);
      if (resultSet != null) {
        Map<ByteArrayWrapper, Boolean> map = new HashMap<ByteArrayWrapper, Boolean>();
        for (ByteArrayWrapper entry : resultSet)
          map.put(entry, Boolean.TRUE);
        if (!map.isEmpty()) {
          newRegion = (Region<ByteArrayWrapper, Boolean>) rC.getOrCreateRegion(destination, RedisDataType.REDIS_SET, context);
          newRegion.putAll(map);
        }
        command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), resultSet.size()));
      } else {
        command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      }
    } else {
      if (resultSet == null || resultSet.isEmpty())
        command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      else
        command.setResponse(Coder.getBulkStringArrayResponse(context.getByteBufAllocator(), resultSet));
    }
  }

  protected abstract boolean isStorage();

  protected abstract Set<ByteArrayWrapper> setOp(Set<ByteArrayWrapper> firstSet, List<Set<ByteArrayWrapper>> setList);
}
