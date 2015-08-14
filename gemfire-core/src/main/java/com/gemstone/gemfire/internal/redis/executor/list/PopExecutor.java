package com.gemstone.gemfire.internal.redis.executor.list;

import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisDataType;

public abstract class PopExecutor extends ListExecutor implements Extendable {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region keyRegion = getRegion(context, key);

    if (keyRegion == null || keyRegion.size() == LIST_EMPTY_SIZE) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    String indexKey = popType() == ListDirection.LEFT ? "head" : "tail";
    String oppositeKey = popType() == ListDirection.RIGHT ? "head" : "tail";
    Integer index = 0;
    int originalIndex = index;
    int incr = popType() == ListDirection.LEFT ? 1 : -1;
    ByteArrayWrapper valueWrapper = null;
    
    /**
     * 
     * First attempt to hop over an index by moving the index 
     * down one in the meta data region. The desired index to 
     * remove is held within the field index
     * 
     */
    
    boolean indexChanged = false;
    do {
      index = (Integer) keyRegion.get(indexKey);
      Integer opp = (Integer) keyRegion.get(oppositeKey);
      if (index.equals(opp))
        break;
      indexChanged = keyRegion.replace(indexKey, index, index + incr);
    } while(!indexChanged);
    
    /**
     * 
     * Now attempt to remove the value of the index. We must do a
     * get to ensure a returned value and then call remove with the
     * value to ensure no one else has removed it first. Otherwise, 
     * try other indexes 
     * 
     */
    
    boolean removed = false;
    int i = 0;
    do {
      valueWrapper = (ByteArrayWrapper) keyRegion.get(index);
      if (valueWrapper != null)
        removed = keyRegion.remove(index, valueWrapper);
      
      /**
       * 
       * If remove has passed, our job is done and we can break and
       * stop looking for a value
       * 
       */
      
      if (removed)
        break;

      /**
       * 
       * If the index has not been removed, we need to look at other indexes.
       * Two cases exist:
       * 
       * ************************** FIRST MISS ***********************************
       *   Push occurring at the same time, further index update first
       *   |    This is location of miss
       *   |      |
       *   |      |
       *   V      V
       * [-4]    [X]    [-2]    [-1]    [0]     [1]     [2]
       *        <-- Direction of index update
       * If this is the first miss, the index is re obtained from the meta region
       * and that index is trying. However, if the index in the meta data region
       * is not further out, that index is not used and moves on to the second case
       * **************************************************************************
       * 
       * ************************* SUBSEQUENT MISSES ******************************
       *   Push occurring at the same time, further index update first
       *   |    This is location of miss
       *   |      |
       *   |      |
       *   V      V
       * [-4]    [X]    [-2]    [-1]    [0]     [1]     [2]
       * Direction of index update -->
       * If this is not the first miss then we move down to the other end of the list
       * which means the next not empty index will be attempted to be removed
       * **************************************************************************
       * 
       * If it is the case that the list is empty, it will exit this loop
       * 
       */
      
      index += incr;
      Integer metaIndex = (Integer) keyRegion.get(indexKey);
      if (i < 1 && (popType() == ListDirection.LEFT && metaIndex < originalIndex ||
          popType() == ListDirection.RIGHT && metaIndex > originalIndex))
        index = metaIndex;
      i++;
    } while (!removed && keyRegion.size() != LIST_EMPTY_SIZE);
    if (valueWrapper != null)
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), valueWrapper.toBytes()));
    else
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
  }

  protected abstract ListDirection popType();

}
