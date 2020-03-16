/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.redis.internal.executor.list;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public abstract class ListExecutor extends AbstractExecutor {

  protected static final int LIST_EMPTY_SIZE = 2;

  protected enum ListDirection {
    LEFT, RIGHT
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Region<Object, Object> getOrCreateRegion(ExecutionHandlerContext context,
      ByteArrayWrapper key,
      RedisDataType type) {
    return (Region<Object, Object>) context.getRegionProvider().getOrCreateRegion(key,
        type, context);
  }

  @SuppressWarnings("unchecked")
  protected Region<Object, Object> getRegion(ExecutionHandlerContext context,
      ByteArrayWrapper key) {
    return (Region<Object, Object>) context.getRegionProvider().getRegion(key);
  }

  /**
   * Helper method to be used by the push commands to push elements onto a list. Because our current
   * setup requires non trivial code to push elements in to a Region, I wanted all the push code to
   * reside in one place.
   *
   * @param key Name of the list
   * @param commandElems Pieces of the command, this is where the elements that need to be pushed
   *        live
   * @param startIndex The index to start with in the commandElems list, inclusive
   * @param endIndex The index to end with in the commandElems list, exclusive
   * @param keyRegion Region of list
   * @param pushType ListDirection.LEFT || ListDirection.RIGHT
   * @param context Context of this push
   */
  protected void pushElements(ByteArrayWrapper key, List<byte[]> commandElems, int startIndex,
      int endIndex, Region<Object, Object> keyRegion, ListDirection pushType,
      ExecutionHandlerContext context) {

    String indexKey = pushType == ListDirection.LEFT ? "head" : "tail";
    String oppositeKey = pushType == ListDirection.RIGHT ? "head" : "tail";
    Integer index = (Integer) keyRegion.get(indexKey);
    Integer opp = (Integer) keyRegion.get(oppositeKey);
    if (index != null && (!index.equals(opp))) {
      index += pushType == ListDirection.LEFT ? -1 : 1; // Subtract index if left push, add if right
    }
    // push

    /*
     * Multi push command
     *
     * For every element that needs to be added
     */

    for (int i = startIndex; i < endIndex; i++) {
      byte[] value = commandElems.get(i);
      ByteArrayWrapper wrapper = new ByteArrayWrapper(value);

      /*
       *
       * First, use the start index to attempt to insert the value into the Region
       *
       */

      Object oldValue;
      do {
        oldValue = keyRegion.putIfAbsent(index, wrapper);
        if (oldValue != null) {
          index += pushType == ListDirection.LEFT ? -1 : 1; // Subtract index if left push, add if
          // right push
        }
      } while (oldValue != null);

      /*
       *
       * Next, update the index in the meta data region. Keep trying to replace the existing index
       * unless the index is further out than previously inserted, that's ok. Example below:
       *
       * ********************** LPUSH/LPUSH *************************** Push occurring at the same
       * time, further index update first | This push | | | | V V [-4] [-3] [-2] [-1] [0] [1] [2]
       *
       * In this case, -4 would already exist in the meta data region, therefore we do not try to
       * put -3 in the meta data region because a further index is already there.
       * ***************************************************************
       *
       * Another example
       *
       * ********************** LPUSH/LPOP ***************************** This push | Simultaneous
       * LPOP, meta data head index already updated to -2 | | | | V V [-4] [X] [-2] [-1] [0] [1] [2]
       *
       * In this case, -2 would already exist in the meta data region, but we need to make sure the
       * element at -4 is visible to all other threads so we will attempt to change the index to -4
       * as long as it is greater than -4
       * ***************************************************************
       *
       */

      boolean indexSet;
      do {
        Integer existingIndex = (Integer) keyRegion.get(indexKey);
        if (index != null && ((pushType == ListDirection.RIGHT && existingIndex < index)
            || (pushType == ListDirection.LEFT && existingIndex > index))) {
          indexSet = keyRegion.replace(indexKey, existingIndex, index);
        } else {
          break;
        }
      } while (!indexSet);

    }
  }

}
