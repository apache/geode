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
package org.apache.geode.redis.internal.executor.sortedset;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;

import java.util.List;

import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.data.RedisSortedSet;

public class SortedSetRankRangeOptions extends AbstractSortedSetRangeOptions<Integer> {
  SortedSetRankRangeOptions(List<byte[]> commandElements, boolean isRev) {
    super(commandElements, isRev);
  }

  @Override
  void parseRangeArguments(List<byte[]> commandElements) {
    try {
      start = new RangeLimit<>(narrowLongToInt(bytesToLong(commandElements.get(2))), false);
      end = new RangeLimit<>(narrowLongToInt(bytesToLong(commandElements.get(3))), false);
    } catch (NumberFormatException ex) {
      throw new RedisException(ERROR_NOT_INTEGER);
    }
  }

  @Override
  void handleLimitArguments(List<byte[]> commandElements, int index) {
    // Range by rank commands do not (currently) support the LIMIT argument
    throw new RedisException(ERROR_SYNTAX);
  }

  @Override
  int compareStartToEnd() {
    // If only one of the start or end are indexing from the end of the sorted set by using negative
    // indexes, it's not possible to determine at this point if the range will be empty, so return
    // as if we have a valid range
    if (start.value < 0 ^ end.value < 0) {
      return -1;
    }
    return Integer.compare(start.value, end.value);
  }

  @Override
  public int getRangeIndex(RedisSortedSet.ScoreSet scoreSet, boolean isStartIndex) {
    int index;
    int rangeValue = isStartIndex ? start.value : end.value;
    index = getBoundedRankIndex(rangeValue, scoreSet.size(), isStartIndex);
    if (isRev) {
      // scoreSet.size() - 1 is the maximum index of elements in the sorted set, so in a reverse
      // ordered set we count backwards from there
      index = scoreSet.size() - 1 - index;
    }
    return index;
  }

  @Override
  public boolean hasLimit() {
    return false;
  }

  private int getBoundedRankIndex(int index, int size, boolean isStartIndex) {
    if (index >= 0) {
      if (index > size) {
        return size;
      } else {
        // Add 1 because rank ranges use inclusive maximum, so without this, we would return one
        // element too few
        return isStartIndex ? index : index + 1;
      }
    } else {
      int offsetIndex = size + index;
      if (offsetIndex < 0) {
        return 0;
      } else {
        // Add 1 because rank ranges use inclusive maximum, so without this, we would return one
        // element too few
        return isStartIndex ? offsetIndex : offsetIndex + 1;
      }
    }
  }
}
