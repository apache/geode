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
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.LIMIT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.NEGATIVE_ZERO;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.WITHSCORES;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.data.RedisSortedSet;

public abstract class AbstractSortedSetRangeOptions<T> {
  RangeLimit<T> start;
  RangeLimit<T> end;
  boolean isRev;
  boolean withScores;
  boolean hasLimit;
  int offset;
  // When count it not specified, return the entire range
  int count = Integer.MAX_VALUE;

  AbstractSortedSetRangeOptions(List<byte[]> commandElements, boolean isRev) {
    this.isRev = isRev;
    parseRangeArguments(commandElements);
    parseAdditionalArguments(commandElements);
  }

  void parseAdditionalArguments(List<byte[]> commandElements) {
    if (commandElements.size() <= 4) {
      return;
    }

    // Start parsing at index = 4, since 0 is the command name, 1 is the key, and 2 and 3 are the
    // start or end of the range
    for (int index = 4; index < commandElements.size(); ++index) {
      byte[] option = commandElements.get(index);
      if (equalsIgnoreCaseBytes(option, LIMIT)) {
        handleLimitArguments(commandElements, index);
        index += 2;
      } else if (equalsIgnoreCaseBytes(option, WITHSCORES)) {
        handleWithScoresArgument();
      } else {
        throw new RedisException(ERROR_SYNTAX);
      }
    }
  }

  void handleLimitArguments(List<byte[]> commandElements, int commandIndex) {
    if (!equalsIgnoreCaseBytes(commandElements.get(commandIndex), LIMIT)) {
      throw new RedisException(ERROR_SYNTAX);
    }

    // Throw if we don't have enough arguments left to correctly specify LIMIT
    if (commandElements.size() <= commandIndex + 2) {
      throw new RedisException(ERROR_SYNTAX);
    }

    byte[] offsetBytes = commandElements.get(commandIndex + 1);
    if (Arrays.equals(offsetBytes, NEGATIVE_ZERO)) {
      throw new RedisException(ERROR_NOT_INTEGER);
    }

    int parsedOffset;
    int parsedCount;
    try {
      parsedOffset = narrowLongToInt(bytesToLong(offsetBytes));
      parsedCount = narrowLongToInt(bytesToLong(commandElements.get(commandIndex + 2)));
    } catch (NumberFormatException ex) {
      throw new RedisException(ERROR_NOT_INTEGER);
    }

    hasLimit = true;
    this.offset = parsedOffset;
    if (parsedCount < 0) {
      this.count = Integer.MAX_VALUE;
    } else {
      this.count = parsedCount;
    }
  }

  void handleWithScoresArgument() {
    this.withScores = true;
  }

  // The range will contain no entries if:
  // limit is specified and count is zero
  // limit is specified and offset is negative
  // the start is greater than the end (or vice versa for some reverse ranges)
  // start == end and either are exclusive
  boolean containsNoEntries() {
    int startVsEnd = compareStartToEnd();
    return (hasLimit() && (count == 0 || offset < 0)) || startVsEnd == 1
        || (startVsEnd == 0 && (isStartExclusive() || isEndExclusive()));
  }

  public boolean isStartExclusive() {
    return start.isExclusive;
  }

  public T getStart() {
    return start.value;
  }

  public boolean isEndExclusive() {
    return end.isExclusive;
  }

  public T getEnd() {
    return end.value;
  }

  public boolean isRev() {
    return isRev;
  }

  public boolean isWithScores() {
    return withScores;
  }

  public boolean hasLimit() {
    return hasLimit;
  }

  public int getOffset() {
    return offset;
  }

  public int getCount() {
    return count;
  }

  abstract void parseRangeArguments(List<byte[]> commandElements);

  abstract int compareStartToEnd();

  public abstract int getRangeIndex(RedisSortedSet.ScoreSet scoreSet, boolean isStartIndex);

  public static class RangeLimit<T> {
    final T value;
    final boolean isExclusive;

    public RangeLimit(T value, boolean isExclusive) {
      this.value = value;
      this.isExclusive = isExclusive;
    }
  }
}
