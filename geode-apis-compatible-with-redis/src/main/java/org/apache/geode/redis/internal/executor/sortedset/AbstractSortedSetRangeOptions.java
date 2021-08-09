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

import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLIMIT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNEGATIVE_ZERO;

import java.util.Arrays;
import java.util.List;

public abstract class AbstractSortedSetRangeOptions<T> {
  boolean isStartExclusive;
  T startRange;
  boolean isEndExclusive;
  T endRange;
  boolean hasLimit;
  int offset;
  int count;

  AbstractSortedSetRangeOptions(byte[] minimumBytes, byte[] maximumBytes) {
    parseStartRange(minimumBytes);
    parseEndRange(maximumBytes);
  }

  void parseLimitArguments(List<byte[]> commandElements, int commandIndex) {
    if (!equalsIgnoreCaseBytes(commandElements.get(commandIndex), bLIMIT)) {
      throw new IllegalArgumentException();
    }

    // Throw if we don't have enough arguments left to correctly specify LIMIT
    if (commandElements.size() <= commandIndex + 2) {
      throw new IllegalArgumentException();
    }

    byte[] offsetBytes = commandElements.get(commandIndex + 1);
    if (Arrays.equals(offsetBytes, bNEGATIVE_ZERO)) {
      throw new NumberFormatException();
    }

    int parsedOffset = narrowLongToInt(bytesToLong(offsetBytes));
    int parsedCount = narrowLongToInt(bytesToLong(commandElements.get(commandIndex + 2)));

    hasLimit = true;
    this.offset = parsedOffset;
    if (parsedCount < 0) {
      this.count = Integer.MAX_VALUE;
    } else {
      this.count = parsedCount;
    }
  }

  // If limit specified but count is zero, or min > max, or min == max and either are exclusive, the
  // range cannot contain any elements
  boolean isEmptyRange(boolean isRev) {
    int startVsEnd = compareStartToEnd();
    if (isRev) {
      return (hasLimit && (count == 0 || offset < 0)) || startVsEnd == -1
          || (startVsEnd == 0 && (isStartExclusive || isEndExclusive));
    } else {
      return (hasLimit && (count == 0 || offset < 0)) || startVsEnd == 1
          || (startVsEnd == 0 && (isStartExclusive || isEndExclusive));
    }
  }

  public boolean isStartExclusive() {
    return isStartExclusive;
  }

  public T getStartRange() {
    return startRange;
  }

  public boolean isEndExclusive() {
    return isEndExclusive;
  }

  public T getEndRange() {
    return endRange;
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

  abstract void parseStartRange(byte[] minimumBytes);

  abstract void parseEndRange(byte[] maximumBytes);

  abstract int compareStartToEnd();
}
