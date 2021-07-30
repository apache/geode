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
  boolean isMinExclusive;
  T minimum;
  boolean isMaxExclusive;
  T maximum;
  boolean hasLimit;
  int offset;
  int count;

  AbstractSortedSetRangeOptions(byte[] minimumBytes, byte[] maximumBytes) {
    parseMinimum(minimumBytes);
    parseMaximum(maximumBytes);
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
  boolean isEmptyRange() {
    int minVsMax = compareMinToMax();
    return (hasLimit && (count == 0 || offset < 0)) || minVsMax == 1
        || (minVsMax == 0 && (isMinExclusive || isMaxExclusive));
  }

  public boolean isMinExclusive() {
    return isMinExclusive;
  }

  public T getMinimum() {
    return minimum;
  }

  public boolean isMaxExclusive() {
    return isMaxExclusive;
  }

  public T getMaximum() {
    return maximum;
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

  abstract void parseMinimum(byte[] minimumBytes);

  abstract void parseMaximum(byte[] maximumBytes);

  abstract int compareMinToMax();
}
