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

import static org.apache.geode.redis.internal.netty.Coder.bytesToDouble;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEFT_PAREN;

import java.util.Arrays;

public class SortedSetRangeOptions {
  private final double minDouble;
  private final boolean minExclusive;
  private final double maxDouble;
  private final boolean maxExclusive;

  private boolean hasLimit = false;
  private int offset = 0;
  private int count = 0;

  public SortedSetRangeOptions(byte[] minBytes, byte[] maxBytes) {
    if (minBytes[0] == bLEFT_PAREN) {
      // A value of "(" is equivalent to "(0"
      if (minBytes.length == 1) {
        minDouble = 0;
      } else {
        minDouble =
            bytesToDouble(Arrays.copyOfRange(minBytes, 1, minBytes.length));
      }
      minExclusive = true;
    } else {
      minExclusive = false;
      minDouble = bytesToDouble(minBytes);
    }

    if (maxBytes[0] == bLEFT_PAREN) {
      // A value of "(" is equivalent to "(0"
      if (maxBytes.length == 1) {
        maxDouble = 0;
      } else {
        maxDouble =
            bytesToDouble(Arrays.copyOfRange(maxBytes, 1, maxBytes.length));
      }
      maxExclusive = true;
    } else {
      maxExclusive = false;
      maxDouble = bytesToDouble(maxBytes);
    }
  }

  public void setLimitValues(int offset, int count) {
    hasLimit = true;
    this.offset = offset;
    this.count = count;
  }

  public double getMinDouble() {
    return minDouble;
  }

  public boolean isMinExclusive() {
    return minExclusive;
  }

  public double getMaxDouble() {
    return maxDouble;
  }

  public boolean isMaxExclusive() {
    return maxExclusive;
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
}
