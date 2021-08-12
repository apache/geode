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

public class SortedSetScoreRangeOptions extends AbstractSortedSetRangeOptions<Double> {

  public SortedSetScoreRangeOptions(byte[] startBytes, byte[] endBytes) {
    super(startBytes, endBytes);
  }

  @Override
  void parseStartRange(byte[] startBytes) {
    if (startBytes[0] == bLEFT_PAREN) {
      // A value of "(" is equivalent to "(0"
      if (startBytes.length == 1) {
        startRange = 0.0;
      } else {
        startRange = bytesToDouble(Arrays.copyOfRange(startBytes, 1, startBytes.length));
      }
      isStartExclusive = true;
    } else {
      isStartExclusive = false;
      startRange = bytesToDouble(startBytes);
    }
  }

  @Override
  void parseEndRange(byte[] endBytes) {
    if (endBytes[0] == bLEFT_PAREN) {
      // A value of "(" is equivalent to "(0"
      if (endBytes.length == 1) {
        endRange = 0.0;
      } else {
        endRange = bytesToDouble(Arrays.copyOfRange(endBytes, 1, endBytes.length));
      }
      isEndExclusive = true;
    } else {
      isEndExclusive = false;
      endRange = bytesToDouble(endBytes);
    }
  }

  @Override
  int compareStartToEnd() {
    return Double.compare(startRange, endRange);
  }
}
