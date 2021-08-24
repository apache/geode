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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_MIN_MAX_NOT_A_FLOAT;
import static org.apache.geode.redis.internal.netty.Coder.bytesToDouble;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEFT_PAREN;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.data.RedisSortedSet;

public class SortedSetScoreRangeOptions extends AbstractSortedSetRangeOptions<Double> {

  public SortedSetScoreRangeOptions(List<byte[]> commandElements, boolean isRev) {
    super(commandElements, isRev);
  }

  @Override
  void parseRangeArguments(List<byte[]> commandElements) {
    try {
      start = parseOneRangeArgument(commandElements.get(2));
      end = parseOneRangeArgument(commandElements.get(3));
    } catch (NumberFormatException ex) {
      throw new RedisException(ERROR_MIN_MAX_NOT_A_FLOAT);
    }
  }

  private RangeLimit<Double> parseOneRangeArgument(byte[] bytes) {
    double score;
    if (bytes[0] == bLEFT_PAREN) {
      // A value of "(" is equivalent to "(0"
      if (bytes.length == 1) {
        score = 0.0;
      } else {
        score = bytesToDouble(Arrays.copyOfRange(bytes, 1, bytes.length));
        if (Double.isNaN(score)) {
          throw new NumberFormatException();
        }
      }
      return new RangeLimit<>(score, true);
    } else {
      score = bytesToDouble(bytes);
      if (Double.isNaN(score)) {
        throw new NumberFormatException();
      }
      return new RangeLimit<>(score, false);
    }
  }

  @Override
  int compareStartToEnd() {
    if (isRev()) {
      return Double.compare(end.value, start.value);
    } else {
      return Double.compare(start.value, end.value);
    }
  }

  @Override
  public int getRangeIndex(RedisSortedSet.ScoreSet scoreSet, boolean isStartIndex) {
    int index;
    RangeLimit<Double> rangeLimit = isStartIndex ? start : end;
    RedisSortedSet.AbstractOrderedSetEntry entry = new RedisSortedSet.ScoreDummyOrderedSetEntry(
        rangeLimit.value, rangeLimit.isExclusive, isStartIndex ^ isRev);
    index = scoreSet.indexOf(entry);
    if (isRev) {
      // Subtract 1 from the index here because when treating the set as reverse ordered, we
      // overshoot the correct index due to the comparison in ScoreDummyOrderedSetEntry assuming
      // non-reverse ordering
      index--;
    }
    return index;
  }
}
