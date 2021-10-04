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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_MIN_MAX_NOT_A_VALID_STRING;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.data.RedisSortedSet.checkDummyMemberNames;
import static org.apache.geode.redis.internal.data.RedisSortedSet.javaImplementationOfAnsiCMemCmp;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bGREATEST_MEMBER_NAME;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEAST_MEMBER_NAME;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.data.RedisSortedSet;

public class SortedSetLexRangeOptions extends AbstractSortedSetRangeOptions<byte[]> {

  @VisibleForTesting
  public SortedSetLexRangeOptions(List<byte[]> commandElements, boolean isRev) {
    super(commandElements, isRev);
  }

  @Override
  void parseRangeArguments(List<byte[]> commandElements) {
    start = parseOneRangeArgument(commandElements.get(2));
    end = parseOneRangeArgument(commandElements.get(3));
  }


  private RangeLimit<byte[]> parseOneRangeArgument(byte[] bytes) {
    boolean isExclusive;
    byte[] value;
    if (bytes.length == 1) {
      if (bytes[0] == '+') {
        isExclusive = false;
        value = bGREATEST_MEMBER_NAME;
      } else if (bytes[0] == '-') {
        isExclusive = false;
        value = bLEAST_MEMBER_NAME;
      } else if (bytes[0] == '(') {
        isExclusive = true;
        value = new byte[0];
      } else if (bytes[0] == '[') {
        isExclusive = false;
        value = new byte[0];
      } else {
        throw new RedisException(ERROR_MIN_MAX_NOT_A_VALID_STRING);
      }
    } else if (bytes[0] == '(') {
      isExclusive = true;
      value = Arrays.copyOfRange(bytes, 1, bytes.length);
    } else if (bytes[0] == '[') {
      isExclusive = false;
      value = Arrays.copyOfRange(bytes, 1, bytes.length);
    } else {
      throw new RedisException(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    }
    return new RangeLimit<>(value, isExclusive);
  }

  @Override
  void handleWithScoresArgument() {
    // BYLEX ranges do not support the WITHSCORES argument
    throw new RedisException(ERROR_SYNTAX);
  }

  @Override
  int compareStartToEnd() {
    if (isRev) {
      return compareMemberNames(end.value, start.value);
    } else {
      return compareMemberNames(start.value, end.value);
    }
  }

  @Override
  public int getRangeIndex(RedisSortedSet.ScoreSet scoreSet, boolean isStartIndex) {
    int index;
    RangeLimit<byte[]> rangeLimit = isStartIndex ? start : end;
    RedisSortedSet.AbstractOrderedSetEntry entry =
        new RedisSortedSet.MemberDummyOrderedSetEntry(rangeLimit.value,
            rangeLimit.isExclusive, isStartIndex ^ isRev);
    index = scoreSet.indexOf(entry);
    if (isRev) {
      // Subtract 1 from the index here because when treating the set as reverse ordered, we
      // overshoot the correct index due to the comparison in MemberDummyOrderedSetEntry assuming
      // non-reverse ordering
      index--;
    }
    return index;
  }

  private int compareMemberNames(byte[] nameOne, byte[] nameTwo) {
    int dummyNameComparison = checkDummyMemberNames(nameOne, nameTwo);
    if (dummyNameComparison == 0) {
      return javaImplementationOfAnsiCMemCmp(nameOne, nameTwo);
    } else {
      return dummyNameComparison;
    }
  }
}
