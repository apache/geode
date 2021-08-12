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

import static org.apache.geode.redis.internal.data.RedisSortedSet.checkDummyMemberNames;
import static org.apache.geode.redis.internal.data.RedisSortedSet.javaImplementationOfAnsiCMemCmp;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bGREATEST_MEMBER_NAME;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEAST_MEMBER_NAME;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEFT_PAREN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEFT_SQUARE_BRACKET;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMINUS;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPLUS;

import java.util.Arrays;

public class SortedSetLexRangeOptions extends AbstractSortedSetRangeOptions<byte[]> {

  SortedSetLexRangeOptions(byte[] minimumBytes, byte[] maximumBytes) {
    super(minimumBytes, maximumBytes);
  }

  @Override
  void parseStartRange(byte[] startBytes) {
    if (startBytes.length == 1) {
      if (startBytes[0] == bPLUS) {
        isStartExclusive = false;
        startRange = bGREATEST_MEMBER_NAME;
      } else if (startBytes[0] == bMINUS) {
        isStartExclusive = false;
        startRange = bLEAST_MEMBER_NAME;
      } else if (startBytes[0] == bLEFT_PAREN) {
        isStartExclusive = true;
        startRange = new byte[0];
      } else if (startBytes[0] == bLEFT_SQUARE_BRACKET) {
        isStartExclusive = false;
        startRange = new byte[0];
      } else {
        throw new IllegalArgumentException();
      }
    } else if (startBytes[0] == bLEFT_PAREN) {
      isStartExclusive = true;
      startRange = Arrays.copyOfRange(startBytes, 1, startBytes.length);
    } else if (startBytes[0] == bLEFT_SQUARE_BRACKET) {
      isStartExclusive = false;
      startRange = Arrays.copyOfRange(startBytes, 1, startBytes.length);
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  void parseEndRange(byte[] endBytes) {
    if (endBytes.length == 1) {
      if (endBytes[0] == bPLUS) {
        isEndExclusive = false;
        endRange = bGREATEST_MEMBER_NAME;
      } else if (endBytes[0] == bMINUS) {
        isEndExclusive = false;
        endRange = bLEAST_MEMBER_NAME;
      } else if (endBytes[0] == bLEFT_PAREN) {
        isEndExclusive = true;
        endRange = new byte[0];
      } else if (endBytes[0] == bLEFT_SQUARE_BRACKET) {
        isEndExclusive = false;
        endRange = new byte[0];
      } else {
        throw new IllegalArgumentException();
      }
    } else if (endBytes[0] == bLEFT_PAREN) {
      isEndExclusive = true;
      endRange = Arrays.copyOfRange(endBytes, 1, endBytes.length);
    } else if (endBytes[0] == bLEFT_SQUARE_BRACKET) {
      isEndExclusive = false;
      endRange = Arrays.copyOfRange(endBytes, 1, endBytes.length);
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  int compareStartToEnd() {
    int dummyNameComparison = checkDummyMemberNames(startRange, endRange);
    if (dummyNameComparison == 0) {
      return javaImplementationOfAnsiCMemCmp(startRange, endRange);
    } else {
      return dummyNameComparison;
    }
  }
}
