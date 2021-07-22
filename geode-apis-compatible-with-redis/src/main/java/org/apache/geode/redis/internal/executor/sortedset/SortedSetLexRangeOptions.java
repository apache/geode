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
  void parseMinimum(byte[] minBytes) {
    if (minBytes.length == 1) {
      if (minBytes[0] == bPLUS) {
        isMinExclusive = false;
        minimum = bGREATEST_MEMBER_NAME;
      } else if (minBytes[0] == bMINUS) {
        isMinExclusive = false;
        minimum = bLEAST_MEMBER_NAME;
      } else if (minBytes[0] == bLEFT_PAREN) {
        isMinExclusive = true;
        minimum = new byte[0];
      } else if (minBytes[0] == bLEFT_SQUARE_BRACKET) {
        isMinExclusive = false;
        minimum = new byte[0];
      } else {
        throw new IllegalArgumentException();
      }
    } else if (minBytes[0] == bLEFT_PAREN) {
      isMinExclusive = true;
      minimum = Arrays.copyOfRange(minBytes, 1, minBytes.length);
    } else if (minBytes[0] == bLEFT_SQUARE_BRACKET) {
      isMinExclusive = false;
      minimum = Arrays.copyOfRange(minBytes, 1, minBytes.length);
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  void parseMaximum(byte[] maxBytes) {
    if (maxBytes.length == 1) {
      if (maxBytes[0] == bPLUS) {
        isMaxExclusive = false;
        maximum = bGREATEST_MEMBER_NAME;
      } else if (maxBytes[0] == bMINUS) {
        isMaxExclusive = false;
        maximum = bLEAST_MEMBER_NAME;
      } else if (maxBytes[0] == bLEFT_PAREN) {
        isMaxExclusive = true;
        maximum = new byte[0];
      } else if (maxBytes[0] == bLEFT_SQUARE_BRACKET) {
        isMaxExclusive = false;
        maximum = new byte[0];
      } else {
        throw new IllegalArgumentException();
      }
    } else if (maxBytes[0] == bLEFT_PAREN) {
      isMaxExclusive = true;
      maximum = Arrays.copyOfRange(maxBytes, 1, maxBytes.length);
    } else if (maxBytes[0] == bLEFT_SQUARE_BRACKET) {
      isMaxExclusive = false;
      maximum = Arrays.copyOfRange(maxBytes, 1, maxBytes.length);
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  int compareMinToMax() {
    int dummyNameComparison = checkDummyMemberNames(minimum, maximum);
    if (dummyNameComparison == 0) {
      return javaImplementationOfAnsiCMemCmp(minimum, maximum);
    } else {
      return dummyNameComparison;
    }
  }
}
