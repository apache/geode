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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.netty.Coder.bytesToDouble;
import static org.apache.geode.redis.internal.netty.Coder.bytesToInt;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEFT_PAREN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bRADISH_LIMIT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bRADISH_WITHSCORES;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZRangeByScoreExecutor extends AbstractExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    RedisSortedSetCommands redisSortedSetCommands = context.getSortedSetCommands();

    List<byte[]> commandElements = command.getProcessedCommand();

    SortedSetRangeOptions rangeOptions;
    boolean withScores = false;

    try {
      byte[] minBytes = commandElements.get(2);
      byte[] maxBytes = commandElements.get(3);
      rangeOptions = parseRangeArguments(minBytes, maxBytes);
    } catch (NumberFormatException ex) {
      return RedisResponse.error(ERROR_MIN_MAX_NOT_A_FLOAT);
    }

    if (commandElements.size() >= 5) {
      int currentCommandElement = 4;
      while (currentCommandElement < commandElements.size()) {
        try {
          if (equalsIgnoreCaseBytes(commandElements.get(currentCommandElement),
              bRADISH_WITHSCORES)) {
            withScores = true;
            currentCommandElement++;
          } else {
            parseLimitArguments(rangeOptions, commandElements, currentCommandElement);
            currentCommandElement += 3;
          }
        } catch (NumberFormatException ex) {
          return RedisResponse.error(ERROR_NOT_INTEGER);
        } catch (Exception e) {
          return RedisResponse.error(ERROR_SYNTAX);
        }
      }
    }

    // If the range is empty (min > max or min == max and both are exclusive), or
    // limit specified but count is zero, return early
    if ((rangeOptions.hasLimit() && rangeOptions.getCount() == 0) ||
        rangeOptions.getMinDouble() > rangeOptions.getMaxDouble() ||
        (rangeOptions.getMinDouble() == rangeOptions.getMaxDouble())
            && rangeOptions.isMinExclusive() && rangeOptions.isMaxExclusive()) {
      return RedisResponse.emptyArray();
    }

    List<byte[]> result =
        redisSortedSetCommands.zrangebyscore(command.getKey(), rangeOptions, withScores);

    return RedisResponse.array(result);
  }

  void parseLimitArguments(SortedSetRangeOptions rangeOptions, List<byte[]> commandElements,
      int commandIndex)
      throws Exception {
    int offset;
    int count;
    if (equalsIgnoreCaseBytes(commandElements.get(commandIndex), bRADISH_LIMIT)) {
      offset = bytesToInt(commandElements.get(commandIndex + 1));
      count = bytesToInt(commandElements.get(commandIndex + 2));
      if (count < 0) {
        count = Integer.MAX_VALUE;
      }
    } else {
      throw new Exception();
    }
    rangeOptions.setLimitValues(offset, count);
  }

  SortedSetRangeOptions parseRangeArguments(byte[] minBytes, byte[] maxBytes) {
    boolean minExclusive = false;
    double minDouble;
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
      minDouble = bytesToDouble(minBytes);
    }

    boolean maxExclusive = false;
    double maxDouble;
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
      maxDouble = bytesToDouble(maxBytes);
    }
    return new SortedSetRangeOptions(minDouble, minExclusive, maxDouble, maxExclusive);
  }
}
