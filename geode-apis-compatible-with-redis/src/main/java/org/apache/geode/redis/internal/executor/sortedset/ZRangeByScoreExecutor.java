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
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bRADISH_LIMIT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bRADISH_WITHSCORES;

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
      rangeOptions = new SortedSetRangeOptions(minBytes, maxBytes);
    } catch (NumberFormatException ex) {
      return RedisResponse.error(ERROR_MIN_MAX_NOT_A_FLOAT);
    }

    // Native redis allows multiple "withscores" and "limit ? ?" clauses; the last "limit"
    // clause overrides any previous ones
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
        } catch (NumberFormatException nfex) {
          return RedisResponse.error(ERROR_NOT_INTEGER);
        } catch (IllegalArgumentException iex) {
          return RedisResponse.error(ERROR_SYNTAX);
        }
      }
    }

    // If the range is empty (min > max or min == max and both are exclusive), or
    // limit specified but count is zero, return early
    if ((rangeOptions.hasLimit() && (rangeOptions.getCount() == 0 || rangeOptions.getOffset() < 0))
        ||
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
      int commandIndex) {
    int offset;
    int count;
    if (equalsIgnoreCaseBytes(commandElements.get(commandIndex), bRADISH_LIMIT)
        && commandElements.size() > commandIndex + 2) {
      offset = narrowLongToInt(bytesToLong(commandElements.get(commandIndex + 1)));
      count = narrowLongToInt(bytesToLong(commandElements.get(commandIndex + 2)));
      if (count < 0) {
        count = Integer.MAX_VALUE;
      }
    } else {
      throw new IllegalArgumentException();
    }
    rangeOptions.setLimitValues(offset, count);
  }
}
