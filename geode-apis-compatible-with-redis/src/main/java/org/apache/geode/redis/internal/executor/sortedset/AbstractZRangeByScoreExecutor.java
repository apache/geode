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
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.Coder.isNaN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bWITHSCORES;

import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public abstract class AbstractZRangeByScoreExecutor extends AbstractExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElements = command.getProcessedCommand();
    SortedSetScoreRangeOptions rangeOptions;

    try {
      byte[] startBytes = commandElements.get(2);
      byte[] endBytes = commandElements.get(3);
      if (isNaN(startBytes) || isNaN(endBytes)) {
        return RedisResponse.error(ERROR_MIN_MAX_NOT_A_FLOAT);
      }
      rangeOptions = new SortedSetScoreRangeOptions(startBytes, endBytes);
    } catch (NumberFormatException ex) {
      return RedisResponse.error(ERROR_MIN_MAX_NOT_A_FLOAT);
    }

    // Native redis allows multiple "withscores" and "limit ? ?" clauses; the last "limit"
    // clause overrides any previous ones
    boolean withScores = false;
    if (commandElements.size() >= 5) {
      int currentCommandElement = 4;

      while (currentCommandElement < commandElements.size()) {
        try {
          if (equalsIgnoreCaseBytes(commandElements.get(currentCommandElement),
              bWITHSCORES)) {
            withScores = true;
            currentCommandElement++;
          } else {
            rangeOptions.parseLimitArguments(commandElements, currentCommandElement);
            currentCommandElement += 3;
          }
        } catch (NumberFormatException nfex) {
          return RedisResponse.error(ERROR_NOT_INTEGER);
        } catch (IllegalArgumentException iex) {
          return RedisResponse.error(ERROR_SYNTAX);
        }
      }
    }

    // If the range is empty (min == max and both are exclusive,
    // or limit specified but count is zero), return early
    if (rangeOptions.isEmptyRange(isRev())) {
      return RedisResponse.emptyArray();
    }

    // Check if limits are impossible; For ZREVRANGEBYSCORE, start and end are reversed
    if (isRev() ? (rangeOptions.getStartRange() < rangeOptions.getEndRange())
        : (rangeOptions.getStartRange() > rangeOptions.getEndRange())) {
      return RedisResponse.emptyArray();
    }

    RedisSortedSetCommands redisSortedSetCommands = context.getSortedSetCommands();
    List<byte[]> result;
    if (isRev()) {
      result = redisSortedSetCommands.zrevrangebyscore(command.getKey(), rangeOptions, withScores);
    } else {
      result = redisSortedSetCommands.zrangebyscore(command.getKey(), rangeOptions, withScores);
    }

    return RedisResponse.array(result);
  }

  public abstract boolean isRev();
}
