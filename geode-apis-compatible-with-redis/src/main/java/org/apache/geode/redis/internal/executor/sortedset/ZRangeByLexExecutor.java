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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZRangeByLexExecutor extends AbstractExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElements = command.getProcessedCommand();

    SortedSetLexRangeOptions rangeOptions;

    try {
      byte[] minBytes = commandElements.get(2);
      byte[] maxBytes = commandElements.get(3);
      rangeOptions = new SortedSetLexRangeOptions(minBytes, maxBytes);
    } catch (IllegalArgumentException ex) {
      return RedisResponse.error(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    }

    // Native redis allows multiple "limit ? ?" clauses; the last "limit" clause overrides any
    // previous ones
    // Start parsing at index = 4, since 0 is the command name, 1 is the key, 2 is the min and 3 is
    // the max
    if (commandElements.size() >= 5) {
      for (int index = 4; index < commandElements.size(); ++index) {
        try {
          rangeOptions.parseLimitArguments(commandElements, index);
          // If we successfully parse a set of three LIMIT options, increment the index past them
          index += 2;
        } catch (NumberFormatException nfex) {
          return RedisResponse.error(ERROR_NOT_INTEGER);
        } catch (IllegalArgumentException iex) {
          return RedisResponse.error(ERROR_SYNTAX);
        }
      }
    }

    // If the range is empty, return early
    if (rangeOptions.isEmptyRange()) {
      return RedisResponse.emptyArray();
    }
    // If offset is negative
    if (rangeOptions.offset < 0) {
      return RedisResponse.emptyArray();
    }

    List<byte[]> result =
        context.getSortedSetCommands().zrangebylex(command.getKey(), rangeOptions);

    return RedisResponse.array(result);
  }
}
