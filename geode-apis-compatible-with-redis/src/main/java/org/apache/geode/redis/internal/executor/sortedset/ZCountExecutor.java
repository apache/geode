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

import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZCountExecutor extends AbstractExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    RedisSortedSetCommands redisSortedSetCommands = context.getSortedSetCommands();

    List<byte[]> commandElements = command.getProcessedCommand();

    SortedSetRangeOptions rangeOptions;

    try {
      byte[] minBytes = commandElements.get(2);
      byte[] maxBytes = commandElements.get(3);
      rangeOptions = new SortedSetRangeOptions(minBytes, maxBytes);
    } catch (NumberFormatException ex) {
      return RedisResponse.error(ERROR_MIN_MAX_NOT_A_FLOAT);
    }

    // If the range is empty (min > max or min == max and both are exclusive), return early
    if (rangeOptions.getMinDouble() > rangeOptions.getMaxDouble() ||
        (rangeOptions.getMinDouble() == rangeOptions.getMaxDouble())
            && rangeOptions.isMinExclusive() && rangeOptions.isMaxExclusive()) {
      return RedisResponse.integer(0);
    }

    long count = redisSortedSetCommands.zcount(command.getKey(), rangeOptions);

    return RedisResponse.integer(count);
  }
}
