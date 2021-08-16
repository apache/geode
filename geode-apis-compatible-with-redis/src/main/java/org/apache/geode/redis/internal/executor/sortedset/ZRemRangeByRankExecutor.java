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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;

import java.util.List;

import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZRemRangeByRankExecutor implements Executor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    int min;
    int max;
    List<byte[]> commandElements = command.getProcessedCommand();

    try {
      // We need to be able to accept long arguments to match Redis behaviour, even though
      // internally int is sufficient, as sorted sets have a maximum size of Integer.MAX_VALUE
      long longMin = bytesToLong(commandElements.get(2));
      min = narrowLongToInt(longMin);
      long longMax = bytesToLong(commandElements.get(3));
      max = narrowLongToInt(longMax);
    } catch (NumberFormatException nfe) {
      return RedisResponse.error(ERROR_NOT_INTEGER);
    }

    RedisSortedSetCommands redisSortedSetCommands = context.getSortedSetCommands();

    int retVal = redisSortedSetCommands.zremrangebyrank(command.getKey(), min, max);

    return RedisResponse.integer(retVal);
  }

}
