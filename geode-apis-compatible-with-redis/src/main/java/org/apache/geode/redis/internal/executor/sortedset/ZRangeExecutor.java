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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.netty.Coder.bytesToInt;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;

import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZRangeExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    boolean withScores = false;
    int min, max;
    RedisSortedSetCommands redisSortedSetCommands = context.getSortedSetCommands();
    List<byte[]> commandElements = command.getProcessedCommand();

    try {
      min = bytesToInt(commandElements.get(2));
      max = bytesToInt(commandElements.get(3));
    } catch (NumberFormatException nfe) {
      return RedisResponse.error(ERROR_NOT_INTEGER);
    }
    if (commandElements.size() == 5) {
      if (equalsIgnoreCaseBytes(commandElements.get(4), "WITHSCORES".getBytes())) {
        withScores = true;
      } else {
        return RedisResponse.error(ERROR_SYNTAX);
      }
    }

    List<byte[]> retVal = redisSortedSetCommands.zrange(command.getKey(), min, max, withScores);

    return RedisResponse.array(retVal);
  }
}
