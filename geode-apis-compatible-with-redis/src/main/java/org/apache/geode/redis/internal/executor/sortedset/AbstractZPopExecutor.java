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

import java.util.List;

import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public abstract class AbstractZPopExecutor extends AbstractExecutor {

  protected abstract List<byte[]> zpop(RedisSortedSetCommands sortedSetCommands, RedisKey key,
      int count);

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElements = command.getProcessedCommand();

    int count = 1;
    if (commandElements.size() > 2) {
      try {
        count = Coder.narrowLongToInt(Coder.bytesToLong(commandElements.get(2)));
      } catch (NumberFormatException nex) {
        return RedisResponse.error(RedisConstants.ERROR_NOT_INTEGER);
      }
    }

    if (count < 1) {
      return RedisResponse.emptyArray();
    }

    return RedisResponse.array(zpop(context.getSortedSetCommands(), command.getKey(), count), true);
  }
}
