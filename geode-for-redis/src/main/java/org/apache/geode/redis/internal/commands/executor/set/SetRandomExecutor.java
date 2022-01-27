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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;

import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.services.RegionProvider;

public abstract class SetRandomExecutor implements CommandExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    RedisKey key = command.getKey();
    boolean hasCount = commandElems.size() == 3;

    int count;
    if (hasCount) {
      try {
        count = narrowLongToInt(bytesToLong(commandElems.get(2)));
      } catch (NumberFormatException e) {
        return RedisResponse.error(getError());
      }
    } else {
      count = 1;
    }

    List<byte[]> results =
        context.lockedExecute(key, () -> performCommand(count, context.getRegionProvider(), key));

    if (hasCount) {
      return RedisResponse.array(results, true);
    } else {
      if (results.isEmpty()) {
        return RedisResponse.nil();
      } else {
        return RedisResponse.bulkString(results.iterator().next());
      }
    }
  }



  protected abstract List<byte[]> performCommand(int count, RegionProvider regionProvider,
      RedisKey key);

  protected abstract String getError();
}
