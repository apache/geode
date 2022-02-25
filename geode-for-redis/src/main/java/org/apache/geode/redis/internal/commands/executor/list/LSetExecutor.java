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

package org.apache.geode.redis.internal.commands.executor.list;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INDEX_OUT_OF_RANGE;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NO_SUCH_KEY;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisList;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class LSetExecutor implements CommandExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    Region<RedisKey, RedisData> region = context.getRegion();
    List<byte[]> commandElements = command.getProcessedCommand();

    RedisKey key = command.getKey();
    if (!context.listLockedExecute(key, false, RedisData::exists)) {
      return RedisResponse.error(ERROR_NO_SUCH_KEY);
    }

    long index = Coder.bytesToLong(commandElements.get(2));
    byte[] value = commandElements.get(3);
    long listSize = (long) context.listLockedExecute(key, false, RedisList::llen);

    final int adjustedIndex = (int) (index >= 0 ? index : listSize + index);
    if (adjustedIndex > listSize - 1 || adjustedIndex < 0) {
      return RedisResponse.error(ERROR_INDEX_OUT_OF_RANGE);
    }

    boolean success = context.listLockedExecute(key, false,
        list -> list.lset(region, key, adjustedIndex, value));

    return success ? RedisResponse.ok() : RedisResponse.error(ERROR_NO_SUCH_KEY);
  }
}
