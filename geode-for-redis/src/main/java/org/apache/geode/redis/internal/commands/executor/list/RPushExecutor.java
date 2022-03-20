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

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class RPushExecutor implements CommandExecutor {

  @Override
  public final RedisResponse executeCommand(final Command command,
      final ExecutionHandlerContext context) {
    List<byte[]> commandElements = command.getProcessedCommand();
    Region<RedisKey, RedisData> region = context.getRegion();
    RedisKey key = command.getKey();

    List<byte[]> elementsToAdd = commandElements.subList(2, commandElements.size());

    final long newLength = context.listLockedExecute(key, false,
        list -> list.rpush(context, elementsToAdd, key, shouldPushOnlyIfKeyExists()));

    return RedisResponse.integer(newLength);
  }

  protected boolean shouldPushOnlyIfKeyExists() {
    return false;
  }
}
