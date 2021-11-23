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

package org.apache.geode.redis.internal.commands.executor.key;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NO_SUCH_KEY;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisKeyExistsException;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public abstract class AbstractRenameExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<RedisKey> commandElems = command.getProcessedCommandKeys();
    RedisKey key = command.getKey();
    RedisKey newKey = commandElems.get(2);

    if (key.equals(newKey)) {
      return getTargetSameAsSourceResponse();
    }

    if (key.getSlot() != newKey.getSlot()) {
      return RedisResponse.crossSlot(ERROR_WRONG_SLOT);
    }

    try {
      if (!executeRenameCommand(key, newKey, context)) {
        return getNoSuchKeyResponse();
      }
    } catch (RedisKeyExistsException ignored) {
      return getKeyExistsResponse();
    }

    return getSuccessResponse();
  }

  protected static boolean rename(ExecutionHandlerContext context, RedisKey oldKey, RedisKey newKey,
      boolean ifTargetNotExists) {
    List<RedisKey> lockOrdering = Arrays.asList(oldKey, newKey);

    return context.lockedExecute(oldKey, lockOrdering,
        () -> context.getRedisData(oldKey)
            .rename(context.getRegion(), oldKey, newKey, ifTargetNotExists));
  }

  protected RedisResponse getNoSuchKeyResponse() {
    return RedisResponse.error(ERROR_NO_SUCH_KEY);
  }

  protected abstract boolean executeRenameCommand(RedisKey key,
      RedisKey newKey,
      ExecutionHandlerContext context);

  protected abstract RedisResponse getTargetSameAsSourceResponse();

  protected abstract RedisResponse getSuccessResponse();

  protected abstract RedisResponse getKeyExistsResponse();

}
