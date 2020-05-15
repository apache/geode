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

package org.apache.geode.redis.internal.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.set.RedisSetCommands;
import org.apache.geode.redis.internal.executor.set.RedisSetCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.string.StringExecutor;

public class RenameExecutor extends StringExecutor {
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    if (commandElems.size() < 3) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ArityDef.KEYS));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    ByteArrayWrapper newKey = new ByteArrayWrapper(commandElems.get(2));

    try (@SuppressWarnings("unused")
    AutoCloseableLock lockForOldKey = context.getLockService().lock(key)) {
      try (@SuppressWarnings("unused")
      AutoCloseableLock lockForNewKey = context.getLockService().lock(newKey)) {
        RedisDataType redisDataType = context.getKeyRegistrar().getType(key);
        if (redisDataType == null) {
          command.setResponse(
              Coder.getErrorResponse(context.getByteBufAllocator(),
                  RedisConstants.ERROR_NO_SUCH_KEY));
          return;
        }
        switch (redisDataType) {
          case REDIS_STRING:
            @SuppressWarnings("unchecked")
            Region<ByteArrayWrapper, Object> region =
                (Region<ByteArrayWrapper, Object>) context.getRegionProvider()
                    .getRegionForType(redisDataType);
            Object value = region.get(key);
            context.getKeyRegistrar().register(newKey, redisDataType);
            region.put(newKey, value);
            removeEntry(key, redisDataType, context);
            break;
          case REDIS_HASH:
            // TODO this all needs to be done atomically. Add RENAME support to RedisHashCommands
            RedisHashCommands redisHashCommands =
                new RedisHashCommandsFunctionExecutor(context.getRegionProvider().getDataRegion());
            Collection<ByteArrayWrapper> fieldsAndValues = redisHashCommands.hgetall(key);
            redisHashCommands.del(key);
            redisHashCommands.del(newKey);
            redisHashCommands.hset(newKey, new ArrayList<>(fieldsAndValues), false);
            break;
          case REDIS_SET:
            // TODO this all needs to be done atomically. Add RENAME support to RedisSetCommands
            RedisSetCommands redisSetCommands =
                new RedisSetCommandsFunctionExecutor(context.getRegionProvider().getDataRegion());
            Set<ByteArrayWrapper> members = redisSetCommands.smembers(key);
            redisSetCommands.del(key);
            redisSetCommands.del(newKey);
            redisSetCommands.sadd(newKey, new ArrayList<>(members));
            break;
          case REDIS_LIST:
            throw new RuntimeException("Renaming List isn't supported");
          case REDIS_SORTEDSET:
            throw new RuntimeException("Renaming SortedSet isn't supported");
          case NONE:
          default:
            break;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        command.setResponse(
            Coder.getErrorResponse(context.getByteBufAllocator(), "Thread interrupted."));
        return;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), "Thread interrupted."));
      return;
    }

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), "OK"));
  }
}
