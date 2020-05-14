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

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisDataType;
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

    if (!context.getKeyRegistrar().isRegistered(key)) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_NO_SUCH_KEY));
      return;
    }

    try (@SuppressWarnings("unused")
    AutoCloseableLock lockForOldKey = context.getLockService().lock(key)) {
      try (@SuppressWarnings("unused")
      AutoCloseableLock lockForNewKey = context.getLockService().lock(newKey)) {
        RedisDataType redisDataType = context.getKeyRegistrar().getType(key);
        switch (redisDataType) {
          case REDIS_STRING:
          case REDIS_HASH:
          case REDIS_SET:
            @SuppressWarnings("unchecked")
            Region<ByteArrayWrapper, Object> region =
                (Region<ByteArrayWrapper, Object>) context.getRegionProvider()
                    .getRegionForType(redisDataType);
            Object value = region.get(key);
            context.getKeyRegistrar().register(newKey, redisDataType);
            region.put(newKey, value);
            removeEntry(key, redisDataType, context);
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
