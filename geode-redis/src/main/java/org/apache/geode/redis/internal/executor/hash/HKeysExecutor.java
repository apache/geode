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
package org.apache.geode.redis.internal.executor.hash;

import java.util.List;

import org.apache.geode.cache.TimeoutException;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;

/**
 * <pre>
 * Implementation for the
 * HKEYS command to return list of fields in the hash.
 * It will return an empty list if the key does not exist.
 *
 * Examples:
 *
 * redis> HSET myhash field1 "Hello"
 * (integer) 1
 * redis> HSET myhash field2 "World"
 * (integer) 1
 * redis> HKEYS myhash
 * 1) "field1"
 * 2) "field2"
 *
 * </pre>
 */
public class HKeysExecutor extends HashExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    ByteArrayWrapper key = command.getKey();
    List<ByteArrayWrapper> keys;
    try (AutoCloseableLock regionLock = withRegionLock(context, key)) {
      RedisHash keyMap = getMap(context, key);
      keys = keyMap.keys();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), "Thread interrupted."));
      return;
    } catch (TimeoutException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          "Timeout acquiring lock. Please try again."));
      return;
    }

    if (keys.isEmpty()) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }

    respondBulkStrings(command, context, keys);
  }
}
