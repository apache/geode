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
 *
 * Implements the Redis HEXISTS command to determine if a hash field exists for a given key.
 *
 * Examples:
 *
 * redis> HSET myhash field1 "foo"
 * (integer) 1
 * redis> HEXISTS myhash field1
 * (integer) 1
 * redis> HEXISTS myhash field2
 * (integer) 0
 *
 * </pre>
 */
public class HExistsExecutor extends HashExecutor {

  private static final int NOT_EXISTS = 0;

  private static final int EXISTS = 1;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    boolean hasField;
    byte[] byteField = commandElems.get(FIELD_INDEX);
    ByteArrayWrapper field = new ByteArrayWrapper(byteField);
    ByteArrayWrapper key = command.getKey();
    try (AutoCloseableLock regionLock = withRegionLock(context, key)) {
      RedisHash map = getMap(context, key);

      if (map == null || map.isEmpty()) {
        command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
        return;
      }
      hasField = map.containsKey(field);
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

    if (hasField) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), EXISTS));
    } else {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
    }

  }

}
