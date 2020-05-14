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

import java.util.Collection;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;

/**
 * <pre>
 * Implements the Redis HGETALL command to return
 *
 * Returns all fields and values of the hash stored at key.
 *
 * Examples:
 *
 * redis> HSET myhash field1 "Hello"
 * (integer) 1
 * redis> HSET myhash field2 "World"
 * (integer) 1
 * redis> HGETALL myhash
 * 1) "field1"
 * 2) "Hello"
 * 3) "field2"
 * 4) "World"
 * </pre>
 */
public class HGetAllExecutor extends HashExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    ByteArrayWrapper key = command.getKey();
    RedisHashCommands redisHashCommands =
        new RedisHashCommandsFunctionExecutor(context.getRegionProvider().getDataRegion());
    Collection<ByteArrayWrapper> fieldsAndValues = redisHashCommands.hgetall(key);
    try {
      command.setResponse(Coder.getArrayResponse(context.getByteBufAllocator(), fieldsAndValues));
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          RedisConstants.SERVER_ERROR_MESSAGE));
    }
  }

}
