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

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * <pre>
 *
 * Implementation of the HINCRBY command to increment the number stored at field
 * in the hash stored at key by increment value.
 *
 * Examples:
 *
 * redis> HSET myhash field 5
 * (integer) 1
 * redis> HINCRBY myhash field 1
 * (integer) 6
 * redis> HINCRBY myhash field -1
 * (integer) 5
 * redis> HINCRBY myhash field -10
 * (integer) -5
 *
 *
 * </pre>
 */
public class HIncrByExecutor extends HashExecutor {

  private static final String ERROR_INCREMENT_NOT_USABLE =
      "The increment on this key must be numeric";

  private static final int INCREMENT_INDEX = FIELD_INDEX + 1;

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    RedisKey key = command.getKey();
    byte[] field = commandElems.get(FIELD_INDEX);

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    long increment;
    try {
      increment = Coder.bytesToLong(incrArray);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_INCREMENT_NOT_USABLE);
    }

    RedisHashCommands redisHashCommands = context.getRedisHashCommands();

    long value = redisHashCommands.hincrby(key, field, increment);
    return RedisResponse.integer(value);
  }

}
