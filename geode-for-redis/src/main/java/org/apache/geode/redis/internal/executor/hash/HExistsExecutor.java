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
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

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
public class HExistsExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    byte[] field = commandElems.get(HASH_FIELD_INDEX);
    RedisKey key = command.getKey();
    RedisHashCommands redisHashCommands = context.getHashCommands();

    return RedisResponse.integer(redisHashCommands.hexists(key, field));
  }

}
