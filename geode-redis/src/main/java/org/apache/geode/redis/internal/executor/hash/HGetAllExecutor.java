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

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

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
  public RedisResponse executeCommandWithResponse(Command command,
      ExecutionHandlerContext context) {
    ByteArrayWrapper key = command.getKey();
    RedisHashCommands redisHashCommands = createRedisHashCommands(context);
    Collection<ByteArrayWrapper> fieldsAndValues = redisHashCommands.hgetall(key);

    return RedisResponse.array(fieldsAndValues);
  }

}
