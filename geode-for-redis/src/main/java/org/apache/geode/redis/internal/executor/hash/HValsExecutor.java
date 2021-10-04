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

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * <pre>
 *  Implementation of the Redis HVAL command to returns all values in the hash stored at a given
 * key.
 *
 * 	Examples:
 *
 * 	redis> HSET myhash field1 "Hello"
 * 	(integer) 1
 * 	redis> HSET myhash field2 "World"
 * 	(integer) 1
 * 	redis> HVALS myhash
 * 	1) "Hello"
 * 	2) "World"
 * </pre>
 */
public class HValsExecutor extends AbstractExecutor {

  /**
   * <pre>
   * 	redis>
   * </pre>
   *
   * @param command the command runtime handle
   * @param context the context (ex: region provider)
   */
  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    RedisKey key = command.getKey();

    RedisHashCommands redisHashCommands = context.getHashCommands();
    Collection<byte[]> values = redisHashCommands.hvals(key);

    if (values.isEmpty()) {
      return RedisResponse.emptyArray();
    }

    return RedisResponse.array(values);
  }

}
