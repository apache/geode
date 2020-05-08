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
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;

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
public class HValsExecutor extends HashExecutor {

  /**
   * <pre>
   * 	redis>
   * </pre>
   *
   * @param command the command runtime handle
   * @param context the context (ex: region provider)
   */
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_HASH, context);

    RedisHash map =
        context.getRegionProvider().getHashRegion().get(key);

    if (map == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }

    Collection<ByteArrayWrapper> vals = map.values();
    if (vals.isEmpty()) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }

    respondBulkStrings(command, context, vals);
  }

}
