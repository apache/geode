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
package org.apache.geode.redis.internal.executor.set;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.RedisResponse;

public class SMoveExecutor extends SetExecutor {

  private static final int MOVED = 1;

  private static final int NOT_MOVED = 0;

  @Override
  public RedisResponse executeCommandWithResponse(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    ByteArrayWrapper source = command.getKey();
    ByteArrayWrapper destination = new ByteArrayWrapper(commandElems.get(2));
    ByteArrayWrapper member = new ByteArrayWrapper(commandElems.get(3));

    // TODO: remove the need for this type check
    RedisDataType destinationType = context.getKeyRegistrar().getType(destination);
    if (destinationType != null && destinationType != RedisDataType.REDIS_SET) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }

    RedisSetCommands redisSetCommands = createRedisSetCommands(context);

    boolean removed = redisSetCommands.srem(source,
        new ArrayList<>(Collections.singletonList(member))) == 1;
    if (!removed) {
      return RedisResponse.integer(NOT_MOVED);
    }
    redisSetCommands.sadd(destination, new ArrayList<>(Collections.singletonList(member)));
    return RedisResponse.integer(MOVED);
  }
}
