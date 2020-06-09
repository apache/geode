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

import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisDataType;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SMoveExecutor extends SetExecutor {

  private static final int MOVED = 1;

  private static final int NOT_MOVED = 0;

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    ByteArrayWrapper source = command.getKey();
    ByteArrayWrapper destination = new ByteArrayWrapper(commandElems.get(2));
    ByteArrayWrapper member = new ByteArrayWrapper(commandElems.get(3));

    String destinationType = getRedisKeyCommands(context).type(destination);
    if (!destinationType.equals(RedisDataType.REDIS_SET.toString())
        && !destinationType.equals("none")) {
      return RedisResponse.wrongType(RedisConstants.ERROR_WRONG_TYPE);
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
