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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SMoveExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<RedisKey, RedisData> region = context.getRegion();
    RedisKey source = command.getKey();
    RedisKey destination = new RedisKey(commandElems.get(2));
    byte[] member = commandElems.get(3);

    // TODO: this command should lock both source and destination before changing them

    String destinationType = context.dataLockedExecute(destination, false, RedisData::type);
    if (!destinationType.equals(REDIS_SET.toString()) && !destinationType.equals("none")) {
      return RedisResponse.wrongType(ERROR_WRONG_TYPE);
    }

    ArrayList<byte[]> membersToRemove = new ArrayList<>(Collections.singletonList(member));
    boolean removed = 1 == context.setLockedExecute(source, false,
        set -> set.srem(membersToRemove, region, source));
    if (removed) {
      ArrayList<byte[]> membersToAdd = new ArrayList<>(Collections.singletonList(member));
      context.setLockedExecute(destination, false,
          set -> set.sadd(membersToAdd, region, destination));
    }
    return RedisResponse.integer(removed);
  }
}
