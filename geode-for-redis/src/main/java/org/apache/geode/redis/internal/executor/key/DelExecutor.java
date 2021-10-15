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
 *
 */
package org.apache.geode.redis.internal.executor.key;

import java.util.List;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class DelExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<RedisKey> commandElems = command.getProcessedCommandKeys();

    long numRemoved = commandElems
        .subList(1, commandElems.size())
        .stream()
        .filter(key -> del(context, key))
        .count();

    return RedisResponse.integer(numRemoved);
  }

  public static boolean del(ExecutionHandlerContext context, RedisKey key) {
    return context.dataLockedExecute(key, false, data -> {
      // data unused in this lambda but needs to be created to possibly trigger MOVED exception
      return context.getRegion().remove(key) != null;
    });
  }
}
