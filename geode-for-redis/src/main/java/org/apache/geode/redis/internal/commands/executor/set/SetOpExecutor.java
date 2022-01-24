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


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.services.RegionProvider;

public abstract class SetOpExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    RegionProvider regionProvider = context.getRegionProvider();
    List<RedisKey> commandElements = command.getProcessedCommandKeys();
    List<RedisKey> setKeys = commandElements.subList(1, commandElements.size());

    RedisResponse result = context.lockedExecute(setKeys.get(0), new ArrayList<>(setKeys),
        () -> performCommand(regionProvider, setKeys));

    return result;
  }

  protected abstract RedisResponse performCommand(RegionProvider regionProvider,
      List<RedisKey> setKeys);
}


// Used for set operations without store that return an array
abstract class SetOpArrayResult extends SetOpExecutor {
  protected RedisResponse performCommand(RegionProvider regionProvider, List<RedisKey> setKeys) {
    return RedisResponse.array(getResult(regionProvider, setKeys), true);
  }

  protected abstract Set<byte[]> getResult(RegionProvider regionProvider, List<RedisKey> setKeys);
}


// Used for set operations with store that return an integer
abstract class SetOpIntegerResult extends SetOpExecutor {
  protected RedisResponse performCommand(RegionProvider regionProvider, List<RedisKey> setKeys) {
    RedisKey destKey = setKeys.remove(0);
    return RedisResponse.integer(getResult(regionProvider, setKeys, destKey));
  }

  protected abstract int getResult(RegionProvider regionProvider, List<RedisKey> setKeys,
      RedisKey destKey);
}
