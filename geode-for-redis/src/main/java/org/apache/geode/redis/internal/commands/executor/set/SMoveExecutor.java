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

import static org.apache.geode.redis.internal.data.RedisSet.smove;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisDataMovedException;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.services.RegionProvider;

public class SMoveExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    List<RedisKey> commandKeys = command.getProcessedCommandKeys();
    List<RedisKey> setKeys = commandKeys.subList(1, 3);

    byte[] member = commandElems.get(3);
    RegionProvider regionProvider = context.getRegionProvider();
    try {
      for (RedisKey k : setKeys) {
        regionProvider.ensureKeyIsLocal(k);
      }
    } catch (RedisDataMovedException ex) {
      return RedisResponse.error(ex.getMessage());
    }

    int removed = context.lockedExecute(setKeys.get(0), new ArrayList<>(setKeys),
        () -> smove(regionProvider, setKeys, member));

    return RedisResponse.integer(removed);
  }
}
