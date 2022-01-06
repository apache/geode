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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;
import static org.apache.geode.redis.internal.data.RedisSet.smove;

import java.util.Arrays;
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
    RedisKey sourceKey = command.getKey();
    RedisKey destKey = new RedisKey(commandElems.get(2));
    byte[] member = commandElems.get(3);
    RegionProvider regionProvider = context.getRegionProvider();

    try {
      regionProvider.ensureKeyIsLocal(sourceKey);
      regionProvider.ensureKeyIsLocal(destKey);
    } catch (RedisDataMovedException ex) {
      return RedisResponse.crossSlot(ERROR_WRONG_SLOT);
    }

    int removed = context.lockedExecute(sourceKey, Arrays.asList(sourceKey, destKey),
        () -> smove(sourceKey, destKey, member, regionProvider));

    return RedisResponse.integer(removed);
  }
}
