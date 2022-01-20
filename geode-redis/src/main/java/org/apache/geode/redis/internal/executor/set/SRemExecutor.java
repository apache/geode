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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;

public class SRemExecutor extends SetExecutor {
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<ByteArrayWrapper> commandElements = command.getProcessedCommandWrappers();

    if (commandElements.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SREM));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_SET, context);

    RedisSet geodeRedisSet =
        new GeodeRedisSetWithFunctions(key, context.getRegionProvider().getSetRegion());


    ArrayList<ByteArrayWrapper> membersToRemove =
        new ArrayList<>(commandElements.subList(2, commandElements.size()));
    AtomicBoolean setWasDeleted = new AtomicBoolean();
    long membersRemoved = geodeRedisSet.srem(membersToRemove, setWasDeleted);
    if (setWasDeleted.get()) {
      context.getKeyRegistrar().unregisterIfType(key, RedisDataType.REDIS_SET);
    }
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), membersRemoved));
  }
}