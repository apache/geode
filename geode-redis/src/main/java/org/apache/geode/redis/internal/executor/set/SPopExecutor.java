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

import java.util.Collection;
import java.util.List;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;

public class SPopExecutor extends SetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    int popCount = 1;
    if (commandElems.size() == 3) {
      popCount = Integer.parseInt(new String(commandElems.get(2)));
    }

    ByteArrayWrapper key = command.getKey();
    RedisSetCommands redisSetCommands =
        new RedisSetCommandsFunctionExecutor(context.getRegionProvider().getSetRegion());
    Collection<ByteArrayWrapper> popped = redisSetCommands.spop(key, popCount);
    if (popped.isEmpty()) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }
    try {
      if (popCount == 1) {
        command.setResponse(
            Coder.getBulkStringResponse(context.getByteBufAllocator(), popped.iterator().next()));
      } else {
        command.setResponse(Coder.getArrayResponse(context.getByteBufAllocator(), popped));
      }
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          RedisConstants.SERVER_ERROR_MESSAGE));
    }
  }
}
