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
import java.util.Random;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class SPopExecutor extends SetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    int popCount = 1;

    if (commandElems.size() < 2 || commandElems.size() > 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SPOP));
      return;
    }

    if (commandElems.size() == 3) {
      try {
        popCount = Integer.parseInt(new String(commandElems.get(2)));
      } catch (NumberFormatException nex) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SPOP));
        return;
      }
    }

    ByteArrayWrapper key = command.getKey();

    List<ByteArrayWrapper> popped = new ArrayList<>();
    try (AutoCloseableLock regionLock = withRegionLock(context, key)) {
      Region<ByteArrayWrapper, DeltaSet> region = getRegion(context);
      DeltaSet original = region.get(key);
      if (original == null) {
        command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
        return;
      }

      synchronized (original) {
        int originalSize = original.size();
        if (originalSize == 0) {
          command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
          return;
        }

        if (popCount > originalSize) {
          // remove them all
          popped.addAll(original.members());
        } else {
          ByteArrayWrapper[] setMembers =
              original.members().toArray(new ByteArrayWrapper[originalSize]);
          Random rand = new Random();
          while (popped.size() < popCount) {
            popped.add(setMembers[rand.nextInt(originalSize)]);
          }
        }
        DeltaSet.srem(region, key, popped);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), "Thread interrupted."));
      return;
    } catch (TimeoutException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          "Timeout acquiring lock. Please try again."));
      return;
    }

    try {
      if (popCount == 1) {
        command
            .setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), popped.get(0)));
      } else {
        command.setResponse(Coder.getArrayResponse(context.getByteBufAllocator(), popped));
      }
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          RedisConstants.SERVER_ERROR_MESSAGE));
    }
  }

}
