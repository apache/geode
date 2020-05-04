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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

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

public class SRandMemberExecutor extends SetExecutor {

  private static final String ERROR_NOT_NUMERIC = "The count provided must be numeric";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command
          .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SRANDMEMBER));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    try (AutoCloseableLock regionLock = withRegionLock(context, key)) {
      Region<ByteArrayWrapper, SetDelta> region = getRegion(context);

      int count = 1;

      if (commandElems.size() > 2) {
        try {
          count = Coder.bytesToInt(commandElems.get(2));
        } catch (NumberFormatException e) {
          command.setResponse(
              Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
          return;
        }
      }

      Set<ByteArrayWrapper> set = SetDelta.members(region, key);

      if (set == null || count == 0) {
        command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
        return;
      }

      int members = set.size();

      if (members <= count && count != 1) {
        respondBulkStrings(command, context, new HashSet<ByteArrayWrapper>(set));
        return;
      }

      Random rand = new Random();

      ByteArrayWrapper[] entries = set.toArray(new ByteArrayWrapper[members]);

      try {
        if (count == 1) {
          ByteArrayWrapper randEntry = entries[rand.nextInt(entries.length)];
          command.setResponse(
              Coder.getBulkStringResponse(context.getByteBufAllocator(), randEntry.toBytes()));
        } else if (count > 0) {
          Set<ByteArrayWrapper> randEntries = new HashSet<>();
          do {
            ByteArrayWrapper s = entries[rand.nextInt(entries.length)];
            randEntries.add(s);
          } while (randEntries.size() < count);
          command.setResponse(Coder.getArrayResponse(context.getByteBufAllocator(), randEntries));
        } else {
          count = -count;
          List<ByteArrayWrapper> randEntries = new ArrayList<>();
          for (int i = 0; i < count; i++) {
            ByteArrayWrapper s = entries[rand.nextInt(entries.length)];
            randEntries.add(s);
          }
          command.setResponse(Coder.getArrayResponse(context.getByteBufAllocator(), randEntries));
        }
      } catch (CoderException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
            RedisConstants.SERVER_ERROR_MESSAGE));
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
  }
}
