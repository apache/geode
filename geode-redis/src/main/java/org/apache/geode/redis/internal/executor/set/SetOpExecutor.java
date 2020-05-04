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
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Extendable;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RegionProvider;

public abstract class SetOpExecutor extends SetExecutor implements Extendable {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    int setsStartIndex = isStorage() ? 2 : 1;

    if (commandElems.size() < setsStartIndex + 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    RegionProvider regionProvider = context.getRegionProvider();
    ByteArrayWrapper destination = null;
    if (isStorage()) {
      destination = command.getKey();
    }

    ByteArrayWrapper firstSetKey = new ByteArrayWrapper(commandElems.get(setsStartIndex++));
    if (destination != null) {
      try (AutoCloseableLock regionLock = withRegionLock(context, destination)) {
        doActualSetOperation(command, context, commandElems, setsStartIndex, regionProvider,
            destination, firstSetKey);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        command.setResponse(
            Coder.getErrorResponse(context.getByteBufAllocator(), "Thread interrupted."));
      } catch (TimeoutException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
            "Timeout acquiring lock. Please try again."));
      }
    } else {
      doActualSetOperation(command, context, commandElems, setsStartIndex, regionProvider,
          destination, firstSetKey);
    }
  }

  private boolean doActualSetOperation(Command command, ExecutionHandlerContext context,
      List<byte[]> commandElems, int setsStartIndex,
      RegionProvider regionProvider, ByteArrayWrapper destination,
      ByteArrayWrapper firstSetKey) {
    Region<ByteArrayWrapper, SetDelta> region = this.getRegion(context);
    Set<ByteArrayWrapper> firstSet = SetDelta.members(region, firstSetKey);

    List<Set<ByteArrayWrapper>> setList = new ArrayList<>();
    for (int i = setsStartIndex; i < commandElems.size(); i++) {
      ByteArrayWrapper key = new ByteArrayWrapper(commandElems.get(i));

      Set<ByteArrayWrapper> entry = SetDelta.members(region, key);
      if (entry != null) {
        setList.add(entry);
      } else if (this instanceof SInterExecutor) {
        setList.add(new HashSet<>());
      }
    }

    if (setList.isEmpty() && !isStorage()) {
      respondBulkStrings(command, context, firstSet);
      return true;
    }

    Set<ByteArrayWrapper> resultSet = setOp(firstSet, setList);
    if (isStorage()) {
      regionProvider.removeKey(destination);
      if (resultSet != null) {
        if (!resultSet.isEmpty()) {
          region.put(destination, new SetDelta(resultSet));
          context.getKeyRegistrar().register(destination, RedisDataType.REDIS_SET);
        }
        command
            .setResponse(
                Coder.getIntegerResponse(context.getByteBufAllocator(), resultSet.size()));
      } else {
        command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
      }
    } else {
      if (resultSet == null || resultSet.isEmpty()) {
        command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      } else {
        respondBulkStrings(command, context, resultSet);
      }
    }
    return false;
  }

  protected abstract boolean isStorage();

  protected abstract Set<ByteArrayWrapper> setOp(Set<ByteArrayWrapper> firstSet,
      List<Set<ByteArrayWrapper>> setList);
}
