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
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisResponse;
import org.apache.geode.redis.internal.RegionProvider;

public abstract class SetOpExecutor extends SetExecutor {

  @Override
  public RedisResponse executeCommandWithResponse(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    int setsStartIndex = isStorage() ? 2 : 1;

    RegionProvider regionProvider = context.getRegionProvider();
    ByteArrayWrapper destination = null;
    if (isStorage()) {
      destination = command.getKey();
    }

    ByteArrayWrapper firstSetKey = new ByteArrayWrapper(commandElems.get(setsStartIndex++));
    RedisResponse response;

    if (destination != null) {
      try (AutoCloseableLock regionLock = withRegionLock(context, destination)) {
        response = doActualSetOperation(command, context, commandElems, setsStartIndex,
            regionProvider, destination, firstSetKey);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        response = RedisResponse.error("Thread interrupted");
      } catch (TimeoutException e) {
        return RedisResponse.error("Timeout acquiring lock. Please try again");
      }
    } else {
      response = doActualSetOperation(command, context, commandElems, setsStartIndex,
          regionProvider, destination, firstSetKey);
    }

    return response;
  }

  private RedisResponse doActualSetOperation(Command command, ExecutionHandlerContext context,
      List<byte[]> commandElems, int setsStartIndex,
      RegionProvider regionProvider, ByteArrayWrapper destination,
      ByteArrayWrapper firstSetKey) {
    Region<ByteArrayWrapper, RedisData> region = this.getRegion(context);
    Set<ByteArrayWrapper> firstSet = new RedisSetInRegion(region).smembers(firstSetKey);

    List<Set<ByteArrayWrapper>> setList = new ArrayList<>();
    for (int i = setsStartIndex; i < commandElems.size(); i++) {
      ByteArrayWrapper key = new ByteArrayWrapper(commandElems.get(i));

      Set<ByteArrayWrapper> entry = new RedisSetInRegion(region).smembers(key);
      if (entry != null) {
        setList.add(entry);
      } else if (this instanceof SInterExecutor) {
        setList.add(new HashSet<>());
      }
    }

    if (setList.isEmpty() && !isStorage()) {
      return respondBulkStrings(firstSet);
    }

    RedisResponse response;

    Set<ByteArrayWrapper> resultSet = setOp(firstSet, setList);
    if (isStorage()) {
      regionProvider.removeKey(destination);
      if (resultSet != null) {
        if (!resultSet.isEmpty()) {
          region.put(destination, new RedisSet(resultSet));
        }
        response = RedisResponse.integer(resultSet.size());
      } else {
        response = RedisResponse.integer(0);
      }
    } else {
      if (resultSet == null || resultSet.isEmpty()) {
        response = RedisResponse.emptyArray();
      } else {
        response = respondBulkStrings(resultSet);
      }
    }

    return response;
  }

  protected abstract boolean isStorage();

  protected abstract Set<ByteArrayWrapper> setOp(Set<ByteArrayWrapper> firstSet,
      List<Set<ByteArrayWrapper>> setList);
}
