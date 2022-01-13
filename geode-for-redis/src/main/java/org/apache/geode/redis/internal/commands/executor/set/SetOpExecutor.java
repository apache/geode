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

import static java.util.Collections.emptySet;
import static org.apache.geode.redis.internal.data.RedisSet.sdiff;
import static org.apache.geode.redis.internal.data.RedisSet.sdiffstore;
import static org.apache.geode.redis.internal.data.RedisSet.sinter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisDataMovedException;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.data.RedisSet.MemberSet;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.services.RegionProvider;

public abstract class SetOpExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    int setsStartIndex = 1;

    if (isStorage()) {
      setsStartIndex++;
    }

    List<RedisKey> commandElements = command.getProcessedCommandKeys();
    List<RedisKey> setKeys = commandElements.subList(setsStartIndex, commandElements.size());
    RegionProvider regionProvider = context.getRegionProvider();
    try {
      for (RedisKey k : setKeys) {
        regionProvider.ensureKeyIsLocal(k);
      }
    } catch (RedisDataMovedException ex) {
      return RedisResponse.error(ex.getMessage());
    }

    /*
     * SINTERSTORE, SUNION, SUNIONSTORE currently use the else part of the code
     * for their implementation.
     * TODO: Once the above commands have been implemented remove the if else and
     * refactor so the implementation is in the executor. After delete doActualSetOperation,
     * doStoreSetOp,
     * doStoreSetOpWhileLocked, computeStoreSetOp, fetchSets
     */
    if (command.isOfType(RedisCommandType.SDIFF) || command.isOfType(RedisCommandType.SDIFFSTORE)) {
      if (isStorage()) {
        RedisKey destinationKey = command.getKey();
        int resultSize = context.lockedExecute(destinationKey, new ArrayList<>(setKeys),
            () -> sdiffstore(regionProvider, destinationKey, setKeys));
        return RedisResponse.integer(resultSize);
      }

      Set<byte[]> resultSet = context.lockedExecute(setKeys.get(0), new ArrayList<>(setKeys),
          () -> sdiff(regionProvider, setKeys));
      return RedisResponse.array(resultSet, true);
    } else if (command.isOfType(RedisCommandType.SINTER)) {
      Set<byte[]> resultSet = context.lockedExecute(setKeys.get(0), new ArrayList<>(setKeys),
          () -> sinter(regionProvider, setKeys));
      return RedisResponse.array(resultSet, true);
    }
    return doActualSetOperation(command, context, setKeys);
  }

  private RedisResponse doActualSetOperation(Command command, ExecutionHandlerContext context,
      List<RedisKey> setKeys) {
    if (isStorage()) {
      RedisKey destination = command.getKey();
      int storeCount = doStoreSetOp(command.getCommandType(), context, destination, setKeys);
      return RedisResponse.integer(storeCount);
    }

    Set<byte[]> resultSet = null;
    for (RedisKey key : setKeys) {
      Set<byte[]> keySet = context.setLockedExecute(key, true,
          set -> new MemberSet(set.smembers()));
      if (resultSet == null) {
        resultSet = keySet;
      } else if (doSetOp(resultSet, keySet)) {
        break;
      }
    }

    return RedisResponse.array(resultSet, true);
  }


  protected int doStoreSetOp(RedisCommandType setOp, ExecutionHandlerContext context,
      RedisKey destination,
      List<RedisKey> setKeys) {
    List<MemberSet> nonDestinationSets = fetchSets(context, setKeys, destination);
    return context.lockedExecute(destination,
        () -> doStoreSetOpWhileLocked(setOp, context, destination, nonDestinationSets));
  }

  private int doStoreSetOpWhileLocked(RedisCommandType setOp, ExecutionHandlerContext context,
      RedisKey destination,
      List<MemberSet> nonDestinationSets) {
    Set<byte[]> result =
        computeStoreSetOp(setOp, nonDestinationSets, context, destination);
    if (result.isEmpty()) {
      context.getRegion().remove(destination);
      return 0;
    } else {
      context.getRegion().put(destination, new RedisSet(result));
      return result.size();
    }
  }

  private Set<byte[]> computeStoreSetOp(RedisCommandType setOp, List<MemberSet> nonDestinationSets,
      ExecutionHandlerContext context, RedisKey destination) {
    MemberSet result = null;
    if (nonDestinationSets.isEmpty()) {
      return emptySet();
    }
    for (MemberSet set : nonDestinationSets) {
      if (set == null) {
        RedisSet redisSet = context.getRedisSet(destination, false);
        set = new MemberSet(redisSet.smembers());
      }
      if (result == null) {
        result = set;
      } else {
        switch (setOp) {
          case SUNIONSTORE:
            result.addAll(set);
            break;
          case SINTERSTORE:
            result.retainAll(set);
            break;
          default:
            throw new IllegalStateException(
                "expected a set store command but found: " + setOp);
        }
      }
    }
    return result;
  }

  /**
   * Gets the set data for the given keys, excluding the destination if it was in setKeys.
   * The result will have an element for each corresponding key and a null element if
   * the corresponding key is the destination.
   * This is all done outside the striped executor to prevent a deadlock.
   */
  private List<MemberSet> fetchSets(ExecutionHandlerContext context, List<RedisKey> setKeys,
      RedisKey destination) {
    List<MemberSet> result = new ArrayList<>(setKeys.size());
    for (RedisKey key : setKeys) {
      if (key.equals(destination)) {
        result.add(null);
      } else {
        result.add(context.setLockedExecute(key, false,
            set -> new MemberSet(set.smembers())));
      }
    }
    return result;
  }

  /**
   * @return true if no further calls of doSetOp are needed
   */
  protected abstract boolean doSetOp(Set<byte[]> resultSet, Set<byte[]> nextSet);

  protected abstract boolean isStorage();

}
