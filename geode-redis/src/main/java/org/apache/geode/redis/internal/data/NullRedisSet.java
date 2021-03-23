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
 *
 */

package org.apache.geode.redis.internal.data;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.executor.set.RedisSetCommands;
import org.apache.geode.redis.internal.executor.set.RedisSetCommandsFunctionInvoker;

class NullRedisSet extends RedisSet {

  NullRedisSet() {
    super(new HashSet<>());
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  Collection<ByteArrayWrapper> spop(Region<RedisKey, RedisData> region,
      RedisKey key, int popCount) {
    return emptyList();
  }

  @Override
  Collection<ByteArrayWrapper> srandmember(int count) {
    return emptyList();
  }

  @Override
  public boolean sismember(ByteArrayWrapper member) {
    return false;
  }

  @Override
  public int scard() {
    return 0;
  }

  @Override
  long sadd(ArrayList<ByteArrayWrapper> membersToAdd, Region<RedisKey, RedisData> region,
      RedisKey key) {
    region.create(key, new RedisSet(membersToAdd));
    return membersToAdd.size();
  }

  @Override
  long srem(ArrayList<ByteArrayWrapper> membersToRemove, Region<RedisKey, RedisData> region,
      RedisKey key) {
    return 0;
  }

  @Override
  @VisibleForTesting
  Set<ByteArrayWrapper> smembers() {
    // some callers want to be able to modify the set returned
    return new HashSet<>();
  }

  private enum SetOp {
    UNION, INTERSECTION, DIFF
  }

  public int sunionstore(CommandHelper helper, RedisKey destination,
      ArrayList<RedisKey> setKeys) {
    return doSetOp(SetOp.UNION, helper, destination, setKeys);
  }

  public int sinterstore(CommandHelper helper, RedisKey destination,
      ArrayList<RedisKey> setKeys) {
    return doSetOp(SetOp.INTERSECTION, helper, destination, setKeys);
  }

  public int sdiffstore(CommandHelper helper, RedisKey destination,
      ArrayList<RedisKey> setKeys) {
    return doSetOp(SetOp.DIFF, helper, destination, setKeys);
  }

  private int doSetOp(SetOp setOp, CommandHelper helper,
      RedisKey destination, ArrayList<RedisKey> setKeys) {
    ArrayList<Set<ByteArrayWrapper>> nonDestinationSets =
        fetchSets(helper.getRegion(), setKeys, destination);
    return helper.getStripedExecutor()
        .execute(destination,
            () -> doSetOpWhileLocked(setOp, helper, destination, nonDestinationSets));
  }

  private int doSetOpWhileLocked(SetOp setOp, CommandHelper helper,
      RedisKey destination,
      ArrayList<Set<ByteArrayWrapper>> nonDestinationSets) {
    Set<ByteArrayWrapper> result = computeSetOp(setOp, nonDestinationSets, helper, destination);
    if (result.isEmpty()) {
      helper.getRegion().remove(destination);
      return 0;
    } else {
      helper.getRegion().put(destination, new RedisSet(result));
      return result.size();
    }
  }

  private Set<ByteArrayWrapper> computeSetOp(SetOp setOp,
      ArrayList<Set<ByteArrayWrapper>> nonDestinationSets,
      CommandHelper helper,
      RedisKey destination) {
    Set<ByteArrayWrapper> result = null;
    if (nonDestinationSets.isEmpty()) {
      return emptySet();
    }
    for (Set<ByteArrayWrapper> set : nonDestinationSets) {
      if (set == null) {
        set = helper.getRedisSet(destination, false).smembers();
      }
      if (result == null) {
        result = set;
      } else {
        switch (setOp) {
          case UNION:
            result.addAll(set);
            break;
          case INTERSECTION:
            result.retainAll(set);
            break;
          case DIFF:
            result.removeAll(set);
            break;
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
  private ArrayList<Set<ByteArrayWrapper>> fetchSets(
      Region<RedisKey, RedisData> region,
      ArrayList<RedisKey> setKeys,
      RedisKey destination) {
    ArrayList<Set<ByteArrayWrapper>> result = new ArrayList<>(setKeys.size());
    RedisSetCommands redisSetCommands = new RedisSetCommandsFunctionInvoker(region);
    for (RedisKey key : setKeys) {
      if (key.equals(destination)) {
        result.add(null);
      } else {
        result.add(redisSetCommands.internalsmembers(key));
      }
    }
    return result;
  }
}
