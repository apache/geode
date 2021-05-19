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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

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
  Collection<byte[]> spop(Region<RedisKey, RedisData> region, RedisKey key, int popCount) {
    return emptyList();
  }

  @Override
  Collection<byte[]> srandmember(int count) {
    return emptyList();
  }

  @Override
  public boolean sismember(byte[] member) {
    return false;
  }

  @Override
  public int scard() {
    return 0;
  }

  @Override
  long sadd(List<byte[]> membersToAdd, Region<RedisKey, RedisData> region, RedisKey key) {
    region.create(key, new RedisSet(membersToAdd));
    return membersToAdd.size();
  }

  @Override
  long srem(List<byte[]> membersToRemove, Region<RedisKey, RedisData> region, RedisKey key) {
    return 0;
  }

  @Override
  public Set<byte[]> smembers() {
    // some callers want to be able to modify the set returned
    return Collections.emptySet();
  }

  private enum SetOp {
    UNION, INTERSECTION, DIFF
  }

  public int sunionstore(CommandHelper helper, RedisKey destination, List<RedisKey> setKeys) {
    return doSetOp(SetOp.UNION, helper, destination, setKeys);
  }

  public int sinterstore(CommandHelper helper, RedisKey destination, List<RedisKey> setKeys) {
    return doSetOp(SetOp.INTERSECTION, helper, destination, setKeys);
  }

  public int sdiffstore(CommandHelper helper, RedisKey destination, List<RedisKey> setKeys) {
    return doSetOp(SetOp.DIFF, helper, destination, setKeys);
  }

  private int doSetOp(SetOp setOp, CommandHelper helper, RedisKey destination,
      List<RedisKey> setKeys) {
    List<Set<byte[]>> nonDestinationSets = fetchSets(helper.getRegion(), setKeys, destination);
    return helper.getStripedExecutor()
        .execute(destination,
            () -> doSetOpWhileLocked(setOp, helper, destination, nonDestinationSets));
  }

  private int doSetOpWhileLocked(SetOp setOp, CommandHelper helper, RedisKey destination,
      List<Set<byte[]>> nonDestinationSets) {
    Set<byte[]> result = computeSetOp(setOp, nonDestinationSets, helper, destination);
    if (result.isEmpty()) {
      helper.getRegion().remove(destination);
      return 0;
    } else {
      helper.getRegion().put(destination, new RedisSet(result));
      return result.size();
    }
  }

  private Set<byte[]> computeSetOp(SetOp setOp, List<Set<byte[]>> nonDestinationSets,
      CommandHelper helper, RedisKey destination) {
    ObjectOpenCustomHashSet<byte[]> result = null;
    if (nonDestinationSets.isEmpty()) {
      return emptySet();
    }
    for (Set<byte[]> set : nonDestinationSets) {
      if (set == null) {
        set = helper.getRedisSet(destination, false).smembers();
      }
      if (result == null) {
        result = new ObjectOpenCustomHashSet<>(set, ByteArrays.HASH_STRATEGY);
      } else {
        switch (setOp) {
          case UNION:
            result.addAll(set);
            break;
          case INTERSECTION:
            set = new ObjectOpenCustomHashSet<>(set, ByteArrays.HASH_STRATEGY);
            result.retainAll(set);
            break;
          case DIFF:
            set = new ObjectOpenCustomHashSet<>(set, ByteArrays.HASH_STRATEGY);
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
  private List<Set<byte[]>> fetchSets(Region<RedisKey, RedisData> region, List<RedisKey> setKeys,
      RedisKey destination) {
    List<Set<byte[]>> result = new ArrayList<>(setKeys.size());
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
