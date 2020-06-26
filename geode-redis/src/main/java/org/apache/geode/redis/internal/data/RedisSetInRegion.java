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

import static java.util.Collections.emptySet;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.executor.set.RedisSetCommands;
import org.apache.geode.redis.internal.executor.set.RedisSetCommandsFunctionExecutor;

public class RedisSetInRegion extends RedisKeyInRegion implements RedisSetCommands {

  @SuppressWarnings("unchecked")
  public RedisSetInRegion(Region<ByteArrayWrapper, RedisData> region,
      RedisStats redisStats) {
    super(region, redisStats);
  }

  @Override
  public long sadd(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {

    RedisSet redisSet = checkType(getRedisData(key));

    if (redisSet != null) {
      return redisSet.sadd(membersToAdd, region, key);
    } else {
      region.create(key, new RedisSet(membersToAdd));
      return membersToAdd.size();
    }
  }

  public int sunionstore(StripedExecutor stripedExecutor, ByteArrayWrapper destination,
      ArrayList<ByteArrayWrapper> setKeys) {
    ArrayList<Set<ByteArrayWrapper>> nonDestinationSets = fetchSets(setKeys, destination);
    return stripedExecutor
        .execute(destination, () -> doSunionstore(destination, nonDestinationSets));
  }

  private int doSunionstore(ByteArrayWrapper destination,
      ArrayList<Set<ByteArrayWrapper>> nonDestinationSets) {
    RedisSet redisSet = checkType(region.get(destination));
    redisSet = new RedisSet(computeUnion(nonDestinationSets, redisSet));
    region.put(destination, redisSet);
    return redisSet.scard();
  }

  private Set<ByteArrayWrapper> computeUnion(ArrayList<Set<ByteArrayWrapper>> nonDestinationSets,
      RedisSet redisSet) {
    Set<ByteArrayWrapper> result = null;
    if (nonDestinationSets.isEmpty()) {
      return emptySet();
    }
    for (Set<ByteArrayWrapper> set : nonDestinationSets) {
      if (set == null) {
        set = redisSet.smembers();
      }
      if (result == null) {
        result = set;
      } else {
        result.addAll(set);
      }
    }
    return result;
  }

  public int sinterstore(StripedExecutor stripedExecutor, ByteArrayWrapper destination,
      ArrayList<ByteArrayWrapper> setKeys) {
    ArrayList<Set<ByteArrayWrapper>> nonDestinationSets = fetchSets(setKeys, destination);
    return stripedExecutor
        .execute(destination, () -> doSinterstore(destination, nonDestinationSets));
  }

  private int doSinterstore(ByteArrayWrapper destination,
      ArrayList<Set<ByteArrayWrapper>> nonDestinationSets) {
    RedisSet redisSet = checkType(region.get(destination));
    redisSet = new RedisSet(computeIntersection(nonDestinationSets, redisSet));
    region.put(destination, redisSet);
    return redisSet.scard();
  }

  private Set<ByteArrayWrapper> computeIntersection(
      ArrayList<Set<ByteArrayWrapper>> nonDestinationSets, RedisSet redisSet) {
    Set<ByteArrayWrapper> result = null;
    if (nonDestinationSets.isEmpty()) {
      return emptySet();
    }
    for (Set<ByteArrayWrapper> set : nonDestinationSets) {
      if (set == null) {
        set = redisSet.smembers();
      }
      if (result == null) {
        result = set;
      } else {
        result.retainAll(set);
      }
    }
    return result;
  }

  public int sdiffstore(StripedExecutor stripedExecutor, ByteArrayWrapper destination,
      ArrayList<ByteArrayWrapper> setKeys) {
    ArrayList<Set<ByteArrayWrapper>> nonDestinationSets = fetchSets(setKeys, destination);
    return stripedExecutor
        .execute(destination, () -> doSdiffstore(destination, nonDestinationSets));
  }

  private int doSdiffstore(ByteArrayWrapper destination,
      ArrayList<Set<ByteArrayWrapper>> nonDestinationSets) {
    RedisSet redisSet = checkType(region.get(destination));
    redisSet = new RedisSet(computeDiff(nonDestinationSets, redisSet));
    region.put(destination, redisSet);
    return redisSet.scard();
  }

  private Set<ByteArrayWrapper> computeDiff(ArrayList<Set<ByteArrayWrapper>> nonDestinationSets,
      RedisSet redisSet) {
    Set<ByteArrayWrapper> result = null;
    if (nonDestinationSets.isEmpty()) {
      return emptySet();
    }
    for (Set<ByteArrayWrapper> set : nonDestinationSets) {
      if (set == null) {
        set = redisSet.smembers();
      }
      if (result == null) {
        result = set;
      } else {
        result.removeAll(set);
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
  @SuppressWarnings("unchecked")
  private ArrayList<Set<ByteArrayWrapper>> fetchSets(ArrayList<ByteArrayWrapper> setKeys,
      ByteArrayWrapper destination) {
    ArrayList<Set<ByteArrayWrapper>> result = new ArrayList<>(setKeys.size());
    Region fetchRegion = region;
    if (fetchRegion instanceof LocalDataSet) {
      LocalDataSet lds = (LocalDataSet) fetchRegion;
      fetchRegion = lds.getProxy();
    }
    RedisSetCommands redisSetCommands = new RedisSetCommandsFunctionExecutor(fetchRegion);
    for (ByteArrayWrapper key : setKeys) {
      if (key.equals(destination)) {
        result.add(null);
      } else {
        result.add(redisSetCommands.smembers(key));
      }
    }
    return result;
  }

  @Override
  public int sunionstore(ByteArrayWrapper destination, ArrayList<ByteArrayWrapper> setKeys) {
    throw new IllegalStateException("should never be called");
  }

  @Override
  public int sinterstore(ByteArrayWrapper destination, ArrayList<ByteArrayWrapper> setKeys) {
    throw new IllegalStateException("should never be called");
  }

  @Override
  public int sdiffstore(ByteArrayWrapper destination, ArrayList<ByteArrayWrapper> setKeys) {
    throw new IllegalStateException("should never be called");
  }

  @Override
  public long srem(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToRemove) {
    return getRedisSet(key).srem(membersToRemove, region, key);
  }

  @Override
  public Set<ByteArrayWrapper> smembers(
      ByteArrayWrapper key) {
    return getRedisSet(key).smembers();
  }

  @Override
  public int scard(ByteArrayWrapper key) {
    return getRedisSet(key).scard();
  }

  @Override
  public boolean sismember(
      ByteArrayWrapper key, ByteArrayWrapper member) {
    return getRedisSet(key).sismember(member);
  }

  @Override
  public Collection<ByteArrayWrapper> srandmember(
      ByteArrayWrapper key, int count) {
    return getRedisSet(key).srandmember(count);
  }

  @Override
  public Collection<ByteArrayWrapper> spop(
      ByteArrayWrapper key, int popCount) {
    return getRedisSet(key).spop(region, key, popCount);
  }

  @Override
  public List<Object> sscan(
      ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return getRedisSet(key).sscan(matchPattern, count, cursor);
  }

  private RedisSet getRedisSet(ByteArrayWrapper key) {
    return checkType(getRedisDataOrDefault(key, RedisSet.EMPTY));
  }

  private RedisSet checkType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_SET) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisSet) redisData;
  }

}
