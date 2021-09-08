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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.executor.set.RedisSetCommands;

public class RedisSetCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor implements
    RedisSetCommands {

  public RedisSetCommandsFunctionExecutor(RegionProvider regionProvider,
      CacheTransactionManager txManager) {
    super(regionProvider, txManager);
  }

  private RedisSet getRedisSet(RedisKey key, boolean updateStats) {
    return getRegionProvider().getTypedRedisData(RedisDataType.REDIS_SET, key, updateStats);
  }

  @Override
  public long sadd(RedisKey key, List<byte[]> membersToAdd) {
    return stripedExecute(key,
        () -> getRedisSet(key, false)
            .sadd(membersToAdd, getRegion(), key));
  }

  @Override
  public int sunionstore(RedisKey destination, List<RedisKey> setKeys) {
    return sunionstore(getRegionProvider(), destination, setKeys);
  }

  @Override
  public int sinterstore(RedisKey destination, List<RedisKey> setKeys) {
    return sinterstore(getRegionProvider(), destination, setKeys);
  }

  @Override
  public int sdiffstore(RedisKey destination, List<RedisKey> setKeys) {
    return sdiffstore(getRegionProvider(), destination, setKeys);
  }

  @Override
  public long srem(RedisKey key, List<byte[]> membersToRemove) {
    return stripedExecute(key, () -> getRedisSet(key, false)
        .srem(membersToRemove, getRegion(), key));
  }

  @Override
  public Set<byte[]> smembers(RedisKey key) {
    return stripedExecute(key, () -> new HashSet<>(getRedisSet(key, true).smembers()));
  }

  @Override
  public Set<byte[]> internalsmembers(RedisKey key) {
    return stripedExecute(key, () -> new HashSet<>(getRedisSet(key, false).smembers()));
  }

  @Override
  public int scard(RedisKey key) {
    return stripedExecute(key, () -> getRedisSet(key, true).scard());
  }

  @Override
  public boolean sismember(RedisKey key, byte[] member) {
    return stripedExecute(key, () -> getRedisSet(key, true).sismember(member));
  }

  @Override
  public Collection<byte[]> srandmember(RedisKey key, int count) {
    return stripedExecute(key, () -> getRedisSet(key, true).srandmember(count));
  }

  @Override
  public Collection<byte[]> spop(RedisKey key, int popCount) {
    return stripedExecute(key, () -> getRedisSet(key, false)
        .spop(getRegion(), key, popCount));
  }

  @Override
  public Pair<BigInteger, List<Object>> sscan(RedisKey key, GlobPattern matchPattern, int count,
      BigInteger cursor) {
    return stripedExecute(key, () -> getRedisSet(key, true).sscan(matchPattern, count, cursor));
  }

  private enum SetOp {
    UNION, INTERSECTION, DIFF
  }

  private int sunionstore(RegionProvider regionProvider, RedisKey destination,
      List<RedisKey> setKeys) {
    return doSetOp(SetOp.UNION, regionProvider, destination, setKeys);
  }

  private int sinterstore(RegionProvider regionProvider, RedisKey destination,
      List<RedisKey> setKeys) {
    return doSetOp(SetOp.INTERSECTION, regionProvider, destination, setKeys);
  }

  private int sdiffstore(RegionProvider regionProvider, RedisKey destination,
      List<RedisKey> setKeys) {
    return doSetOp(SetOp.DIFF, regionProvider, destination, setKeys);
  }

  private int doSetOp(SetOp setOp, RegionProvider regionProvider, RedisKey destination,
      List<RedisKey> setKeys) {
    List<Set<byte[]>> nonDestinationSets = fetchSets(regionProvider, setKeys, destination);
    return regionProvider.execute(destination,
        () -> doSetOpWhileLocked(setOp, regionProvider, destination, nonDestinationSets));
  }

  private int doSetOpWhileLocked(SetOp setOp, RegionProvider regionProvider,
      RedisKey destination,
      List<Set<byte[]>> nonDestinationSets) {
    Set<byte[]> result =
        computeSetOp(setOp, nonDestinationSets, regionProvider, destination);
    if (result.isEmpty()) {
      regionProvider.getDataRegion().remove(destination);
      return 0;
    } else {
      regionProvider.getDataRegion().put(destination, new RedisSet(result));
      return result.size();
    }
  }

  private Set<byte[]> computeSetOp(SetOp setOp, List<Set<byte[]>> nonDestinationSets,
      RegionProvider regionProvider, RedisKey destination) {
    ObjectOpenCustomHashSet<byte[]> result = null;
    if (nonDestinationSets.isEmpty()) {
      return emptySet();
    }
    for (Set<byte[]> set : nonDestinationSets) {
      if (set == null) {
        RedisSet redisSet =
            regionProvider.getTypedRedisData(RedisDataType.REDIS_SET, destination, false);
        set = redisSet.smembers();
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
  private List<Set<byte[]>> fetchSets(RegionProvider regionProvider, List<RedisKey> setKeys,
      RedisKey destination) {
    List<Set<byte[]>> result = new ArrayList<>(setKeys.size());
    for (RedisKey key : setKeys) {
      if (key.equals(destination)) {
        result.add(null);
      } else {
        result.add(regionProvider.getSetCommands().internalsmembers(key));
      }
    }
    return result;
  }
}
