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
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_HASH;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_STRING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;
import org.apache.geode.redis.internal.executor.set.RedisSetCommands;
import org.apache.geode.redis.internal.executor.set.RedisSetCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.string.RedisStringCommands;
import org.apache.geode.redis.internal.executor.string.SetOptions;

/**
 * Provides a method for every commands that can be done
 * on {@link RedisData} instances.
 * This class provides any other resources needed to execute
 * a command on RedisData, for example the region the data
 * is stored in and the stats that need to be updated.
 * It does not keep any state changed by a command so a
 * single instance of it can be used concurrently by
 * multiple commands and a canonical instance can be used
 * to prevent garbage creation.
 */
public class RedisDataCommands implements RedisKeyCommands, RedisSetCommands, RedisHashCommands,
    RedisStringCommands {
  private final Region<ByteArrayWrapper, RedisData> region;
  private final RedisStats redisStats;
  private final StripedExecutor stripedExecutor;


  public RedisDataCommands(Region<ByteArrayWrapper, RedisData> region, RedisStats redisStats,
      StripedExecutor stripedExecutor) {
    this.region = region;
    this.redisStats = redisStats;
    this.stripedExecutor = stripedExecutor;
  }

  public Region<ByteArrayWrapper, RedisData> getRegion() {
    return region;
  }

  public RedisStats getRedisStats() {
    return redisStats;
  }

  public StripedExecutor getStripedExecutor() {
    return stripedExecutor;
  }

  ///////////////////////////////////////////////////////////////
  /////////////////////// KEY COMMANDS //////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public boolean del(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> region.remove(key) != null);
  }

  @Override
  public boolean exists(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisData(key) != null);
  }

  @Override
  public long pttl(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> dopttl(key));
  }

  private long dopttl(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return -2;
    }
    return redisData.pttl(region, key);
  }

  @Override
  public int pexpireat(ByteArrayWrapper key, long timestamp) {
    return stripedExecutor.execute(key, () -> dopexpireat(key, timestamp));
  }

  private int dopexpireat(ByteArrayWrapper key, long timestamp) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return 0;
    }
    long now = System.currentTimeMillis();
    if (now >= timestamp) {
      // already expired
      doExpiration(key);
    } else {
      redisData.setExpirationTimestamp(region, key, timestamp);
    }
    return 1;
  }

  @Override
  public int persist(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> dopersist(key));
  }

  private int dopersist(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return 0;
    }
    return redisData.persist(region, key);
  }

  @Override
  public String type(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> dotype(key));
  }

  private String dotype(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return "none";
    }
    return redisData.getType().toString();
  }

  @Override
  public boolean rename(ByteArrayWrapper oldKey, ByteArrayWrapper newKey) {
    // caller has already done all the stripedExecutor locking
    RedisData value = getRedisData(oldKey);
    if (value == null) {
      return false;
    }

    region.put(newKey, value);
    region.remove(oldKey);

    return true;
  }

  RedisData getRedisData(ByteArrayWrapper key) {
    return getRedisDataOrDefault(key, null);
  }

  private RedisData getRedisDataOrDefault(ByteArrayWrapper key, RedisData defaultValue) {
    RedisData result = region.get(key);
    if (result != null) {
      if (result.hasExpired()) {
        doExpiration(key);
        result = null;
      }
    }
    if (result == null) {
      return defaultValue;
    } else {
      return result;
    }
  }

  private void doExpiration(ByteArrayWrapper key) {
    long start = redisStats.startExpiration();
    region.remove(key);
    redisStats.endExpiration(start);
  }

  ///////////////////////////////////////////////////////////////
  /////////////////// SET COMMANDS /////////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public long sadd(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {
    return stripedExecutor.execute(key, () -> getRedisSet(key).sadd(membersToAdd, region, key));
  }

  @Override
  public int sunionstore(ByteArrayWrapper destination,
      ArrayList<ByteArrayWrapper> setKeys) {
    ArrayList<Set<ByteArrayWrapper>> nonDestinationSets = fetchSets(setKeys, destination);
    return stripedExecutor
        .execute(destination, () -> doSunionstore(destination, nonDestinationSets));
  }

  private int doSunionstore(ByteArrayWrapper destination,
      ArrayList<Set<ByteArrayWrapper>> nonDestinationSets) {
    RedisSet redisSet = checkSetType(region.get(destination));
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

  @Override
  public int sinterstore(ByteArrayWrapper destination,
      ArrayList<ByteArrayWrapper> setKeys) {
    ArrayList<Set<ByteArrayWrapper>> nonDestinationSets = fetchSets(setKeys, destination);
    return stripedExecutor
        .execute(destination, () -> doSinterstore(destination, nonDestinationSets));
  }

  private int doSinterstore(ByteArrayWrapper destination,
      ArrayList<Set<ByteArrayWrapper>> nonDestinationSets) {
    RedisSet redisSet = checkSetType(region.get(destination));
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

  @Override
  public int sdiffstore(ByteArrayWrapper destination,
      ArrayList<ByteArrayWrapper> setKeys) {
    ArrayList<Set<ByteArrayWrapper>> nonDestinationSets = fetchSets(setKeys, destination);
    return stripedExecutor
        .execute(destination, () -> doSdiffstore(destination, nonDestinationSets));
  }

  private int doSdiffstore(ByteArrayWrapper destination,
      ArrayList<Set<ByteArrayWrapper>> nonDestinationSets) {
    RedisSet redisSet = checkSetType(region.get(destination));
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
  private ArrayList<Set<ByteArrayWrapper>> fetchSets(ArrayList<ByteArrayWrapper> setKeys,
      ByteArrayWrapper destination) {
    ArrayList<Set<ByteArrayWrapper>> result = new ArrayList<>(setKeys.size());
    RedisSetCommands redisSetCommands = new RedisSetCommandsFunctionExecutor(region);
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
  public long srem(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToRemove) {
    return stripedExecutor.execute(key, () -> getRedisSet(key).srem(membersToRemove, region, key));
  }

  @Override
  public Set<ByteArrayWrapper> smembers(
      ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisSet(key).smembers());
  }

  @Override
  public int scard(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisSet(key).scard());
  }

  @Override
  public boolean sismember(
      ByteArrayWrapper key, ByteArrayWrapper member) {
    return stripedExecutor.execute(key, () -> getRedisSet(key).sismember(member));
  }

  @Override
  public Collection<ByteArrayWrapper> srandmember(
      ByteArrayWrapper key, int count) {
    return stripedExecutor.execute(key, () -> getRedisSet(key).srandmember(count));
  }

  @Override
  public Collection<ByteArrayWrapper> spop(
      ByteArrayWrapper key, int popCount) {
    return stripedExecutor.execute(key, () -> getRedisSet(key).spop(region, key, popCount));
  }

  @Override
  public List<Object> sscan(
      ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return stripedExecutor.execute(key, () -> getRedisSet(key).sscan(matchPattern, count, cursor));
  }

  private RedisSet getRedisSet(ByteArrayWrapper key) {
    return checkSetType(getRedisDataOrDefault(key, RedisSet.EMPTY));
  }

  private RedisSet checkSetType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_SET) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisSet) redisData;
  }

  ///////////////////////////////////////////////////////////////
  /////////////////// HASH COMMANDS /////////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public int hset(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToSet, boolean NX) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hset(region, key, fieldsToSet, NX));
  }

  @Override
  public int hdel(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToRemove) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hdel(region, key, fieldsToRemove));
  }

  @Override
  public Collection<ByteArrayWrapper> hgetall(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hgetall());
  }

  @Override
  public int hexists(ByteArrayWrapper key, ByteArrayWrapper field) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hexists(field));
  }

  @Override
  public ByteArrayWrapper hget(ByteArrayWrapper key, ByteArrayWrapper field) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hget(field));
  }

  @Override
  public int hlen(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hlen());
  }

  @Override
  public int hstrlen(ByteArrayWrapper key, ByteArrayWrapper field) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hstrlen(field));
  }

  @Override
  public List<ByteArrayWrapper> hmget(ByteArrayWrapper key, List<ByteArrayWrapper> fields) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hmget(fields));
  }

  @Override
  public Collection<ByteArrayWrapper> hvals(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hvals());
  }

  @Override
  public Collection<ByteArrayWrapper> hkeys(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hkeys());
  }

  @Override
  public List<Object> hscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return stripedExecutor.execute(key, () -> getRedisHash(key).hscan(matchPattern, count, cursor));
  }

  @Override
  public long hincrby(ByteArrayWrapper key, ByteArrayWrapper field, long increment) {
    return stripedExecutor.execute(key,
        () -> getRedisHash(key).hincrby(region, key, field, increment));
  }

  @Override
  public double hincrbyfloat(ByteArrayWrapper key, ByteArrayWrapper field, double increment) {
    return stripedExecutor.execute(key,
        () -> getRedisHash(key).hincrbyfloat(region, key, field, increment));
  }

  private RedisHash getRedisHash(ByteArrayWrapper key) {
    return checkHashType(getRedisDataOrDefault(key, RedisHash.EMPTY));
  }

  private static RedisHash checkHashType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_HASH) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisHash) redisData;
  }

  ///////////////////////////////////////////////////////////////
  ////////////////// STRING COMMANDS ////////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public long append(ByteArrayWrapper key, ByteArrayWrapper valueToAppend) {
    return stripedExecutor.execute(key,
        () -> getRedisStringOrDefault(key).append(valueToAppend, region, key));
  }

  @Override
  public ByteArrayWrapper get(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisStringOrDefault(key).get());
  }

  @Override
  public ByteArrayWrapper mget(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisStringOrForceDefault(key).get());
  }

  @Override
  public boolean set(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    return stripedExecutor.execute(key, () -> RedisString.NULL.set(this, key, value, options));
  }

  @Override
  public long incr(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisStringOrDefault(key).incr(region, key));
  }

  @Override
  public long decr(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisStringOrDefault(key).decr(region, key));
  }

  @Override
  public ByteArrayWrapper getset(ByteArrayWrapper key, ByteArrayWrapper value) {
    return stripedExecutor.execute(key,
        () -> getRedisStringOrDefault(key).getset(region, key, value));
  }

  @Override
  public long incrby(ByteArrayWrapper key, long increment) {
    return stripedExecutor.execute(key,
        () -> getRedisStringOrDefault(key).incrby(region, key, increment));
  }

  @Override
  public double incrbyfloat(ByteArrayWrapper key, double increment) {
    return stripedExecutor.execute(key,
        () -> getRedisStringOrDefault(key).incrbyfloat(region, key, increment));
  }

  @Override
  public int bitop(String operation, ByteArrayWrapper key,
      List<ByteArrayWrapper> sources) {
    return RedisString.NULL.bitop(this, operation, key, sources);
  }

  @Override
  public long decrby(ByteArrayWrapper key, long decrement) {
    return stripedExecutor.execute(key,
        () -> getRedisStringOrDefault(key).decrby(region, key, decrement));
  }

  @Override
  public ByteArrayWrapper getrange(ByteArrayWrapper key, long start, long end) {
    return stripedExecutor.execute(key, () -> getRedisStringOrDefault(key).getrange(start, end));
  }

  @Override
  public int setrange(ByteArrayWrapper key, int offset, byte[] value) {
    return stripedExecutor.execute(key,
        () -> getRedisStringOrDefault(key).setrange(region, key, offset, value));
  }

  @Override
  public int bitpos(ByteArrayWrapper key, int bit, int start, Integer end) {
    return stripedExecutor.execute(key,
        () -> getRedisStringOrDefault(key).bitpos(region, key, bit, start, end));
  }

  @Override
  public long bitcount(ByteArrayWrapper key, int start, int end) {
    return stripedExecutor.execute(key, () -> getRedisStringOrDefault(key).bitcount(start, end));
  }

  @Override
  public long bitcount(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisStringOrDefault(key).bitcount());
  }

  @Override
  public int strlen(ByteArrayWrapper key) {
    return stripedExecutor.execute(key, () -> getRedisStringOrDefault(key).strlen());
  }

  @Override
  public int getbit(ByteArrayWrapper key, int offset) {
    return stripedExecutor.execute(key, () -> getRedisStringOrDefault(key).getbit(offset));
  }

  @Override
  public int setbit(ByteArrayWrapper key, long offset, int value) {
    int byteIndex = (int) (offset / 8);
    byte bitIndex = (byte) (offset % 8);
    return stripedExecutor.execute(key,
        () -> getRedisStringOrDefault(key).setbit(region, key, value, byteIndex, bitIndex));
  }

  private RedisString checkStringType(RedisData redisData, boolean force) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_STRING) {
      if (force) {
        return RedisString.NULL;
      }
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisString) redisData;
  }

  RedisString getRedisString(ByteArrayWrapper key) {
    return checkStringType(getRedisData(key), false);
  }

  private RedisString getRedisStringOrDefault(ByteArrayWrapper key) {
    return checkStringType(getRedisDataOrDefault(key, RedisString.NULL), false);
  }

  private RedisString getRedisStringOrForceDefault(ByteArrayWrapper key) {
    return checkStringType(getRedisDataOrDefault(key, RedisString.NULL), true);
  }

  RedisString getRedisStringForSet(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null || redisData.getType() != REDIS_STRING) {
      return null;
    }
    return (RedisString) redisData;
  }
}
