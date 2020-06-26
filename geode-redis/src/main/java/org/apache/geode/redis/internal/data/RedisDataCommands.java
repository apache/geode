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
import java.util.Arrays;
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
import org.apache.geode.redis.internal.executor.string.RedisStringCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.string.SetOptions;
import org.apache.geode.redis.internal.netty.Coder;

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

  public StripedExecutor getStripedExecutor() {
    return stripedExecutor;
  }

  ///////////////////////////////////////////////////////////////
  /////////////////////// KEY COMMANDS //////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public boolean del(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return false;
    }
    return region.remove(key) != null;
  }

  @Override
  public boolean exists(ByteArrayWrapper key) {
    return getRedisData(key) != null;
  }

  @Override
  public long pttl(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return -2;
    }
    return redisData.pttl(region, key);
  }

  @Override
  public int pexpireat(ByteArrayWrapper key, long timestamp) {
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
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return 0;
    }
    return redisData.persist(region, key);
  }

  @Override
  public String type(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null) {
      return "none";
    }
    return redisData.getType().toString();
  }

  private RedisData getRedisData(ByteArrayWrapper key) {
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

  @Override
  public boolean rename(ByteArrayWrapper oldKey, ByteArrayWrapper newKey) {
    RedisData value = getRedisData(oldKey);
    if (value == null) {
      return false;
    }

    region.put(newKey, value);
    region.remove(oldKey);

    return true;
  }

  ///////////////////////////////////////////////////////////////
  /////////////////// SET COMMANDS /////////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public long sadd(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {

    RedisSet redisSet = checkSetType(getRedisData(key));

    if (redisSet != null) {
      return redisSet.sadd(membersToAdd, region, key);
    } else {
      region.create(key, new RedisSet(membersToAdd));
      return membersToAdd.size();
    }
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
    RedisHash hash = checkHashType(getRedisData(key));
    if (hash != null) {
      return hash.hset(region, key, fieldsToSet, NX);
    } else {
      region.put(key, new RedisHash(fieldsToSet));
      return fieldsToSet.size() / 2;
    }
  }

  @Override
  public int hdel(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToRemove) {
    return getRedisHash(key).hdel(region, key, fieldsToRemove);
  }

  @Override
  public Collection<ByteArrayWrapper> hgetall(ByteArrayWrapper key) {
    return getRedisHash(key).hgetall();
  }

  @Override
  public int hexists(ByteArrayWrapper key, ByteArrayWrapper field) {
    return getRedisHash(key).hexists(field);
  }

  @Override
  public ByteArrayWrapper hget(ByteArrayWrapper key, ByteArrayWrapper field) {
    return getRedisHash(key).hget(field);
  }

  @Override
  public int hlen(ByteArrayWrapper key) {
    return getRedisHash(key).hlen();
  }

  @Override
  public int hstrlen(ByteArrayWrapper key, ByteArrayWrapper field) {
    return getRedisHash(key).hstrlen(field);
  }

  @Override
  public List<ByteArrayWrapper> hmget(ByteArrayWrapper key, List<ByteArrayWrapper> fields) {
    return getRedisHash(key).hmget(fields);
  }

  @Override
  public Collection<ByteArrayWrapper> hvals(ByteArrayWrapper key) {
    return getRedisHash(key).hvals();
  }

  @Override
  public Collection<ByteArrayWrapper> hkeys(ByteArrayWrapper key) {
    return getRedisHash(key).hkeys();
  }

  @Override
  public List<Object> hscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return getRedisHash(key).hscan(matchPattern, count, cursor);
  }

  @Override
  public long hincrby(ByteArrayWrapper key, ByteArrayWrapper field, long increment) {
    RedisHash hash = checkHashType(getRedisData(key));
    if (hash != null) {
      return hash.hincrby(region, key, field, increment);
    } else {
      region.put(key,
          new RedisHash(Arrays.asList(field, new ByteArrayWrapper(Coder.longToBytes(increment)))));
      return increment;
    }
  }

  @Override
  public double hincrbyfloat(ByteArrayWrapper key, ByteArrayWrapper field, double increment) {
    RedisHash hash = checkHashType(getRedisData(key));
    if (hash != null) {
      return hash.hincrbyfloat(region, key, field, increment);
    } else {
      region.put(key,
          new RedisHash(
              Arrays.asList(field, new ByteArrayWrapper(Coder.doubleToBytes(increment)))));
      return increment;
    }
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
    RedisString redisString = getRedisString(key);

    if (redisString != null) {
      return redisString.append(valueToAppend, region, key);
    } else {
      region.put(key, new RedisString(valueToAppend));
      return valueToAppend.length();
    }
  }

  @Override
  public ByteArrayWrapper get(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString != null) {
      return redisString.get();
    } else {
      return null;
    }
  }

  @Override
  public ByteArrayWrapper mget(ByteArrayWrapper key) {
    // like get but does not do a type check
    RedisData redisData = getRedisData(key);
    if (redisData instanceof RedisString) {
      RedisString redisString = (RedisString) redisData;
      return redisString.get();
    }
    return null;
  }

  @Override
  public boolean set(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    if (options != null) {
      if (options.isNX()) {
        return setnx(key, value, options);
      }

      if (options.isXX() && getRedisData(key) == null) {
        return false;
      }
    }

    RedisString redisString = getRedisStringForSet(key);
    if (redisString == null) {
      redisString = new RedisString(value);
    } else {
      redisString.set(value);
    }
    handleSetExpiration(redisString, options);
    region.put(key, redisString);
    return true;
  }

  @Override
  public long incr(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = {Coder.NUMBER_1_BYTE};
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return 1;
    }

    return redisString.incr(region, key);
  }

  @Override
  public long decr(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      redisString = new RedisString(new ByteArrayWrapper(Coder.stringToBytes("-1")));
      region.put(key, redisString);
      return -1;
    }

    return redisString.decr(region, key);
  }

  @Override
  public ByteArrayWrapper getset(ByteArrayWrapper key, ByteArrayWrapper value) {
    ByteArrayWrapper result = null;
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      region.put(key, new RedisString(value));
    } else {
      result = redisString.get();
      redisString.set(value);
      redisString.persistNoDelta();
      region.put(key, redisString);
    }
    return result;
  }

  @Override
  public long incrby(ByteArrayWrapper key, long increment) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = Coder.longToBytes(increment);
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return increment;
    }

    return redisString.incrby(region, key, increment);
  }

  @Override
  public double incrbyfloat(ByteArrayWrapper key, double increment) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = Coder.doubleToBytes(increment);
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return increment;
    }

    return redisString.incrbyfloat(region, key, increment);
  }

  @Override
  public int bitop(String operation, ByteArrayWrapper key,
      List<ByteArrayWrapper> sources) {
    List<ByteArrayWrapper> sourceValues = new ArrayList<>();
    int selfIndex = -1;
    // Read all the source values, except for self, before locking the stripe.
    RedisStringCommands commander = new RedisStringCommandsFunctionExecutor(region);
    for (ByteArrayWrapper sourceKey : sources) {
      if (sourceKey.equals(key)) {
        // get self later after the stripe is locked
        selfIndex = sourceValues.size();
        sourceValues.add(null);
      } else {
        sourceValues.add(commander.get(sourceKey));
      }
    }
    int indexOfSelf = selfIndex;
    return stripedExecutor.execute(key, () -> doBitOp(operation, key, indexOfSelf, sourceValues));
  }

  private int doBitOp(String operation, ByteArrayWrapper key, int selfIndex,
      List<ByteArrayWrapper> sourceValues) {
    if (selfIndex != -1) {
      RedisString redisString = getRedisString(key);
      if (redisString != null) {
        sourceValues.set(selfIndex, redisString.getValue());
      }
    }
    int maxLength = 0;
    for (ByteArrayWrapper sourceValue : sourceValues) {
      if (sourceValue != null && maxLength < sourceValue.length()) {
        maxLength = sourceValue.length();
      }
    }
    ByteArrayWrapper newValue;
    switch (operation) {
      case "AND":
        newValue = and(sourceValues, maxLength);
        break;
      case "OR":
        newValue = or(sourceValues, maxLength);
        break;
      case "XOR":
        newValue = xor(sourceValues, maxLength);
        break;
      default: // NOT
        newValue = not(sourceValues.get(0), maxLength);
        break;
    }
    if (newValue.length() == 0) {
      region.remove(key);
    } else {
      RedisString redisString = getRedisStringForSet(key);
      if (redisString == null) {
        redisString = new RedisString(newValue);
      } else {
        redisString.set(newValue);
      }
      region.put(key, redisString);
    }
    return newValue.length();
  }

  private ByteArrayWrapper and(List<ByteArrayWrapper> sourceValues, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = 0;
      boolean firstByte = true;
      for (ByteArrayWrapper sourceValue : sourceValues) {
        byte sourceByte = 0;
        if (sourceValue != null && i < sourceValue.length()) {
          sourceByte = sourceValue.toBytes()[i];
        }
        if (firstByte) {
          b = sourceByte;
          firstByte = false;
        } else {
          b &= sourceByte;
        }
      }
      dest[i] = b;
    }
    return new ByteArrayWrapper(dest);
  }

  private ByteArrayWrapper or(List<ByteArrayWrapper> sourceValues, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = 0;
      boolean firstByte = true;
      for (ByteArrayWrapper sourceValue : sourceValues) {
        byte sourceByte = 0;
        if (sourceValue != null && i < sourceValue.length()) {
          sourceByte = sourceValue.toBytes()[i];
        }
        if (firstByte) {
          b = sourceByte;
          firstByte = false;
        } else {
          b |= sourceByte;
        }
      }
      dest[i] = b;
    }
    return new ByteArrayWrapper(dest);
  }

  private ByteArrayWrapper xor(List<ByteArrayWrapper> sourceValues, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = 0;
      boolean firstByte = true;
      for (ByteArrayWrapper sourceValue : sourceValues) {
        byte sourceByte = 0;
        if (sourceValue != null && i < sourceValue.length()) {
          sourceByte = sourceValue.toBytes()[i];
        }
        if (firstByte) {
          b = sourceByte;
          firstByte = false;
        } else {
          b ^= sourceByte;
        }
      }
      dest[i] = b;
    }
    return new ByteArrayWrapper(dest);
  }

  private ByteArrayWrapper not(ByteArrayWrapper sourceValue, int max) {
    byte[] dest = new byte[max];
    if (sourceValue == null) {
      for (int i = 0; i < max; i++) {
        dest[i] = ~0;
      }
    } else {
      byte[] cA = sourceValue.toBytes();
      for (int i = 0; i < max; i++) {
        dest[i] = (byte) (~cA[i] & 0xFF);
      }
    }
    return new ByteArrayWrapper(dest);
  }

  @Override
  public long decrby(ByteArrayWrapper key, long decrement) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = Coder.longToBytes(-decrement);
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return -decrement;
    }

    return redisString.decrby(region, key, decrement);
  }

  @Override
  public ByteArrayWrapper getrange(ByteArrayWrapper key, long start, long end) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return new ByteArrayWrapper(new byte[0]);
    }
    return redisString.getrange(start, end);
  }

  @Override
  public int setrange(ByteArrayWrapper key, int offset, byte[] value) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newBytes = value;
      if (value.length != 0) {
        if (offset != 0) {
          newBytes = new byte[offset + value.length];
          System.arraycopy(value, 0, newBytes, offset, value.length);
        }
        redisString = new RedisString(new ByteArrayWrapper(newBytes));
        region.put(key, redisString);
      }
      return newBytes.length;
    }
    return redisString.setrange(region, key, offset, value);
  }

  @Override
  public int bitpos(ByteArrayWrapper key, int bit, int start, Integer end) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      if (bit == 0) {
        return 0;
      } else {
        return -1;
      }
    }
    return redisString.bitpos(region, key, bit, start, end);
  }

  @Override
  public long bitcount(ByteArrayWrapper key, int start, int end) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return 0;
    }
    return redisString.bitcount(start, end);
  }

  @Override
  public long bitcount(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return 0;
    }
    return redisString.bitcount();
  }

  @Override
  public int strlen(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return 0;
    }
    return redisString.strlen();
  }

  @Override
  public int getbit(ByteArrayWrapper key, int offset) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return 0;
    }
    return redisString.getbit(offset);
  }

  @Override
  public int setbit(ByteArrayWrapper key, long offset, int value) {
    RedisString redisString = getRedisString(key);
    int byteIndex = (int) (offset / 8);
    byte bitIndex = (byte) (offset % 8);

    if (redisString == null) {
      RedisString newValue;
      if (value == 1) {
        byte[] bytes = new byte[byteIndex + 1];
        bytes[byteIndex] = (byte) (0x80 >> bitIndex);
        newValue = new RedisString(new ByteArrayWrapper(bytes));
      } else {
        // all bits are 0 so use an empty byte array
        newValue = new RedisString(new ByteArrayWrapper(new byte[0]));
      }
      region.put(key, newValue);
      return 0;
    }
    return redisString.setbit(region, key, value, byteIndex, bitIndex);
  }

  private boolean setnx(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    if (getRedisData(key) != null) {
      return false;
    }
    RedisString redisString = new RedisString(value);
    handleSetExpiration(redisString, options);
    region.put(key, redisString);
    return true;
  }

  private void handleSetExpiration(RedisString redisString, SetOptions options) {
    long setExpiration = options == null ? 0L : options.getExpiration();
    if (setExpiration != 0) {
      long now = System.currentTimeMillis();
      long timestamp = now + setExpiration;
      redisString.setExpirationTimestampNoDelta(timestamp);
    } else if (options == null || !options.isKeepTTL()) {
      redisString.persistNoDelta();
    }
  }

  private RedisString checkStringType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_STRING) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisString) redisData;
  }

  private RedisString getRedisString(ByteArrayWrapper key) {
    return checkStringType(getRedisData(key));
  }

  private RedisString getRedisStringForSet(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null || redisData.getType() != REDIS_STRING) {
      return null;
    }
    return (RedisString) redisData;
  }
}
