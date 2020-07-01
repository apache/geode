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

import static org.apache.geode.redis.internal.data.RedisSet.NULL_REDIS_SET;
import static org.apache.geode.redis.internal.data.RedisString.NULL_REDIS_STRING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;
import org.apache.geode.redis.internal.executor.set.RedisSetCommands;
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
  private final CommandHelper helper;

  public RedisDataCommands(CommandHelper helper) {
    this.helper = helper;
  }

  private Region<ByteArrayWrapper, RedisData> getRegion() {
    return helper.getRegion();
  }

  private <T> T stripedExecute(Object key,
      Callable<T> callable) {
    return helper.getStripedExecutor().execute(key, callable);
  }

  private RedisData getRedisData(ByteArrayWrapper key) {
    return helper.getRedisData(key);
  }

  private RedisHash getRedisHash(ByteArrayWrapper key) {
    return helper.getRedisHash(key);
  }

  private RedisSet getRedisSet(ByteArrayWrapper key) {
    return helper.getRedisSet(key);
  }

  private RedisString getRedisString(ByteArrayWrapper key) {
    return helper.getRedisString(key);
  }

  private RedisString getRedisStringIgnoringType(ByteArrayWrapper key) {
    return helper.getRedisStringIgnoringType(key);
  }


  ///////////////////////////////////////////////////////////////
  /////////////////////// KEY COMMANDS //////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public boolean del(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRegion().remove(key) != null);
  }

  @Override
  public boolean exists(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisData(key).exists());
  }

  @Override
  public long pttl(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisData(key).pttl(getRegion(), key));
  }

  @Override
  public int pexpireat(ByteArrayWrapper key, long timestamp) {
    return stripedExecute(key,
        () -> getRedisData(key).pexpireat(helper, key, timestamp));
  }

  @Override
  public int persist(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisData(key).persist(getRegion(), key));
  }

  @Override
  public String type(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisData(key).type());
  }

  @Override
  public boolean rename(ByteArrayWrapper oldKey, ByteArrayWrapper newKey) {
    // caller has already done all the stripedExecutor locking
    return getRedisData(oldKey).rename(getRegion(), oldKey, newKey);
  }

  ///////////////////////////////////////////////////////////////
  /////////////////// SET COMMANDS /////////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public long sadd(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {
    return stripedExecute(key, () -> getRedisSet(key).sadd(membersToAdd,
        getRegion(), key));
  }

  @Override
  public int sunionstore(ByteArrayWrapper destination,
      ArrayList<ByteArrayWrapper> setKeys) {
    return NULL_REDIS_SET.sunionstore(helper, destination, setKeys);
  }

  @Override
  public int sinterstore(ByteArrayWrapper destination,
      ArrayList<ByteArrayWrapper> setKeys) {
    return NULL_REDIS_SET.sinterstore(helper, destination, setKeys);
  }

  @Override
  public int sdiffstore(ByteArrayWrapper destination,
      ArrayList<ByteArrayWrapper> setKeys) {
    return NULL_REDIS_SET.sdiffstore(helper, destination, setKeys);
  }

  @Override
  public long srem(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToRemove) {
    return stripedExecute(key, () -> getRedisSet(key).srem(membersToRemove,
        getRegion(), key));
  }

  @Override
  public Set<ByteArrayWrapper> smembers(
      ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisSet(key).smembers());
  }

  @Override
  public int scard(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisSet(key).scard());
  }

  @Override
  public boolean sismember(
      ByteArrayWrapper key, ByteArrayWrapper member) {
    return stripedExecute(key, () -> getRedisSet(key).sismember(member));
  }

  @Override
  public Collection<ByteArrayWrapper> srandmember(
      ByteArrayWrapper key, int count) {
    return stripedExecute(key, () -> getRedisSet(key).srandmember(count));
  }

  @Override
  public Collection<ByteArrayWrapper> spop(
      ByteArrayWrapper key, int popCount) {
    return stripedExecute(key, () -> getRedisSet(key)
        .spop(getRegion(), key, popCount));
  }

  @Override
  public List<Object> sscan(
      ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return stripedExecute(key, () -> getRedisSet(key).sscan(matchPattern, count, cursor));
  }

  ///////////////////////////////////////////////////////////////
  /////////////////// HASH COMMANDS /////////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public int hset(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToSet, boolean NX) {
    return stripedExecute(key, () -> getRedisHash(key)
        .hset(getRegion(), key, fieldsToSet, NX));
  }

  @Override
  public int hdel(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToRemove) {
    return stripedExecute(key, () -> getRedisHash(key)
        .hdel(getRegion(), key, fieldsToRemove));
  }

  @Override
  public Collection<ByteArrayWrapper> hgetall(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisHash(key).hgetall());
  }

  @Override
  public int hexists(ByteArrayWrapper key, ByteArrayWrapper field) {
    return stripedExecute(key, () -> getRedisHash(key).hexists(field));
  }

  @Override
  public ByteArrayWrapper hget(ByteArrayWrapper key, ByteArrayWrapper field) {
    return stripedExecute(key, () -> getRedisHash(key).hget(field));
  }

  @Override
  public int hlen(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisHash(key).hlen());
  }

  @Override
  public int hstrlen(ByteArrayWrapper key, ByteArrayWrapper field) {
    return stripedExecute(key, () -> getRedisHash(key).hstrlen(field));
  }

  @Override
  public List<ByteArrayWrapper> hmget(ByteArrayWrapper key, List<ByteArrayWrapper> fields) {
    return stripedExecute(key, () -> getRedisHash(key).hmget(fields));
  }

  @Override
  public Collection<ByteArrayWrapper> hvals(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisHash(key).hvals());
  }

  @Override
  public Collection<ByteArrayWrapper> hkeys(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisHash(key).hkeys());
  }

  @Override
  public List<Object> hscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return stripedExecute(key, () -> getRedisHash(key).hscan(matchPattern, count, cursor));
  }

  @Override
  public long hincrby(ByteArrayWrapper key, ByteArrayWrapper field, long increment) {
    return stripedExecute(key,
        () -> getRedisHash(key)
            .hincrby(getRegion(), key, field, increment));
  }

  @Override
  public double hincrbyfloat(ByteArrayWrapper key, ByteArrayWrapper field, double increment) {
    return stripedExecute(key,
        () -> getRedisHash(key)
            .hincrbyfloat(getRegion(), key, field, increment));
  }

  ///////////////////////////////////////////////////////////////
  ////////////////// STRING COMMANDS ////////////////////////////
  ///////////////////////////////////////////////////////////////

  @Override
  public long append(ByteArrayWrapper key, ByteArrayWrapper valueToAppend) {
    return stripedExecute(key,
        () -> getRedisString(key).append(valueToAppend, getRegion(), key));
  }

  @Override
  public ByteArrayWrapper get(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key).get());
  }

  @Override
  public ByteArrayWrapper mget(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisStringIgnoringType(key).get());
  }

  @Override
  public boolean set(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    return stripedExecute(key, () -> NULL_REDIS_STRING
        .set(helper, key, value, options));
  }

  @Override
  public long incr(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key).incr(getRegion(), key));
  }

  @Override
  public long decr(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key).decr(getRegion(), key));
  }

  @Override
  public ByteArrayWrapper getset(ByteArrayWrapper key, ByteArrayWrapper value) {
    return stripedExecute(key,
        () -> getRedisString(key).getset(getRegion(), key, value));
  }

  @Override
  public long incrby(ByteArrayWrapper key, long increment) {
    return stripedExecute(key,
        () -> getRedisString(key).incrby(getRegion(), key, increment));
  }

  @Override
  public double incrbyfloat(ByteArrayWrapper key, double increment) {
    return stripedExecute(key,
        () -> getRedisString(key)
            .incrbyfloat(getRegion(), key, increment));
  }

  @Override
  public int bitop(String operation, ByteArrayWrapper key,
      List<ByteArrayWrapper> sources) {
    return NULL_REDIS_STRING.bitop(helper, operation, key, sources);
  }

  @Override
  public long decrby(ByteArrayWrapper key, long decrement) {
    return stripedExecute(key,
        () -> getRedisString(key).decrby(getRegion(), key, decrement));
  }

  @Override
  public ByteArrayWrapper getrange(ByteArrayWrapper key, long start, long end) {
    return stripedExecute(key, () -> getRedisString(key).getrange(start, end));
  }

  @Override
  public int setrange(ByteArrayWrapper key, int offset, byte[] value) {
    return stripedExecute(key,
        () -> getRedisString(key)
            .setrange(getRegion(), key, offset, value));
  }

  @Override
  public int bitpos(ByteArrayWrapper key, int bit, int start, Integer end) {
    return stripedExecute(key,
        () -> getRedisString(key)
            .bitpos(getRegion(), key, bit, start, end));
  }

  @Override
  public long bitcount(ByteArrayWrapper key, int start, int end) {
    return stripedExecute(key, () -> getRedisString(key).bitcount(start, end));
  }

  @Override
  public long bitcount(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key).bitcount());
  }

  @Override
  public int strlen(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key).strlen());
  }

  @Override
  public int getbit(ByteArrayWrapper key, int offset) {
    return stripedExecute(key, () -> getRedisString(key).getbit(offset));
  }

  @Override
  public int setbit(ByteArrayWrapper key, long offset, int value) {
    int byteIndex = (int) (offset / 8);
    byte bitIndex = (byte) (offset % 8);
    return stripedExecute(key,
        () -> getRedisString(key)
            .setbit(getRegion(), key, value, byteIndex, bitIndex));
  }

}
