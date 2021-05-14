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

package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_STRING;

import java.math.BigDecimal;
import java.util.List;

import org.apache.geode.redis.internal.executor.string.RedisStringCommands;
import org.apache.geode.redis.internal.executor.string.SetOptions;

public class RedisStringCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor
    implements
    RedisStringCommands {

  public RedisStringCommandsFunctionExecutor(CommandHelper helper) {
    super(helper);
  }

  private RedisString getRedisString(RedisKey key, boolean updateStats) {
    return helper.getRedisString(key, updateStats);
  }

  private RedisString getRedisStringIgnoringType(RedisKey key, boolean updateStats) {
    return helper.getRedisStringIgnoringType(key, updateStats);
  }

  @Override
  public long append(RedisKey key, byte[] valueToAppend) {
    return stripedExecute(key,
        () -> getRedisString(key, false).append(getRegion(), key, valueToAppend));
  }

  @Override
  public byte[] get(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, true).get());
  }

  @Override
  public byte[] mget(RedisKey key) {
    return stripedExecute(key, () -> getRedisStringIgnoringType(key, true).get());
  }

  @Override
  public boolean set(RedisKey key, byte[] value, SetOptions options) {
    return stripedExecute(key, () -> NULL_REDIS_STRING.set(helper, key, value, options));
  }

  @Override
  public long incr(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, false).incr(getRegion(), key));
  }

  @Override
  public long decr(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, false).decr(getRegion(), key));
  }

  @Override
  public byte[] getset(RedisKey key, byte[] value) {
    return stripedExecute(key, () -> getRedisString(key, true).getset(getRegion(), key, value));
  }

  @Override
  public long incrby(RedisKey key, long increment) {
    return stripedExecute(key,
        () -> getRedisString(key, false).incrby(getRegion(), key, increment));
  }

  @Override
  public BigDecimal incrbyfloat(RedisKey key, BigDecimal increment) {
    return stripedExecute(key,
        () -> getRedisString(key, false).incrbyfloat(getRegion(), key, increment));
  }

  @Override
  public int bitop(String operation, RedisKey key, List<RedisKey> sources) {
    return NULL_REDIS_STRING.bitop(helper, operation, key, sources);
  }

  @Override
  public long decrby(RedisKey key, long decrement) {
    return stripedExecute(key,
        () -> getRedisString(key, false).decrby(getRegion(), key, decrement));
  }

  @Override
  public byte[] getrange(RedisKey key, long start, long end) {
    return stripedExecute(key, () -> getRedisString(key, true).getrange(start, end));
  }

  @Override
  public int setrange(RedisKey key, int offset, byte[] value) {
    return stripedExecute(key,
        () -> getRedisString(key, false).setrange(getRegion(), key, offset, value));
  }

  @Override
  public int bitpos(RedisKey key, int bit, int start, Integer end) {
    return stripedExecute(key, () -> getRedisString(key, true).bitpos(bit, start, end));
  }

  @Override
  public long bitcount(RedisKey key, int start, int end) {
    return stripedExecute(key, () -> getRedisString(key, true).bitcount(start, end));
  }

  @Override
  public long bitcount(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, true).bitcount());
  }

  @Override
  public int strlen(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, true).strlen());
  }

  @Override
  public int getbit(RedisKey key, int offset) {
    return stripedExecute(key, () -> getRedisString(key, true).getbit(offset));
  }

  @Override
  public int setbit(RedisKey key, long offset, int value) {
    int byteIndex = (int) (offset / 8);
    byte bitIndex = (byte) (offset % 8);
    return stripedExecute(key,
        () -> getRedisString(key, false).setbit(getRegion(), key, value, byteIndex, bitIndex));
  }

}
