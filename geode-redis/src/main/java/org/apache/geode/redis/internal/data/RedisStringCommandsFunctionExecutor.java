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

  public RedisStringCommandsFunctionExecutor(
      CommandHelper helper) {
    super(helper);
  }

  private RedisString getRedisString(ByteArrayWrapper key, boolean updateStats) {
    return helper.getRedisString(key, updateStats);
  }

  private RedisString getRedisStringIgnoringType(ByteArrayWrapper key, boolean updateStats) {
    return helper.getRedisStringIgnoringType(key, updateStats);
  }

  @Override
  public long append(ByteArrayWrapper key, ByteArrayWrapper valueToAppend) {
    return stripedExecute(key,
        () -> getRedisString(key, false)
            .append(valueToAppend, getRegion(), key));
  }

  @Override
  public ByteArrayWrapper get(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key, true).get());
  }

  @Override
  public ByteArrayWrapper mget(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisStringIgnoringType(key, true).get());
  }

  @Override
  public boolean set(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    return stripedExecute(key, () -> NULL_REDIS_STRING
        .set(helper, key, value, options));
  }

  @Override
  public long incr(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key, false).incr(getRegion(), key));
  }

  @Override
  public long decr(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key, false).decr(getRegion(), key));
  }

  @Override
  public ByteArrayWrapper getset(ByteArrayWrapper key, ByteArrayWrapper value) {
    return stripedExecute(key,
        () -> getRedisString(key, true).getset(getRegion(), key, value));
  }

  @Override
  public long incrby(ByteArrayWrapper key, long increment) {
    return stripedExecute(key,
        () -> getRedisString(key, false).incrby(getRegion(), key, increment));
  }

  @Override
  public BigDecimal incrbyfloat(ByteArrayWrapper key, BigDecimal increment) {
    return stripedExecute(key,
        () -> getRedisString(key, false)
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
        () -> getRedisString(key, false).decrby(getRegion(), key, decrement));
  }

  @Override
  public ByteArrayWrapper getrange(ByteArrayWrapper key, long start, long end) {
    return stripedExecute(key, () -> getRedisString(key, true).getrange(start, end));
  }

  @Override
  public int setrange(ByteArrayWrapper key, int offset, byte[] value) {
    return stripedExecute(key,
        () -> getRedisString(key, false)
            .setrange(getRegion(), key, offset, value));
  }

  @Override
  public int bitpos(ByteArrayWrapper key, int bit, int start, Integer end) {
    return stripedExecute(key,
        () -> getRedisString(key, true)
            .bitpos(getRegion(), key, bit, start, end));
  }

  @Override
  public long bitcount(ByteArrayWrapper key, int start, int end) {
    return stripedExecute(key, () -> getRedisString(key, true).bitcount(start, end));
  }

  @Override
  public long bitcount(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key, true).bitcount());
  }

  @Override
  public int strlen(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisString(key, true).strlen());
  }

  @Override
  public int getbit(ByteArrayWrapper key, int offset) {
    return stripedExecute(key, () -> getRedisString(key, true).getbit(offset));
  }

  @Override
  public int setbit(ByteArrayWrapper key, long offset, int value) {
    int byteIndex = (int) (offset / 8);
    byte bitIndex = (byte) (offset % 8);
    return stripedExecute(key,
        () -> getRedisString(key, false)
            .setbit(getRegion(), key, value, byteIndex, bitIndex));
  }

}
