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

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;

public class RedisHashCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor implements
    RedisHashCommands {

  public RedisHashCommandsFunctionExecutor(CommandHelper helper) {
    super(helper);
  }

  private RedisHash getRedisHash(RedisKey key, boolean updateStats) {
    return helper.getRedisHash(key, updateStats);
  }

  @Override
  public int hset(RedisKey key, List<byte[]> fieldsToSet, boolean NX) {
    return stripedExecute(key,
        () -> getRedisHash(key, false)
            .hset(getRegion(), key, fieldsToSet, NX));
  }

  @Override
  public int hdel(RedisKey key, List<byte[]> fieldsToRemove) {
    return stripedExecute(key,
        () -> getRedisHash(key, false)
            .hdel(getRegion(), key, fieldsToRemove));
  }

  @Override
  public Collection<byte[]> hgetall(RedisKey key) {
    return stripedExecute(key, () -> getRedisHash(key, true).hgetall());
  }

  @Override
  public int hexists(RedisKey key, byte[] field) {
    return stripedExecute(key, () -> getRedisHash(key, true).hexists(field));
  }

  @Override
  public byte[] hget(RedisKey key, byte[] field) {
    return stripedExecute(key, () -> getRedisHash(key, true).hget(field));
  }

  @Override
  public int hlen(RedisKey key) {
    return stripedExecute(key, () -> getRedisHash(key, true).hlen());
  }

  @Override
  public int hstrlen(RedisKey key, byte[] field) {
    return stripedExecute(key, () -> getRedisHash(key, true).hstrlen(field));
  }

  @Override
  public List<byte[]> hmget(RedisKey key, List<byte[]> fields) {
    return stripedExecute(key, () -> getRedisHash(key, true).hmget(fields));
  }

  @Override
  public Collection<byte[]> hvals(RedisKey key) {
    return stripedExecute(key, () -> getRedisHash(key, true).hvals());
  }

  @Override
  public Collection<byte[]> hkeys(RedisKey key) {
    return stripedExecute(key, () -> getRedisHash(key, true).hkeys());
  }

  @Override
  public Pair<Integer, List<byte[]>> hscan(RedisKey key, Pattern matchPattern,
      int count, int cursor) {
    return stripedExecute(key,
        () -> getRedisHash(key, true)
            .hscan(matchPattern, count, cursor));
  }

  @Override
  public long hincrby(RedisKey key, byte[] field, long increment) {
    return stripedExecute(key,
        () -> getRedisHash(key, false)
            .hincrby(getRegion(), key, field, increment));
  }

  @Override
  public BigDecimal hincrbyfloat(RedisKey key, byte[] field, BigDecimal increment) {
    return stripedExecute(key,
        () -> getRedisHash(key, false)
            .hincrbyfloat(getRegion(), key, field, increment));
  }

}
