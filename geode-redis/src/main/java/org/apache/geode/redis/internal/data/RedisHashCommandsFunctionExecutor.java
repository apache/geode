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
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;

public class RedisHashCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor implements
    RedisHashCommands {

  public RedisHashCommandsFunctionExecutor(
      CommandHelper helper) {
    super(helper);
  }

  private RedisHash getRedisHash(ByteArrayWrapper key, boolean updateStats) {
    return helper.getRedisHash(key, updateStats);
  }

  @Override
  public int hset(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToSet, boolean NX) {
    return stripedExecute(key,
        () -> getRedisHash(key, false)
            .hset(getRegion(), key, fieldsToSet, NX));
  }

  @Override
  public int hdel(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToRemove) {
    return stripedExecute(key,
        () -> getRedisHash(key, false)
            .hdel(getRegion(), key, fieldsToRemove));
  }

  @Override
  public Collection<ByteArrayWrapper> hgetall(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisHash(key, true).hgetall());
  }

  @Override
  public int hexists(ByteArrayWrapper key, ByteArrayWrapper field) {
    return stripedExecute(key, () -> getRedisHash(key, true).hexists(field));
  }

  @Override
  public ByteArrayWrapper hget(ByteArrayWrapper key, ByteArrayWrapper field) {
    return stripedExecute(key, () -> getRedisHash(key, true).hget(field));
  }

  @Override
  public int hlen(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisHash(key, true).hlen());
  }

  @Override
  public int hstrlen(ByteArrayWrapper key, ByteArrayWrapper field) {
    return stripedExecute(key, () -> getRedisHash(key, true).hstrlen(field));
  }

  @Override
  public List<ByteArrayWrapper> hmget(ByteArrayWrapper key, List<ByteArrayWrapper> fields) {
    return stripedExecute(key, () -> getRedisHash(key, true).hmget(fields));
  }

  @Override
  public Collection<ByteArrayWrapper> hvals(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisHash(key, true).hvals());
  }

  @Override
  public Collection<ByteArrayWrapper> hkeys(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisHash(key, true).hkeys());
  }

  @Override
  public Pair<Integer, List<Object>> hscan(ByteArrayWrapper key, Pattern matchPattern,
      int count, int cursor, UUID clientID) {
    return stripedExecute(key,
        () -> getRedisHash(key, true)
            .hscan(clientID, matchPattern, count, cursor));
  }

  @Override
  public long hincrby(ByteArrayWrapper key, ByteArrayWrapper field, long increment) {
    return stripedExecute(key,
        () -> getRedisHash(key, false)
            .hincrby(getRegion(), key, field, increment));
  }

  @Override
  public BigDecimal hincrbyfloat(ByteArrayWrapper key, ByteArrayWrapper field,
      BigDecimal increment) {
    return stripedExecute(key,
        () -> getRedisHash(key, false)
            .hincrbyfloat(getRegion(), key, field, increment));
  }

}
