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

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;

public class RedisHashCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor implements
    RedisHashCommands {

  public RedisHashCommandsFunctionExecutor(
      CommandHelper helper) {
    super(helper);
  }

  private RedisHash getRedisHash(ByteArrayWrapper key) {
    return helper.getRedisHash(key);
  }

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
  public Pair<BigInteger, List<Object>> hscan(ByteArrayWrapper key, Pattern matchPattern, int count,
      BigInteger cursor) {
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

}
