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

package org.apache.geode.redis.internal.executor.hash;

import static org.apache.geode.redis.internal.RedisCommandType.HDEL;
import static org.apache.geode.redis.internal.RedisCommandType.HEXISTS;
import static org.apache.geode.redis.internal.RedisCommandType.HGET;
import static org.apache.geode.redis.internal.RedisCommandType.HGETALL;
import static org.apache.geode.redis.internal.RedisCommandType.HINCRBY;
import static org.apache.geode.redis.internal.RedisCommandType.HINCRBYFLOAT;
import static org.apache.geode.redis.internal.RedisCommandType.HKEYS;
import static org.apache.geode.redis.internal.RedisCommandType.HLEN;
import static org.apache.geode.redis.internal.RedisCommandType.HMGET;
import static org.apache.geode.redis.internal.RedisCommandType.HSCAN;
import static org.apache.geode.redis.internal.RedisCommandType.HSET;
import static org.apache.geode.redis.internal.RedisCommandType.HSTRLEN;
import static org.apache.geode.redis.internal.RedisCommandType.HVALS;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisCommandsFunctionInvoker;

/**
 * This class is used by netty redis has command executors
 * to invoke a geode function that will run on a
 * particular server to do the redis command.
 */
public class RedisHashCommandsFunctionInvoker extends RedisCommandsFunctionInvoker
    implements RedisHashCommands {

  public RedisHashCommandsFunctionInvoker(Region<RedisKey, RedisData> region) {
    super(region);
  }

  @Override
  public int hset(RedisKey key, List<byte[]> fieldsToSet, boolean NX) {
    return invokeCommandFunction(key, HSET, fieldsToSet, NX);
  }

  @Override
  public int hdel(RedisKey key, List<byte[]> fieldsToRemove) {
    return invokeCommandFunction(key, HDEL, fieldsToRemove);
  }

  @Override
  public Collection<byte[]> hgetall(RedisKey key) {
    return invokeCommandFunction(key, HGETALL);
  }

  @Override
  public int hexists(RedisKey key, byte[] field) {
    return invokeCommandFunction(key, HEXISTS, field);
  }

  @Override
  public byte[] hget(RedisKey key, byte[] field) {
    return invokeCommandFunction(key, HGET, field);
  }

  @Override
  public int hlen(RedisKey key) {
    return invokeCommandFunction(key, HLEN);
  }

  @Override
  public int hstrlen(RedisKey key, byte[] field) {
    return invokeCommandFunction(key, HSTRLEN, field);
  }

  @Override
  public List<byte[]> hmget(RedisKey key,
      List<byte[]> fields) {
    return invokeCommandFunction(key, HMGET, fields);
  }

  @Override
  public Collection<byte[]> hvals(RedisKey key) {
    return invokeCommandFunction(key, HVALS);
  }

  @Override
  public Collection<byte[]> hkeys(RedisKey key) {
    return invokeCommandFunction(key, HKEYS);
  }

  @Override
  public Pair<Integer, List<byte[]>> hscan(RedisKey key, Pattern matchPattern,
      int count, int cursor, UUID clientID) {
    return invokeCommandFunction(key, HSCAN, matchPattern, count, cursor, clientID);
  }

  @Override
  public long hincrby(RedisKey key, byte[] field, long increment) {
    return invokeCommandFunction(key, HINCRBY, field, increment);
  }

  @Override
  public BigDecimal hincrbyfloat(RedisKey key, byte[] field, BigDecimal increment) {
    return invokeCommandFunction(key, HINCRBYFLOAT, field, increment);
  }
}
