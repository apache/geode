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

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.executor.CommandFunction;

@SuppressWarnings("unchecked")
public class RedisHashCommandsFunctionExecutor implements RedisHashCommands {

  private final Region<ByteArrayWrapper, RedisData> region;

  public RedisHashCommandsFunctionExecutor(Region<ByteArrayWrapper, RedisData> region) {
    this.region = region;
  }

  @Override
  public int hset(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToSet, boolean NX) {
    return CommandFunction.execute(HSET, key, new Object[] {fieldsToSet, NX}, region);
  }

  @Override
  public int hdel(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToRemove) {
    return CommandFunction.execute(HDEL, key, fieldsToRemove, region);
  }

  @Override
  public Collection<ByteArrayWrapper> hgetall(ByteArrayWrapper key) {
    return CommandFunction.execute(HGETALL, key, null, region);
  }

  @Override
  public int hexists(ByteArrayWrapper key, ByteArrayWrapper field) {
    return CommandFunction.execute(HEXISTS, key, field, region);
  }

  @Override
  public ByteArrayWrapper hget(ByteArrayWrapper key, ByteArrayWrapper field) {
    return CommandFunction.execute(HGET, key, field, region);
  }

  @Override
  public int hlen(ByteArrayWrapper key) {
    return CommandFunction.execute(HLEN, key, null, region);
  }

  @Override
  public int hstrlen(ByteArrayWrapper key, ByteArrayWrapper field) {
    return CommandFunction.execute(HSTRLEN, key, field, region);
  }

  @Override
  public List<ByteArrayWrapper> hmget(ByteArrayWrapper key,
      List<ByteArrayWrapper> fields) {
    return CommandFunction.execute(HMGET, key, fields, region);
  }

  @Override
  public Collection<ByteArrayWrapper> hvals(ByteArrayWrapper key) {
    return CommandFunction.execute(HVALS, key, null, region);
  }

  @Override
  public Collection<ByteArrayWrapper> hkeys(ByteArrayWrapper key) {
    return CommandFunction.execute(HKEYS, key, null, region);
  }

  @Override
  public List<Object> hscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return CommandFunction.execute(HSCAN, key, new Object[] {matchPattern, count, cursor}, region);
  }

  @Override
  public long hincrby(ByteArrayWrapper key, ByteArrayWrapper field, long increment) {
    return CommandFunction.execute(HINCRBY, key, new Object[] {field, increment}, region);
  }

  @Override
  public double hincrbyfloat(ByteArrayWrapper key, ByteArrayWrapper field, double increment) {
    return CommandFunction.execute(HINCRBYFLOAT, key, new Object[] {field, increment}, region);
  }
}
