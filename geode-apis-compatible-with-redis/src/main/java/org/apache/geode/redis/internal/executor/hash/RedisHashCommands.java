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

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.redis.internal.data.RedisKey;

public interface RedisHashCommands {
  int hset(RedisKey key, List<byte[]> fieldsToSet, boolean NX);

  int hdel(RedisKey key, List<byte[]> fieldsToRemove);

  Collection<byte[]> hgetall(RedisKey key);

  int hexists(RedisKey key, byte[] field);

  byte[] hget(RedisKey key, byte[] field);

  int hlen(RedisKey key);

  int hstrlen(RedisKey key, byte[] field);

  List<byte[]> hmget(RedisKey key, List<byte[]> fields);

  Collection<byte[]> hvals(RedisKey key);

  Collection<byte[]> hkeys(RedisKey key);

  Pair<Integer, List<byte[]>> hscan(RedisKey key, Pattern matchPattern, int count, int cursor);

  long hincrby(RedisKey key, byte[] field, long increment);

  BigDecimal hincrbyfloat(RedisKey key, byte[] field, BigDecimal increment);
}
