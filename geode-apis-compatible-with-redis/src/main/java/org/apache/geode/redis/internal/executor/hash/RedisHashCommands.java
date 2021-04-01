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
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisKey;

public interface RedisHashCommands {
  int hset(RedisKey key, List<ByteArrayWrapper> fieldsToSet, boolean NX);

  int hdel(RedisKey key, List<ByteArrayWrapper> fieldsToRemove);

  Collection<ByteArrayWrapper> hgetall(RedisKey key);

  int hexists(RedisKey key, ByteArrayWrapper field);

  ByteArrayWrapper hget(RedisKey key, ByteArrayWrapper field);

  int hlen(RedisKey key);

  int hstrlen(RedisKey key, ByteArrayWrapper field);

  List<ByteArrayWrapper> hmget(RedisKey key, List<ByteArrayWrapper> fields);

  Collection<ByteArrayWrapper> hvals(RedisKey key);

  Collection<ByteArrayWrapper> hkeys(RedisKey key);

  Pair<Integer, List<Object>> hscan(RedisKey key, Pattern matchPattern, int count,
      int cursor, UUID clientID);

  long hincrby(RedisKey key, ByteArrayWrapper field, long increment);

  BigDecimal hincrbyfloat(RedisKey key, ByteArrayWrapper field, BigDecimal increment);
}
