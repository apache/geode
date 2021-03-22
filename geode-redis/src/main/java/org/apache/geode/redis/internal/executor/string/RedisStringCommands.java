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
package org.apache.geode.redis.internal.executor.string;

import java.math.BigDecimal;
import java.util.List;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisKey;

public interface RedisStringCommands {
  long append(RedisKey key, ByteArrayWrapper valueToAppend);

  ByteArrayWrapper get(RedisKey key);

  boolean set(RedisKey key, ByteArrayWrapper value, SetOptions options);

  long incr(RedisKey key);

  long decr(RedisKey key);

  ByteArrayWrapper getset(RedisKey key, ByteArrayWrapper value);

  long incrby(RedisKey key, long increment);

  long decrby(RedisKey key, long decrement);

  ByteArrayWrapper getrange(RedisKey key, long start, long end);

  long bitcount(RedisKey key, int start, int end);

  long bitcount(RedisKey key);

  int strlen(RedisKey key);

  int getbit(RedisKey key, int offset);

  int setbit(RedisKey key, long offset, int value);

  BigDecimal incrbyfloat(RedisKey key, BigDecimal increment);

  int bitop(String operation, RedisKey destKey, List<RedisKey> sources);

  int bitpos(RedisKey key, int bit, int start, Integer end);

  int setrange(RedisKey key, int offset, byte[] value);

  ByteArrayWrapper mget(RedisKey key);
}
