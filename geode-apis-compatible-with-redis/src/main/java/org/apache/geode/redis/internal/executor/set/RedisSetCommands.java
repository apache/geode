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

package org.apache.geode.redis.internal.executor.set;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.redis.internal.data.RedisKey;

public interface RedisSetCommands {

  long sadd(RedisKey key, List<byte[]> membersToAdd);

  long srem(RedisKey key, List<byte[]> membersToRemove);

  Set<byte[]> smembers(RedisKey key);

  Set<byte[]> internalsmembers(RedisKey key);

  int scard(RedisKey key);

  boolean sismember(RedisKey key, byte[] member);

  Collection<byte[]> srandmember(RedisKey key, int count);

  Collection<byte[]> spop(RedisKey key, int popCount);

  Pair<BigInteger, List<Object>> sscan(RedisKey key, Pattern matchPattern, int count,
      BigInteger cursor);

  int sunionstore(RedisKey destination, List<RedisKey> setKeys);

  int sinterstore(RedisKey destination, List<RedisKey> setKeys);

  int sdiffstore(RedisKey destination, List<RedisKey> setKeys);
}
