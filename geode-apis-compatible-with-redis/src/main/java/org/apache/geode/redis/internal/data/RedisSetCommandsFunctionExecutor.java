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

import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SET;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.redis.internal.executor.set.RedisSetCommands;

public class RedisSetCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor implements
    RedisSetCommands {

  public RedisSetCommandsFunctionExecutor(
      CommandHelper helper) {
    super(helper);
  }

  private RedisSet getRedisSet(RedisKey key, boolean updateStats) {
    return helper.getRedisSet(key, updateStats);
  }

  @Override
  public long sadd(RedisKey key, ArrayList<ByteArrayWrapper> membersToAdd) {
    return stripedExecute(key,
        () -> getRedisSet(key, false)
            .sadd(membersToAdd,
                getRegion(), key));
  }

  @Override
  public int sunionstore(RedisKey destination, ArrayList<RedisKey> setKeys) {
    return NULL_REDIS_SET.sunionstore(helper, destination, setKeys);
  }

  @Override
  public int sinterstore(RedisKey destination, ArrayList<RedisKey> setKeys) {
    return NULL_REDIS_SET.sinterstore(helper, destination, setKeys);
  }

  @Override
  public int sdiffstore(RedisKey destination, ArrayList<RedisKey> setKeys) {
    return NULL_REDIS_SET.sdiffstore(helper, destination, setKeys);
  }

  @Override
  public long srem(RedisKey key, ArrayList<ByteArrayWrapper> membersToRemove) {
    return stripedExecute(key, () -> getRedisSet(key, false).srem(membersToRemove,
        getRegion(), key));
  }

  @Override
  public Set<ByteArrayWrapper> smembers(RedisKey key) {
    return stripedExecute(key, () -> getRedisSet(key, true).smembers());
  }

  @Override
  public Set<ByteArrayWrapper> internalsmembers(RedisKey key) {
    return stripedExecute(key, () -> getRedisSet(key, false).smembers());
  }

  @Override
  public int scard(RedisKey key) {
    return stripedExecute(key, () -> getRedisSet(key, true).scard());
  }

  @Override
  public boolean sismember(RedisKey key, ByteArrayWrapper member) {
    return stripedExecute(key, () -> getRedisSet(key, true).sismember(member));
  }

  @Override
  public Collection<ByteArrayWrapper> srandmember(RedisKey key, int count) {
    return stripedExecute(key, () -> getRedisSet(key, true).srandmember(count));
  }

  @Override
  public Collection<ByteArrayWrapper> spop(RedisKey key, int popCount) {
    return stripedExecute(key, () -> getRedisSet(key, false)
        .spop(getRegion(), key, popCount));
  }

  @Override
  public Pair<BigInteger, List<Object>> sscan(RedisKey key, Pattern matchPattern,
      int count,
      BigInteger cursor) {
    return stripedExecute(key, () -> getRedisSet(key, true).sscan(matchPattern, count, cursor));
  }

}
