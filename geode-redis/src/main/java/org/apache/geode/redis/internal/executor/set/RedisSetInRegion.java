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

import static org.apache.geode.redis.internal.RedisDataType.REDIS_SET;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisDataTypeMismatchException;

/**
 * This class still uses "synchronized" to protect the underlying HashSet even though all writers do
 * so under the {@link SynchronizedStripedExecutor}. The synchronization on this class can be
 * removed once readers are changed to also use the {@link SynchronizedStripedExecutor}.
 */
public class RedisSetInRegion implements RedisSetCommands {
  private final Region<ByteArrayWrapper, RedisData> region;

  @SuppressWarnings("unchecked")
  public RedisSetInRegion(Region<ByteArrayWrapper, RedisData> region) {
    this.region = region;
  }

  @Override
  public long sadd(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToAdd) {

    RedisSet redisSet = checkType(region.get(key));

    if (redisSet != null) {
      return redisSet.sadd(membersToAdd, region, key);
    } else {
      region.create(key, new RedisSet(membersToAdd));
      return membersToAdd.size();
    }
  }

  @Override
  public long srem(
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> membersToRemove) {
    return getRedisSet(key).srem(membersToRemove, region, key);
  }

  @Override
  public Set<ByteArrayWrapper> smembers(
      ByteArrayWrapper key) {
    return getRedisSet(key).smembers();
  }

  @Override
  public int scard(ByteArrayWrapper key) {
    return getRedisSet(key).scard();
  }

  @Override
  public boolean sismember(
      ByteArrayWrapper key, ByteArrayWrapper member) {
    return getRedisSet(key).sismember(member);
  }

  @Override
  public Collection<ByteArrayWrapper> srandmember(
      ByteArrayWrapper key, int count) {
    return getRedisSet(key).srandmember(count);
  }

  @Override
  public Collection<ByteArrayWrapper> spop(
      ByteArrayWrapper key, int popCount) {
    return getRedisSet(key).spop(region, key, popCount);
  }

  @Override
  public List<Object> sscan(
      ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return getRedisSet(key).sscan(matchPattern, count, cursor);
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    return region.remove(key) != null;
  }

  private RedisSet getRedisSet(ByteArrayWrapper key) {
    return checkType(region.getOrDefault(key, RedisSet.EMPTY));
  }

  private RedisSet checkType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_SET) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisSet) redisData;
  }

}
