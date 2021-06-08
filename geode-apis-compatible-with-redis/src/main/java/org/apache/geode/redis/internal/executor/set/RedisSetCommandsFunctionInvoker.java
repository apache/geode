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

import static org.apache.geode.redis.internal.RedisCommandType.INTERNALSMEMBERS;
import static org.apache.geode.redis.internal.RedisCommandType.SADD;
import static org.apache.geode.redis.internal.RedisCommandType.SCARD;
import static org.apache.geode.redis.internal.RedisCommandType.SDIFFSTORE;
import static org.apache.geode.redis.internal.RedisCommandType.SINTERSTORE;
import static org.apache.geode.redis.internal.RedisCommandType.SISMEMBER;
import static org.apache.geode.redis.internal.RedisCommandType.SMEMBERS;
import static org.apache.geode.redis.internal.RedisCommandType.SPOP;
import static org.apache.geode.redis.internal.RedisCommandType.SRANDMEMBER;
import static org.apache.geode.redis.internal.RedisCommandType.SREM;
import static org.apache.geode.redis.internal.RedisCommandType.SSCAN;
import static org.apache.geode.redis.internal.RedisCommandType.SUNIONSTORE;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisCommandsFunctionInvoker;

/**
 * This class is used by netty redis set command executors
 * to invoke a geode function that will run on a
 * particular server to do the redis command.
 */
public class RedisSetCommandsFunctionInvoker extends RedisCommandsFunctionInvoker
    implements RedisSetCommands {

  public RedisSetCommandsFunctionInvoker(Region<RedisKey, RedisData> region) {
    super(region);
  }

  @Override
  public long sadd(RedisKey key, List<byte[]> membersToAdd) {
    return invokeCommandFunction(key, SADD, membersToAdd);
  }

  @Override
  public long srem(RedisKey key, List<byte[]> membersToRemove) {
    return invokeCommandFunction(key, SREM, membersToRemove);
  }

  @Override
  public Set<byte[]> smembers(RedisKey key) {
    return invokeCommandFunction(key, SMEMBERS);
  }

  @Override
  public Set<byte[]> internalsmembers(RedisKey key) {
    return invokeCommandFunction(key, INTERNALSMEMBERS);
  }

  @Override
  public int scard(RedisKey key) {
    return invokeCommandFunction(key, SCARD);
  }

  @Override
  public boolean sismember(RedisKey key, byte[] member) {
    return invokeCommandFunction(key, SISMEMBER, member);
  }

  @Override
  public Collection<byte[]> srandmember(RedisKey key, int count) {
    return invokeCommandFunction(key, SRANDMEMBER, count);
  }

  @Override
  public Collection<byte[]> spop(RedisKey key, int popCount) {
    return invokeCommandFunction(key, SPOP, popCount);
  }

  @Override
  public Pair<BigInteger, List<Object>> sscan(RedisKey key, Pattern matchPattern, int count,
      BigInteger cursor) {
    return invokeCommandFunction(key, SSCAN, matchPattern, count, cursor);
  }

  @Override
  public int sunionstore(RedisKey destination, List<RedisKey> setKeys) {
    return invokeCommandFunction(destination, SUNIONSTORE, setKeys);
  }

  @Override
  public int sinterstore(RedisKey destination, List<RedisKey> setKeys) {
    return invokeCommandFunction(destination, SINTERSTORE, setKeys);
  }

  @Override
  public int sdiffstore(RedisKey destination, List<RedisKey> setKeys) {
    return invokeCommandFunction(destination, SDIFFSTORE, setKeys);
  }
}
