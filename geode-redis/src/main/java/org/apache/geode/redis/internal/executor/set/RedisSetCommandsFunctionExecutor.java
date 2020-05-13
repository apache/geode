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

import static org.apache.geode.redis.internal.RedisCommandType.DEL;
import static org.apache.geode.redis.internal.RedisCommandType.SADD;
import static org.apache.geode.redis.internal.RedisCommandType.SCARD;
import static org.apache.geode.redis.internal.RedisCommandType.SISMEMBER;
import static org.apache.geode.redis.internal.RedisCommandType.SMEMBERS;
import static org.apache.geode.redis.internal.RedisCommandType.SPOP;
import static org.apache.geode.redis.internal.RedisCommandType.SRANDMEMBER;
import static org.apache.geode.redis.internal.RedisCommandType.SREM;
import static org.apache.geode.redis.internal.RedisCommandType.SSCAN;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.CommandFunction;

public class RedisSetCommandsFunctionExecutor implements RedisSetCommands {

  private final Region<ByteArrayWrapper, RedisSet> region;

  public RedisSetCommandsFunctionExecutor(Region<ByteArrayWrapper, RedisSet> region) {
    this.region = region;
  }

  @Override
  public long sadd(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToAdd) {
    return CommandFunction.execute(SADD, key, membersToAdd, region);
  }

  @SuppressWarnings("unchecked")
  @Override
  public long srem(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToRemove,
      AtomicBoolean setWasDeleted) {
    Object[] resultList =
        CommandFunction.execute(SREM, key, membersToRemove, region);

    long membersRemoved = (long) resultList[0];
    Boolean wasDeleted = (Boolean) resultList[1];
    setWasDeleted.set(wasDeleted);
    return membersRemoved;
  }

  @Override
  public Set<ByteArrayWrapper> smembers(ByteArrayWrapper key) {
    return CommandFunction.execute(SMEMBERS, key, null, region);
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    return CommandFunction.execute(DEL, key, RedisDataType.REDIS_SET, region);
  }

  @Override
  public int scard(ByteArrayWrapper key) {
    return CommandFunction.execute(SCARD, key, null, region);
  }

  @Override
  public boolean sismember(ByteArrayWrapper key, ByteArrayWrapper member) {
    return CommandFunction.execute(SISMEMBER, key, member, region);
  }

  @Override
  public Collection<ByteArrayWrapper> srandmember(ByteArrayWrapper key, int count) {
    return CommandFunction.execute(SRANDMEMBER, key, count, region);
  }

  @Override
  public Collection<ByteArrayWrapper> spop(ByteArrayWrapper key, int popCount) {
    return CommandFunction.execute(SPOP, key, popCount, region);
  }

  @Override
  public List<Object> sscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return CommandFunction.execute(SSCAN, key, new Object[] {matchPattern, count, cursor}, region);
  }
}
