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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.executor.CommandFunction;

/**
 * This class is used by netty redis set command executors
 * to invoke a geode function that will run on a
 * particular server to do the redis command.
 */
public class RedisSetCommandsFunctionInvoker implements RedisSetCommands {

  private final Region<ByteArrayWrapper, RedisData> region;

  public RedisSetCommandsFunctionInvoker(Region<ByteArrayWrapper, RedisData> region) {
    this.region = region;
  }

  @Override
  public long sadd(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToAdd) {
    return CommandFunction.invoke(SADD, key, membersToAdd, region);
  }

  @SuppressWarnings("unchecked")
  @Override
  public long srem(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToRemove) {
    return CommandFunction.invoke(SREM, key, membersToRemove, region);
  }

  @Override
  public Set<ByteArrayWrapper> smembers(ByteArrayWrapper key) {
    return CommandFunction.invoke(SMEMBERS, key, null, region);
  }

  @Override
  public int scard(ByteArrayWrapper key) {
    return CommandFunction.invoke(SCARD, key, null, region);
  }

  @Override
  public boolean sismember(ByteArrayWrapper key, ByteArrayWrapper member) {
    return CommandFunction.invoke(SISMEMBER, key, member, region);
  }

  @Override
  public Collection<ByteArrayWrapper> srandmember(ByteArrayWrapper key, int count) {
    return CommandFunction.invoke(SRANDMEMBER, key, count, region);
  }

  @Override
  public Collection<ByteArrayWrapper> spop(ByteArrayWrapper key, int popCount) {
    return CommandFunction.invoke(SPOP, key, popCount, region);
  }

  @Override
  public List<Object> sscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return CommandFunction.invoke(SSCAN, key, new Object[] {matchPattern, count, cursor}, region);
  }

  @Override
  public int sunionstore(ByteArrayWrapper destination, ArrayList<ByteArrayWrapper> setKeys) {
    return CommandFunction.invoke(SUNIONSTORE, destination, setKeys, region);
  }

  @Override
  public int sinterstore(ByteArrayWrapper destination, ArrayList<ByteArrayWrapper> setKeys) {
    return CommandFunction.invoke(SINTERSTORE, destination, setKeys, region);
  }

  @Override
  public int sdiffstore(ByteArrayWrapper destination, ArrayList<ByteArrayWrapper> setKeys) {
    return CommandFunction.invoke(SDIFFSTORE, destination, setKeys, region);
  }
}
