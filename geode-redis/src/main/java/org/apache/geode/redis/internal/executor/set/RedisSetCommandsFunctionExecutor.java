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
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.CommandFunction;

public class RedisSetCommandsFunctionExecutor implements RedisSetCommands {

  private final Region<ByteArrayWrapper, RedisSet> region;

  public RedisSetCommandsFunctionExecutor(Region<ByteArrayWrapper, RedisSet> region) {
    this.region = region;
  }

  @Override
  public long sadd(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToAdd) {
    return executeFunction(SADD, key, membersToAdd);
  }

  @Override
  public long srem(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToRemove,
                   AtomicBoolean setWasDeleted) {
    Pair<Long, Boolean> resultList =
        executeFunction(SREM, key, membersToRemove);

    long membersRemoved = resultList.getLeft();
    Boolean wasDeleted = resultList.getRight();
    setWasDeleted.set(wasDeleted);
    return membersRemoved;
  }

  @Override
  public Set<ByteArrayWrapper> smembers(ByteArrayWrapper key) {
    return executeFunction(SMEMBERS, key, null);
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    return
        executeFunction(DEL, key, RedisDataType.REDIS_SET);
  }

  @Override
  public int scard(ByteArrayWrapper key) {
    return executeFunction(SCARD, key, null);
  }

  @Override
  public boolean sismember(ByteArrayWrapper key, ByteArrayWrapper member) {
    return executeFunction(SISMEMBER, key, member);
  }

  @Override
  public Collection<ByteArrayWrapper> srandmember(ByteArrayWrapper key, int count) {
    return executeFunction(SRANDMEMBER, key, count);
  }

  @Override
  public Collection<ByteArrayWrapper> spop(ByteArrayWrapper key, int popCount) {
    return executeFunction(SPOP, key, popCount);
  }

  @Override
  public List<Object> sscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor) {
    return executeFunction(SSCAN, key, new Object[]{matchPattern, count, cursor});
  }

  @SuppressWarnings("unchecked")
  private <T> T executeFunction(RedisCommandType command,
                            ByteArrayWrapper key,
                            Object commandArguments) {
    SingleResultCollector<T> rc = new SingleResultCollector<>();
    FunctionService
        .onRegion(region)
        .withFilter(Collections.singleton(key))
        .setArguments(new Object[]{command, commandArguments})
        .withCollector(rc)
        .execute(CommandFunction.ID);
    return rc.getResult();
  }


}
