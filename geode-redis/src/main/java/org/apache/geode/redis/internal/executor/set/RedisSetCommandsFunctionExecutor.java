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
import static org.apache.geode.redis.internal.RedisCommandType.SMEMBERS;
import static org.apache.geode.redis.internal.RedisCommandType.SREM;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.executor.CommandFunction;

@SuppressWarnings("unchecked")
public class RedisSetCommandsFunctionExecutor implements RedisSetCommands {

  private final Region<ByteArrayWrapper, RedisSet> region;

  public RedisSetCommandsFunctionExecutor(Region<ByteArrayWrapper, RedisSet> region) {
    this.region = region;
  }

  public static void registerFunctions() {
    SynchronizedStripedExecutor stripedExecutor = new SynchronizedStripedExecutor();
    FunctionService.registerFunction(new CommandFunction(stripedExecutor));
  }

  @Override
  public long sadd(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToAdd) {
    ResultCollector<Object[], List<Long>> results = executeFunction(SADD, key, membersToAdd);
    return results.getResult().get(0);
  }

  @Override
  public long srem(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToRemove,
      AtomicBoolean setWasDeleted) {
    ResultCollector<Object[], List<Long>> results = executeFunction(SREM, key, membersToRemove);
    List<Long> resultList = results.getResult();
    long membersRemoved = resultList.get(0);
    long wasDeleted = resultList.get(1);
    if (wasDeleted != 0) {
      setWasDeleted.set(true);
    }
    return membersRemoved;
  }

  @Override
  public Set<ByteArrayWrapper> members(ByteArrayWrapper key) {
    ResultCollector<Object[], List<Set<ByteArrayWrapper>>> results =
        executeFunction(SMEMBERS, key, null);
    return results.getResult().get(0);
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    ResultCollector<Object[], List<Boolean>> results = executeFunction(DEL, key, null);
    return results.getResult().get(0);
  }

  @Override
  public int scard(ByteArrayWrapper key) {
    ResultCollector<Object[], List<Integer>> results = executeFunction(SCARD, key,null);
    return results.getResult().get(0);
  }

  private ResultCollector executeFunction(RedisCommandType command,
      ByteArrayWrapper key,
      ArrayList<ByteArrayWrapper> commandArguments) {
    return FunctionService
        .onRegion(region)
        .withFilter(Collections.singleton(key))
        .setArguments(new Object[] {command, commandArguments})
        .execute(CommandFunction.ID);
  }
}
