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

package org.apache.geode.redis.internal.executor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.executor.set.RedisSet;
import org.apache.geode.redis.internal.executor.set.StripedExecutor;
import org.apache.geode.redis.internal.executor.set.SynchronizedStripedExecutor;

public class CommandFunction implements Function<Object[]> {

  public static final String ID = "REDIS_COMMAND_FUNCTION";

  private final transient StripedExecutor stripedExecutor;

  public static void register() {
    SynchronizedStripedExecutor stripedExecutor = new SynchronizedStripedExecutor();
    FunctionService.registerFunction(new CommandFunction(stripedExecutor));
  }

  @SuppressWarnings("unchecked")
  public static ResultCollector execute(Region<?,?> region,
                                          RedisCommandType command,
                                          ByteArrayWrapper key,
                                          ArrayList<ByteArrayWrapper> commandArguments) {
    return FunctionService
        .onRegion(region)
        .withFilter(Collections.singleton(key))
        .setArguments(new Object[] {command, commandArguments})
        .execute(ID);
  }


  public CommandFunction(StripedExecutor stripedExecutor) {
    this.stripedExecutor = stripedExecutor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(FunctionContext<Object[]> context) {
    RegionFunctionContextImpl regionFunctionContext =
        (RegionFunctionContextImpl) context;
    ByteArrayWrapper key =
        (ByteArrayWrapper) regionFunctionContext.getFilter().iterator().next();
    Region<ByteArrayWrapper, RedisSet> localRegion =
        regionFunctionContext.getLocalDataSet(regionFunctionContext.getDataSet());
    ResultSender resultSender = regionFunctionContext.getResultSender();
    Object[] args = context.getArguments();
    RedisCommandType command = (RedisCommandType) args[0];
    switch (command) {
      case SADD: {
        ArrayList<ByteArrayWrapper> membersToAdd = (ArrayList<ByteArrayWrapper>) args[1];
        stripedExecutor.execute(key,
            () -> RedisSet.sadd(localRegion, key, membersToAdd),
            (addedCount) -> resultSender.lastResult(addedCount));
        break;
      }
      case SREM: {
        ArrayList<ByteArrayWrapper> membersToRemove = (ArrayList<ByteArrayWrapper>) args[1];
        AtomicBoolean setWasDeleted = new AtomicBoolean();
        stripedExecutor.execute(key,
            () -> RedisSet.srem(localRegion, key, membersToRemove, setWasDeleted),
            (removedCount) -> {
              resultSender.sendResult(removedCount);
              resultSender.lastResult(setWasDeleted.get() ? 1L : 0L);
            });
        break;
      }
      case DEL:
        stripedExecutor.execute(key,
            () -> RedisSet.del(localRegion, key),
            (deleted) -> resultSender.lastResult(deleted));
        break;
      case SMEMBERS:
        stripedExecutor.execute(key,
            () -> RedisSet.smembers(localRegion, key),
            (members) -> resultSender.lastResult(members));
        break;
      case SCARD:
        stripedExecutor.execute(key,
            () -> RedisSet.scard(localRegion, key),
            (size) -> resultSender.lastResult(size));
        break;
      case SISMEMBER: {
        ByteArrayWrapper member = (ByteArrayWrapper) args[1];
        stripedExecutor.execute(key,
            () -> RedisSet.sismember(localRegion, key, member),
            (exists) -> resultSender.lastResult(exists));
        break;
      }
      case SRANDMEMBER: {
        int count = (int) args[1];
        stripedExecutor.execute(key,
            () -> RedisSet.srandmember(localRegion, key, count),
            (members) -> resultSender.lastResult(members));
        break;
      }
      case SPOP: {
        int popCount = (int) args[1];
        stripedExecutor.execute(key,
            () -> RedisSet.spop(localRegion, key, popCount),
            (members) -> resultSender.lastResult(members));
        break;
      }
      case SSCAN: {
        Pattern matchPattern = (Pattern) args[0];
        int count = (int) args[1];
        int cursor = (int) args[2];
        stripedExecutor.execute(key,
            () -> RedisSet.sscan(localRegion, key, matchPattern, count, cursor),
            (members) -> resultSender.lastResult(members));
        break;
      }
      default:
        throw new UnsupportedOperationException(ID + " does not yet support " + command);
    }
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public String getId() {
    return ID;
  }
}
