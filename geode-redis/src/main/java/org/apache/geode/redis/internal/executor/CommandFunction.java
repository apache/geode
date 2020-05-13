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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.set.RedisSetInRegion;
import org.apache.geode.redis.internal.executor.set.SingleResultCollector;
import org.apache.geode.redis.internal.executor.set.StripedExecutor;
import org.apache.geode.redis.internal.executor.set.SynchronizedStripedExecutor;

@SuppressWarnings("unchecked")
public class CommandFunction extends SingleResultRedisFunction {

  public static final String ID = "REDIS_COMMAND_FUNCTION";

  private final transient StripedExecutor stripedExecutor;

  public static void register() {
    SynchronizedStripedExecutor stripedExecutor = new SynchronizedStripedExecutor();
    FunctionService.registerFunction(new CommandFunction(stripedExecutor));
  }

  @SuppressWarnings("unchecked")
  public static <T> T execute(RedisCommandType command,
      ByteArrayWrapper key,
      Object commandArguments, Region region) {
    SingleResultCollector<T> rc = new SingleResultCollector<>();
    FunctionService
        .onRegion(region)
        .withFilter(Collections.singleton(key))
        .setArguments(new Object[] {command, commandArguments})
        .withCollector(rc)
        .execute(CommandFunction.ID)
        .getResult();
    return rc.getResult();
  }


  public CommandFunction(StripedExecutor stripedExecutor) {
    this.stripedExecutor = stripedExecutor;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  protected Object compute(Region localRegion, ByteArrayWrapper key,
      RedisCommandType command, Object[] args) {
    Callable<Object> callable;
    switch (command) {
      case SADD: {
        ArrayList<ByteArrayWrapper> membersToAdd = (ArrayList<ByteArrayWrapper>) args[1];
        callable = () -> new RedisSetInRegion(localRegion).sadd(key, membersToAdd);
        break;
      }
      case SREM: {
        ArrayList<ByteArrayWrapper> membersToRemove = (ArrayList<ByteArrayWrapper>) args[1];
        callable = () -> {
          AtomicBoolean setWasDeleted = new AtomicBoolean();
          long srem = new RedisSetInRegion(localRegion).srem(key, membersToRemove, setWasDeleted);
          return new Object[] {srem, setWasDeleted.get()};
        };
        break;
      }
      case DEL:
        RedisDataType delType = (RedisDataType) args[1];
        callable = executeDel(key, localRegion, delType);
        break;
      case SMEMBERS:
        callable = () -> new RedisSetInRegion(localRegion).smembers(key);
        break;
      case SCARD:
        callable = () -> new RedisSetInRegion(localRegion).scard(key);
        break;
      case SISMEMBER: {
        ByteArrayWrapper member = (ByteArrayWrapper) args[1];
        callable = () -> new RedisSetInRegion(localRegion).sismember(key, member);
        break;
      }
      case SRANDMEMBER: {
        int count = (int) args[1];
        callable = () -> new RedisSetInRegion(localRegion).srandmember(key, count);
        break;
      }
      case SPOP: {
        int popCount = (int) args[1];
        callable = () -> new RedisSetInRegion(localRegion).spop(key, popCount);
        break;
      }
      case SSCAN: {
        Pattern matchPattern = (Pattern) args[0];
        int count = (int) args[1];
        int cursor = (int) args[2];
        callable =
            () -> new RedisSetInRegion(localRegion).sscan(key, matchPattern, count, cursor);
        break;
      }
      case HSET: {
        Object[] hsetArgs = (Object[]) args[1];
        List<ByteArrayWrapper> fieldsToSet = (List<ByteArrayWrapper>) hsetArgs[0];
        boolean NX = (boolean) hsetArgs[1];
        callable = () -> new RedisHashInRegion(localRegion).hset(key, fieldsToSet, NX);
        break;
      }
      case HDEL: {
        List<ByteArrayWrapper> fieldsToRemove = (List<ByteArrayWrapper>) args[1];
        callable = () -> new RedisHashInRegion(localRegion).hdel(key, fieldsToRemove);
        break;
      }
      case HGETALL: {
        callable = () -> new RedisHashInRegion(localRegion).hgetall(key);
        break;
      }
      default:
        throw new UnsupportedOperationException(ID + " does not yet support " + command);
    }
    return stripedExecutor.execute(key, callable);
  }


  @SuppressWarnings("unchecked")
  private Callable<Object> executeDel(ByteArrayWrapper key, Region localRegion,
      RedisDataType delType) {
    switch (delType) {
      case REDIS_SET:
        return () -> new RedisSetInRegion(localRegion).del(key);
      case REDIS_HASH:
        return () -> new RedisHashInRegion(localRegion).del(key);
      default:
        throw new UnsupportedOperationException("DEL does not support " + delType);
    }
  }

}
