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
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.CommandHelper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisHashCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisKeyCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisSetCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisStringCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.string.SetOptions;

@SuppressWarnings("unchecked")
public class CommandFunction extends SingleResultRedisFunction {

  public static final String ID = "REDIS_COMMAND_FUNCTION";

  private final transient RedisKeyCommandsFunctionExecutor keyCommands;
  private final transient RedisHashCommandsFunctionExecutor hashCommands;
  private final transient RedisSetCommandsFunctionExecutor setCommands;
  private final transient RedisStringCommandsFunctionExecutor stringCommands;

  public static void register(Region<ByteArrayWrapper, RedisData> dataRegion,
      StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    FunctionService.registerFunction(new CommandFunction(dataRegion, stripedExecutor, redisStats));
  }

  public static <T> T invoke(RedisCommandType command,
      ByteArrayWrapper key,
      Object commandArguments, Region<ByteArrayWrapper, RedisData> region) {
    do {
      SingleResultCollector<T> resultsCollector = new SingleResultCollector<>();
      try {
        FunctionService
            .onRegion(region)
            .withFilter(Collections.singleton(key))
            .setArguments(new Object[] {command, commandArguments})
            .withCollector(resultsCollector)
            .execute(CommandFunction.ID)
            .getResult();
        return resultsCollector.getResult();
      } catch (BucketMovedException ex) {
        // try again
      }
    } while (true);
  }

  public CommandFunction(Region<ByteArrayWrapper, RedisData> dataRegion,
      StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    super(dataRegion);
    CommandHelper helper = new CommandHelper(dataRegion, redisStats, stripedExecutor);
    keyCommands = new RedisKeyCommandsFunctionExecutor(helper);
    hashCommands = new RedisHashCommandsFunctionExecutor(helper);
    setCommands = new RedisSetCommandsFunctionExecutor(helper);
    stringCommands = new RedisStringCommandsFunctionExecutor(helper);
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  protected Object compute(ByteArrayWrapper key, Object[] args) {
    RedisCommandType command = (RedisCommandType) args[0];
    switch (command) {
      case DEL:
        return keyCommands.del(key);
      case EXISTS:
        return keyCommands.exists(key);
      case TYPE:
        return keyCommands.type(key);
      case PEXPIREAT: {
        long timestamp = (long) args[1];
        return keyCommands.pexpireat(key, timestamp);
      }
      case PERSIST:
        return keyCommands.persist(key);
      case PTTL:
        return keyCommands.pttl(key);
      case APPEND: {
        ByteArrayWrapper valueToAdd = (ByteArrayWrapper) args[1];
        return stringCommands.append(key, valueToAdd);
      }
      case GET:
        return stringCommands.get(key);
      case MGET:
        return stringCommands.mget(key);
      case STRLEN:
        return stringCommands.strlen(key);
      case SET: {
        Object[] argArgs = (Object[]) args[1];
        ByteArrayWrapper value = (ByteArrayWrapper) argArgs[0];
        SetOptions options = (SetOptions) argArgs[1];
        return stringCommands.set(key, value, options);
      }
      case GETSET: {
        ByteArrayWrapper value = (ByteArrayWrapper) args[1];
        return stringCommands.getset(key, value);
      }
      case GETRANGE: {
        Object[] argArgs = (Object[]) args[1];
        long start = (long) argArgs[0];
        long end = (long) argArgs[1];
        return stringCommands.getrange(key, start, end);
      }
      case SETRANGE: {
        Object[] argArgs = (Object[]) args[1];
        int offset = (int) argArgs[0];
        byte[] value = (byte[]) argArgs[1];
        return stringCommands.setrange(key, offset, value);
      }
      case BITCOUNT: {
        Object[] argArgs = (Object[]) args[1];
        if (argArgs == null) {
          return stringCommands.bitcount(key);
        } else {
          int start = (int) argArgs[0];
          int end = (int) argArgs[1];
          return stringCommands.bitcount(key, start, end);
        }
      }
      case BITPOS: {
        Object[] argArgs = (Object[]) args[1];
        int bit = (int) argArgs[0];
        int start = (int) argArgs[1];
        Integer end = (Integer) argArgs[2];
        return stringCommands.bitpos(key, bit, start, end);
      }
      case GETBIT: {
        int offset = (int) args[1];
        return stringCommands.getbit(key, offset);
      }
      case SETBIT: {
        Object[] argArgs = (Object[]) args[1];
        long offset = (long) argArgs[0];
        int value = (int) argArgs[1];
        return stringCommands.setbit(key, offset, value);
      }
      case BITOP: {
        Object[] argArgs = (Object[]) args[1];
        String operation = (String) argArgs[0];
        List<ByteArrayWrapper> sources = (List<ByteArrayWrapper>) argArgs[1];
        return stringCommands.bitop(operation, key, sources);
      }
      case INCR:
        return stringCommands.incr(key);
      case DECR:
        return stringCommands.decr(key);
      case INCRBY: {
        long increment = (long) args[1];
        return stringCommands.incrby(key, increment);
      }
      case INCRBYFLOAT: {
        double increment = (double) args[1];
        return stringCommands.incrbyfloat(key, increment);
      }
      case DECRBY: {
        long decrement = (long) args[1];
        return stringCommands.decrby(key, decrement);
      }
      case SADD: {
        ArrayList<ByteArrayWrapper> membersToAdd = (ArrayList<ByteArrayWrapper>) args[1];
        return setCommands.sadd(key, membersToAdd);
      }
      case SREM: {
        ArrayList<ByteArrayWrapper> membersToRemove = (ArrayList<ByteArrayWrapper>) args[1];
        return setCommands.srem(key, membersToRemove);
      }
      case SMEMBERS:
        return setCommands.smembers(key);
      case SCARD:
        return setCommands.scard(key);
      case SISMEMBER: {
        ByteArrayWrapper member = (ByteArrayWrapper) args[1];
        return setCommands.sismember(key, member);
      }
      case SRANDMEMBER: {
        int count = (int) args[1];
        return setCommands.srandmember(key, count);
      }
      case SPOP: {
        int popCount = (int) args[1];
        return setCommands.spop(key, popCount);
      }
      case SSCAN: {
        Object[] sscanArgs = (Object[]) args[1];
        Pattern matchPattern = (Pattern) sscanArgs[0];
        int count = (int) sscanArgs[1];
        int cursor = (int) sscanArgs[2];
        return setCommands.sscan(key, matchPattern, count, cursor);
      }
      case SUNIONSTORE: {
        ArrayList<ByteArrayWrapper> setKeys = (ArrayList<ByteArrayWrapper>) args[1];
        return setCommands.sunionstore(key, setKeys);
      }
      case SINTERSTORE: {
        ArrayList<ByteArrayWrapper> setKeys = (ArrayList<ByteArrayWrapper>) args[1];
        return setCommands.sinterstore(key, setKeys);
      }
      case SDIFFSTORE: {
        ArrayList<ByteArrayWrapper> setKeys = (ArrayList<ByteArrayWrapper>) args[1];
        return setCommands.sdiffstore(key, setKeys);
      }
      case HSET: {
        Object[] hsetArgs = (Object[]) args[1];
        List<ByteArrayWrapper> fieldsToSet = (List<ByteArrayWrapper>) hsetArgs[0];
        boolean NX = (boolean) hsetArgs[1];
        return hashCommands.hset(key, fieldsToSet, NX);
      }
      case HDEL: {
        List<ByteArrayWrapper> fieldsToRemove = (List<ByteArrayWrapper>) args[1];
        return hashCommands.hdel(key, fieldsToRemove);
      }
      case HGETALL:
        return hashCommands.hgetall(key);
      case HEXISTS: {
        ByteArrayWrapper field = (ByteArrayWrapper) args[1];
        return hashCommands.hexists(key, field);
      }
      case HGET: {
        ByteArrayWrapper field = (ByteArrayWrapper) args[1];
        return hashCommands.hget(key, field);
      }
      case HLEN:
        return hashCommands.hlen(key);
      case HSTRLEN: {
        ByteArrayWrapper field = (ByteArrayWrapper) args[1];
        return hashCommands.hstrlen(key, field);
      }
      case HMGET: {
        List<ByteArrayWrapper> fields = (List<ByteArrayWrapper>) args[1];
        return hashCommands.hmget(key, fields);
      }
      case HVALS:
        return hashCommands.hvals(key);
      case HKEYS:
        return hashCommands.hkeys(key);
      case HSCAN: {
        Object[] hscanArgs = (Object[]) args[1];
        Pattern pattern = (Pattern) hscanArgs[0];
        int count = (int) hscanArgs[1];
        int cursor = (int) hscanArgs[2];
        return hashCommands.hscan(key, pattern, count, cursor);
      }
      case HINCRBY: {
        Object[] hsetArgs = (Object[]) args[1];
        ByteArrayWrapper field = (ByteArrayWrapper) hsetArgs[0];
        long increment = (long) hsetArgs[1];
        return hashCommands.hincrby(key, field, increment);
      }
      case HINCRBYFLOAT: {
        Object[] hsetArgs = (Object[]) args[1];
        ByteArrayWrapper field = (ByteArrayWrapper) hsetArgs[0];
        double increment = (double) hsetArgs[1];
        return hashCommands.hincrbyfloat(key, field, increment);
      }
      default:
        throw new UnsupportedOperationException(ID + " does not yet support " + command);
    }
  }

}
