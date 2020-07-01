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
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.CommandHelper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisDataCommands;
import org.apache.geode.redis.internal.executor.string.SetOptions;

@SuppressWarnings("unchecked")
public class CommandFunction extends SingleResultRedisFunction {

  public static final String ID = "REDIS_COMMAND_FUNCTION";

  private final transient RedisDataCommands dataCommands;

  public static void register(Region<ByteArrayWrapper, RedisData> dataRegion,
      StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    FunctionService.registerFunction(new CommandFunction(dataRegion, stripedExecutor, redisStats));
  }

  public static <T> T execute(RedisCommandType command,
      ByteArrayWrapper key,
      Object commandArguments, Region<ByteArrayWrapper, RedisData> region) {
    SingleResultCollector<T> resultsCollector = new SingleResultCollector<>();
    FunctionService
        .onRegion(region)
        .withFilter(Collections.singleton(key))
        .setArguments(new Object[] {command, commandArguments})
        .withCollector(resultsCollector)
        .execute(CommandFunction.ID)
        .getResult();

    return resultsCollector.getResult();
  }


  public CommandFunction(Region<ByteArrayWrapper, RedisData> dataRegion,
      StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    super(dataRegion);
    dataCommands =
        new RedisDataCommands(new CommandHelper(dataRegion, redisStats, stripedExecutor));
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
        return dataCommands.del(key);
      case EXISTS:
        return dataCommands.exists(key);
      case TYPE:
        return dataCommands.type(key);
      case PEXPIREAT: {
        long timestamp = (long) args[1];
        return dataCommands.pexpireat(key, timestamp);
      }
      case PERSIST:
        return dataCommands.persist(key);
      case PTTL:
        return dataCommands.pttl(key);
      case APPEND: {
        ByteArrayWrapper valueToAdd = (ByteArrayWrapper) args[1];
        return dataCommands.append(key, valueToAdd);
      }
      case GET:
        return dataCommands.get(key);
      case MGET:
        return dataCommands.mget(key);
      case STRLEN:
        return dataCommands.strlen(key);
      case SET: {
        Object[] argArgs = (Object[]) args[1];
        ByteArrayWrapper value = (ByteArrayWrapper) argArgs[0];
        SetOptions options = (SetOptions) argArgs[1];
        return dataCommands.set(key, value, options);
      }
      case GETSET: {
        ByteArrayWrapper value = (ByteArrayWrapper) args[1];
        return dataCommands.getset(key, value);
      }
      case GETRANGE: {
        Object[] argArgs = (Object[]) args[1];
        long start = (long) argArgs[0];
        long end = (long) argArgs[1];
        return dataCommands.getrange(key, start, end);
      }
      case SETRANGE: {
        Object[] argArgs = (Object[]) args[1];
        int offset = (int) argArgs[0];
        byte[] value = (byte[]) argArgs[1];
        return dataCommands.setrange(key, offset, value);
      }
      case BITCOUNT: {
        Object[] argArgs = (Object[]) args[1];
        if (argArgs == null) {
          return dataCommands.bitcount(key);
        } else {
          int start = (int) argArgs[0];
          int end = (int) argArgs[1];
          return dataCommands.bitcount(key, start, end);
        }
      }
      case BITPOS: {
        Object[] argArgs = (Object[]) args[1];
        int bit = (int) argArgs[0];
        int start = (int) argArgs[1];
        Integer end = (Integer) argArgs[2];
        return dataCommands.bitpos(key, bit, start, end);
      }
      case GETBIT: {
        int offset = (int) args[1];
        return dataCommands.getbit(key, offset);
      }
      case SETBIT: {
        Object[] argArgs = (Object[]) args[1];
        long offset = (long) argArgs[0];
        int value = (int) argArgs[1];
        return dataCommands.setbit(key, offset, value);
      }
      case BITOP: {
        Object[] argArgs = (Object[]) args[1];
        String operation = (String) argArgs[0];
        List<ByteArrayWrapper> sources = (List<ByteArrayWrapper>) argArgs[1];
        return dataCommands.bitop(operation, key, sources);
      }
      case INCR:
        return dataCommands.incr(key);
      case DECR:
        return dataCommands.decr(key);
      case INCRBY: {
        long increment = (long) args[1];
        return dataCommands.incrby(key, increment);
      }
      case INCRBYFLOAT: {
        double increment = (double) args[1];
        return dataCommands.incrbyfloat(key, increment);
      }
      case DECRBY: {
        long decrement = (long) args[1];
        return dataCommands.decrby(key, decrement);
      }
      case SADD: {
        ArrayList<ByteArrayWrapper> membersToAdd = (ArrayList<ByteArrayWrapper>) args[1];
        return dataCommands.sadd(key, membersToAdd);
      }
      case SREM: {
        ArrayList<ByteArrayWrapper> membersToRemove = (ArrayList<ByteArrayWrapper>) args[1];
        return dataCommands.srem(key, membersToRemove);
      }
      case SMEMBERS:
        return dataCommands.smembers(key);
      case SCARD:
        return dataCommands.scard(key);
      case SISMEMBER: {
        ByteArrayWrapper member = (ByteArrayWrapper) args[1];
        return dataCommands.sismember(key, member);
      }
      case SRANDMEMBER: {
        int count = (int) args[1];
        return dataCommands.srandmember(key, count);
      }
      case SPOP: {
        int popCount = (int) args[1];
        return dataCommands.spop(key, popCount);
      }
      case SSCAN: {
        Object[] sscanArgs = (Object[]) args[1];
        Pattern matchPattern = (Pattern) sscanArgs[0];
        int count = (int) sscanArgs[1];
        int cursor = (int) sscanArgs[2];
        return dataCommands.sscan(key, matchPattern, count, cursor);
      }
      case SUNIONSTORE: {
        ArrayList<ByteArrayWrapper> setKeys = (ArrayList<ByteArrayWrapper>) args[1];
        return dataCommands.sunionstore(key, setKeys);
      }
      case SINTERSTORE: {
        ArrayList<ByteArrayWrapper> setKeys = (ArrayList<ByteArrayWrapper>) args[1];
        return dataCommands.sinterstore(key, setKeys);
      }
      case SDIFFSTORE: {
        ArrayList<ByteArrayWrapper> setKeys = (ArrayList<ByteArrayWrapper>) args[1];
        return dataCommands.sdiffstore(key, setKeys);
      }
      case HSET: {
        Object[] hsetArgs = (Object[]) args[1];
        List<ByteArrayWrapper> fieldsToSet = (List<ByteArrayWrapper>) hsetArgs[0];
        boolean NX = (boolean) hsetArgs[1];
        return dataCommands.hset(key, fieldsToSet, NX);
      }
      case HDEL: {
        List<ByteArrayWrapper> fieldsToRemove = (List<ByteArrayWrapper>) args[1];
        return dataCommands.hdel(key, fieldsToRemove);
      }
      case HGETALL:
        return dataCommands.hgetall(key);
      case HEXISTS: {
        ByteArrayWrapper field = (ByteArrayWrapper) args[1];
        return dataCommands.hexists(key, field);
      }
      case HGET: {
        ByteArrayWrapper field = (ByteArrayWrapper) args[1];
        return dataCommands.hget(key, field);
      }
      case HLEN:
        return dataCommands.hlen(key);
      case HSTRLEN: {
        ByteArrayWrapper field = (ByteArrayWrapper) args[1];
        return dataCommands.hstrlen(key, field);
      }
      case HMGET: {
        List<ByteArrayWrapper> fields = (List<ByteArrayWrapper>) args[1];
        return dataCommands.hmget(key, fields);
      }
      case HVALS:
        return dataCommands.hvals(key);
      case HKEYS:
        return dataCommands.hkeys(key);
      case HSCAN: {
        Object[] hscanArgs = (Object[]) args[1];
        Pattern pattern = (Pattern) hscanArgs[0];
        int count = (int) hscanArgs[1];
        int cursor = (int) hscanArgs[2];
        return dataCommands.hscan(key, pattern, count, cursor);
      }
      case HINCRBY: {
        Object[] hsetArgs = (Object[]) args[1];
        ByteArrayWrapper field = (ByteArrayWrapper) hsetArgs[0];
        long increment = (long) hsetArgs[1];
        return dataCommands.hincrby(key, field, increment);
      }
      case HINCRBYFLOAT: {
        Object[] hsetArgs = (Object[]) args[1];
        ByteArrayWrapper field = (ByteArrayWrapper) hsetArgs[0];
        double increment = (double) hsetArgs[1];
        return dataCommands.hincrbyfloat(key, field, increment);
      }
      default:
        throw new UnsupportedOperationException(ID + " does not yet support " + command);
    }
  }

}
