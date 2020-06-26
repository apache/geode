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
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
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
    dataCommands = new RedisDataCommands(dataRegion, redisStats, stripedExecutor);
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  protected Object compute(ByteArrayWrapper key, Object[] args) {
    RedisCommandType command = (RedisCommandType) args[0];
    Callable<Object> callable;
    boolean useStripedExecutor = true;
    switch (command) {
      case DEL:
        callable = () -> dataCommands.del(key);
        break;
      case EXISTS:
        callable = () -> dataCommands.exists(key);
        break;
      case TYPE:
        callable = () -> dataCommands.type(key);
        break;
      case PEXPIREAT: {
        long timestamp = (long) args[1];
        callable =
            () -> dataCommands.pexpireat(key, timestamp);
        break;
      }
      case PERSIST:
        callable = () -> dataCommands.persist(key);
        break;
      case PTTL:
        callable = () -> dataCommands.pttl(key);
        break;
      case APPEND: {
        ByteArrayWrapper valueToAdd = (ByteArrayWrapper) args[1];
        callable = () -> dataCommands.append(key, valueToAdd);
        break;
      }
      case GET: {
        callable = () -> dataCommands.get(key);
        break;
      }
      case MGET: {
        callable = () -> dataCommands.mget(key);
        break;
      }
      case STRLEN: {
        callable = () -> dataCommands.strlen(key);
        break;
      }
      case SET: {
        Object[] argArgs = (Object[]) args[1];
        ByteArrayWrapper value = (ByteArrayWrapper) argArgs[0];
        SetOptions options = (SetOptions) argArgs[1];
        callable = () -> dataCommands.set(key, value, options);
        break;
      }
      case GETSET: {
        ByteArrayWrapper value = (ByteArrayWrapper) args[1];
        callable = () -> dataCommands.getset(key, value);
        break;
      }
      case GETRANGE: {
        Object[] argArgs = (Object[]) args[1];
        long start = (long) argArgs[0];
        long end = (long) argArgs[1];
        callable = () -> dataCommands.getrange(key, start, end);
        break;
      }
      case SETRANGE: {
        Object[] argArgs = (Object[]) args[1];
        int offset = (int) argArgs[0];
        byte[] value = (byte[]) argArgs[1];
        callable =
            () -> dataCommands.setrange(key, offset, value);
        break;
      }
      case BITCOUNT: {
        Object[] argArgs = (Object[]) args[1];
        if (argArgs == null) {
          callable = () -> dataCommands.bitcount(key);
        } else {
          int start = (int) argArgs[0];
          int end = (int) argArgs[1];
          callable =
              () -> dataCommands.bitcount(key, start, end);
        }
        break;
      }
      case BITPOS: {
        Object[] argArgs = (Object[]) args[1];
        int bit = (int) argArgs[0];
        int start = (int) argArgs[1];
        Integer end = (Integer) argArgs[2];
        callable =
            () -> dataCommands.bitpos(key, bit, start, end);
        break;
      }
      case GETBIT: {
        int offset = (int) args[1];
        callable = () -> dataCommands.getbit(key, offset);
        break;
      }
      case SETBIT: {
        Object[] argArgs = (Object[]) args[1];
        long offset = (long) argArgs[0];
        int value = (int) argArgs[1];
        callable =
            () -> dataCommands.setbit(key, offset, value);
        break;
      }
      case BITOP: {
        Object[] argArgs = (Object[]) args[1];
        String operation = (String) argArgs[0];
        List<ByteArrayWrapper> sources = (List<ByteArrayWrapper>) argArgs[1];
        callable = () -> dataCommands.bitop(operation, key, sources);
        useStripedExecutor = false;
        break;
      }
      case INCR:
        callable = () -> dataCommands.incr(key);
        break;
      case DECR:
        callable = () -> dataCommands.decr(key);
        break;
      case INCRBY: {
        long increment = (long) args[1];
        callable = () -> dataCommands.incrby(key, increment);
        break;
      }
      case INCRBYFLOAT: {
        double increment = (double) args[1];
        callable =
            () -> dataCommands.incrbyfloat(key, increment);
        break;
      }
      case DECRBY: {
        long decrement = (long) args[1];
        callable = () -> dataCommands.decrby(key, decrement);
        break;
      }
      case SADD: {
        ArrayList<ByteArrayWrapper> membersToAdd = (ArrayList<ByteArrayWrapper>) args[1];
        callable = () -> dataCommands.sadd(key, membersToAdd);
        break;
      }
      case SREM: {
        ArrayList<ByteArrayWrapper> membersToRemove = (ArrayList<ByteArrayWrapper>) args[1];
        callable = () -> dataCommands.srem(key, membersToRemove);
        break;
      }
      case SMEMBERS:
        callable = () -> dataCommands.smembers(key);
        break;
      case SCARD:
        callable = () -> dataCommands.scard(key);
        break;
      case SISMEMBER: {
        ByteArrayWrapper member = (ByteArrayWrapper) args[1];
        callable = () -> dataCommands.sismember(key, member);
        break;
      }
      case SRANDMEMBER: {
        int count = (int) args[1];
        callable = () -> dataCommands.srandmember(key, count);
        break;
      }
      case SPOP: {
        int popCount = (int) args[1];
        callable = () -> dataCommands.spop(key, popCount);
        break;
      }
      case SSCAN: {
        Pattern matchPattern = (Pattern) args[0];
        int count = (int) args[1];
        int cursor = (int) args[2];
        callable =
            () -> dataCommands.sscan(key, matchPattern, count,
                cursor);
        break;
      }
      case SUNIONSTORE: {
        ArrayList<ByteArrayWrapper> setKeys = (ArrayList<ByteArrayWrapper>) args[1];
        callable = () -> dataCommands.sunionstore(key, setKeys);
        useStripedExecutor = false;
        break;
      }
      case SINTERSTORE: {
        ArrayList<ByteArrayWrapper> setKeys = (ArrayList<ByteArrayWrapper>) args[1];
        callable =
            () -> dataCommands.sinterstore(key, setKeys);
        useStripedExecutor = false;
        break;
      }
      case SDIFFSTORE: {
        ArrayList<ByteArrayWrapper> setKeys = (ArrayList<ByteArrayWrapper>) args[1];
        callable =
            () -> dataCommands.sdiffstore(key, setKeys);
        useStripedExecutor = false;
        break;
      }
      case HSET: {
        Object[] hsetArgs = (Object[]) args[1];
        List<ByteArrayWrapper> fieldsToSet = (List<ByteArrayWrapper>) hsetArgs[0];
        boolean NX = (boolean) hsetArgs[1];
        callable = () -> dataCommands.hset(key, fieldsToSet, NX);
        break;
      }
      case HDEL: {
        List<ByteArrayWrapper> fieldsToRemove = (List<ByteArrayWrapper>) args[1];
        callable = () -> dataCommands.hdel(key, fieldsToRemove);
        break;
      }
      case HGETALL: {
        callable = () -> dataCommands.hgetall(key);
        break;
      }
      case HEXISTS: {
        ByteArrayWrapper field = (ByteArrayWrapper) args[1];
        callable = () -> dataCommands.hexists(key, field);
        break;
      }
      case HGET: {
        ByteArrayWrapper field = (ByteArrayWrapper) args[1];
        callable = () -> dataCommands.hget(key, field);
        break;
      }
      case HLEN: {
        callable = () -> dataCommands.hlen(key);
        break;
      }
      case HSTRLEN: {
        ByteArrayWrapper field = (ByteArrayWrapper) args[1];
        callable = () -> dataCommands.hstrlen(key, field);
        break;
      }
      case HMGET: {
        List<ByteArrayWrapper> fields = (List<ByteArrayWrapper>) args[1];
        callable = () -> dataCommands.hmget(key, fields);
        break;
      }
      case HVALS: {
        callable = () -> dataCommands.hvals(key);
        break;
      }
      case HKEYS: {
        callable = () -> dataCommands.hkeys(key);
        break;
      }
      case HSCAN: {
        Object[] hsetArgs = (Object[]) args[1];
        Pattern pattern = (Pattern) hsetArgs[0];
        int count = (int) hsetArgs[1];
        int cursor = (int) hsetArgs[2];
        callable =
            () -> dataCommands.hscan(key, pattern, count, cursor);
        break;
      }
      case HINCRBY: {
        Object[] hsetArgs = (Object[]) args[1];
        ByteArrayWrapper field = (ByteArrayWrapper) hsetArgs[0];
        long increment = (long) hsetArgs[1];
        callable =
            () -> dataCommands.hincrby(key, field, increment);
        break;
      }
      case HINCRBYFLOAT: {
        Object[] hsetArgs = (Object[]) args[1];
        ByteArrayWrapper field = (ByteArrayWrapper) hsetArgs[0];
        double increment = (double) hsetArgs[1];
        callable = () -> dataCommands.hincrbyfloat(key, field,
            increment);
        break;
      }
      default:
        throw new UnsupportedOperationException(ID + " does not yet support " + command);
    }
    if (useStripedExecutor) {
      return dataCommands.getStripedExecutor().execute(key, callable);
    } else {
      try {
        return callable.call();
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
