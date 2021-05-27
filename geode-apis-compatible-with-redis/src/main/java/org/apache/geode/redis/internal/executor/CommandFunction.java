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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.data.CommandHelper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisHashCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisKeyCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisSetCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisSortedSetCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisStringCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZAddOptions;
import org.apache.geode.redis.internal.executor.string.SetOptions;
import org.apache.geode.redis.internal.statistics.RedisStats;

public class CommandFunction extends SingleResultRedisFunction {

  public static final String ID = "REDIS_COMMAND_FUNCTION";
  private static final long serialVersionUID = -1302506316316454732L;

  private final transient RedisKeyCommandsFunctionExecutor keyCommands;
  private final transient RedisHashCommandsFunctionExecutor hashCommands;
  private final transient RedisSetCommandsFunctionExecutor setCommands;
  private final transient RedisStringCommandsFunctionExecutor stringCommands;
  private final transient RedisSortedSetCommandsFunctionExecutor sortedSetCommands;

  public static void register(Region<RedisKey, RedisData> dataRegion,
      StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    FunctionService.registerFunction(new CommandFunction(dataRegion, stripedExecutor, redisStats));
  }

  public static Throwable getInitialCause(FunctionException ex) {
    Throwable result = ex.getCause();
    while (result != null && result.getCause() != null) {
      result = result.getCause();
    }
    if (result == null) {
      if (!ex.getExceptions().isEmpty()) {
        result = ex.getExceptions().get(0);
      }
    }
    return result;
  }

  public CommandFunction(Region<RedisKey, RedisData> dataRegion,
      StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    super(dataRegion);
    CommandHelper helper = new CommandHelper(dataRegion, redisStats, stripedExecutor);
    keyCommands = new RedisKeyCommandsFunctionExecutor(helper);
    hashCommands = new RedisHashCommandsFunctionExecutor(helper);
    setCommands = new RedisSetCommandsFunctionExecutor(helper);
    stringCommands = new RedisStringCommandsFunctionExecutor(helper);
    sortedSetCommands = new RedisSortedSetCommandsFunctionExecutor(helper);
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Object compute(RedisKey key, Object[] args) {
    RedisCommandType command = (RedisCommandType) args[0];
    switch (command) {
      case DEL:
        return keyCommands.del(key);
      case EXISTS:
        return keyCommands.exists(key);
      case TYPE:
        return keyCommands.type(key);
      case INTERNALTYPE:
        return keyCommands.internalType(key);
      case PEXPIREAT: {
        long timestamp = (long) args[1];
        return keyCommands.pexpireat(key, timestamp);
      }
      case PERSIST:
        return keyCommands.persist(key);
      case PTTL:
        return keyCommands.pttl(key);
      case INTERNALPTTL:
        return keyCommands.internalPttl(key);
      case APPEND: {
        byte[] valueToAdd = (byte[]) args[1];
        return stringCommands.append(key, valueToAdd);
      }
      case GET:
        return stringCommands.get(key);
      case MGET:
        return stringCommands.mget(key);
      case STRLEN:
        return stringCommands.strlen(key);
      case SET: {
        byte[] value = (byte[]) args[1];
        SetOptions options = (SetOptions) args[2];
        return stringCommands.set(key, value, options);
      }
      case GETSET: {
        byte[] value = (byte[]) args[1];
        return stringCommands.getset(key, value);
      }
      case GETRANGE: {
        long start = (long) args[1];
        long end = (long) args[2];
        return stringCommands.getrange(key, start, end);
      }
      case SETRANGE: {
        int offset = (int) args[1];
        byte[] value = (byte[]) args[2];
        return stringCommands.setrange(key, offset, value);
      }
      case BITCOUNT: {
        if (args.length == 1) {
          return stringCommands.bitcount(key);
        } else {
          int start = (int) args[1];
          int end = (int) args[2];
          return stringCommands.bitcount(key, start, end);
        }
      }
      case BITPOS: {
        int bit = (int) args[1];
        int start = (int) args[2];
        Integer end = (Integer) args[3];
        return stringCommands.bitpos(key, bit, start, end);
      }
      case GETBIT: {
        int offset = (int) args[1];
        return stringCommands.getbit(key, offset);
      }
      case SETBIT: {
        long offset = (long) args[1];
        int value = (int) args[2];
        return stringCommands.setbit(key, offset, value);
      }
      case BITOP: {
        String operation = (String) args[1];
        List<RedisKey> sources = (List<RedisKey>) args[2];
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
        BigDecimal increment = (BigDecimal) args[1];
        return stringCommands.incrbyfloat(key, increment);
      }
      case DECRBY: {
        long decrement = (long) args[1];
        return stringCommands.decrby(key, decrement);
      }
      case SADD: {
        List<byte[]> membersToAdd = (List<byte[]>) args[1];
        return setCommands.sadd(key, membersToAdd);
      }
      case SREM: {
        List<byte[]> membersToRemove = (List<byte[]>) args[1];
        return setCommands.srem(key, membersToRemove);
      }
      case SMEMBERS:
        return setCommands.smembers(key);
      case INTERNALSMEMBERS:
        return setCommands.internalsmembers(key);
      case SCARD:
        return setCommands.scard(key);
      case SISMEMBER: {
        byte[] member = (byte[]) args[1];
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
        Pattern matchPattern = (Pattern) args[1];
        int count = (int) args[2];
        BigInteger cursor = (BigInteger) args[3];
        return setCommands.sscan(key, matchPattern, count, cursor);
      }
      case SUNIONSTORE: {
        List<RedisKey> setKeys = (List<RedisKey>) args[1];
        return setCommands.sunionstore(key, setKeys);
      }
      case SINTERSTORE: {
        List<RedisKey> setKeys = (List<RedisKey>) args[1];
        return setCommands.sinterstore(key, setKeys);
      }
      case SDIFFSTORE: {
        List<RedisKey> setKeys = (List<RedisKey>) args[1];
        return setCommands.sdiffstore(key, setKeys);
      }
      case HSET: {
        List<byte[]> fieldsToSet = (List<byte[]>) args[1];
        boolean NX = (boolean) args[2];
        return hashCommands.hset(key, fieldsToSet, NX);
      }
      case HDEL: {
        List<byte[]> fieldsToRemove = (List<byte[]>) args[1];
        return hashCommands.hdel(key, fieldsToRemove);
      }
      case HGETALL:
        return hashCommands.hgetall(key);
      case HEXISTS: {
        byte[] field = (byte[]) args[1];
        return hashCommands.hexists(key, field);
      }
      case HGET: {
        byte[] field = (byte[]) args[1];
        return hashCommands.hget(key, field);
      }
      case HLEN:
        return hashCommands.hlen(key);
      case HSTRLEN: {
        byte[] field = (byte[]) args[1];
        return hashCommands.hstrlen(key, field);
      }
      case HMGET: {
        List<byte[]> fields = (List<byte[]>) args[1];
        return hashCommands.hmget(key, fields);
      }
      case HVALS:
        return hashCommands.hvals(key);
      case HKEYS:
        return hashCommands.hkeys(key);
      case HSCAN: {
        Pattern pattern = (Pattern) args[1];
        int count = (int) args[2];
        int cursor = (int) args[3];
        return hashCommands.hscan(key, pattern, count, cursor);
      }
      case HINCRBY: {
        byte[] field = (byte[]) args[1];
        long increment = (long) args[2];
        return hashCommands.hincrby(key, field, increment);
      }
      case HINCRBYFLOAT: {
        byte[] field = (byte[]) args[1];
        BigDecimal increment = (BigDecimal) args[2];
        return hashCommands.hincrbyfloat(key, field, increment);
      }
      case ZADD: {
        List<byte[]> scoresAndMembersToAdd = (List<byte[]>) args[1];
        return sortedSetCommands.zadd(key, scoresAndMembersToAdd, (ZAddOptions) args[2]);
      }
      case ZSCORE: {
        byte[] member = (byte[]) args[1];
        return sortedSetCommands.zscore(key, member);
      }
      default:
        throw new UnsupportedOperationException(ID + " does not yet support " + command);
    }
  }

}
