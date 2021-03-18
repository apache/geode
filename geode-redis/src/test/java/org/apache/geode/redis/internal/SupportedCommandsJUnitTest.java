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

package org.apache.geode.redis.internal;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.geode.redis.internal.netty.Command;

public class SupportedCommandsJUnitTest {

  private final String[] supportedCommands = new String[] {
      "APPEND",
      "AUTH",
      "DECR",
      "DECRBY",
      "DEL",
      "EXISTS",
      "EXPIRE",
      "EXPIREAT",
      "GET",
      "HGET",
      "HDEL",
      "HEXISTS",
      "HGETALL",
      "HINCRBYFLOAT",
      "HMSET",
      "HSET",
      "HSETNX",
      "HLEN",
      "HSCAN",
      "HMGET",
      "HSTRLEN",
      "HINCRBY",
      "HVALS",
      "HKEYS",
      "INCRBYFLOAT",
      "INFO",
      "KEYS",
      "MGET",
      "PERSIST",
      "PEXPIRE",
      "PEXPIREAT",
      "PING",
      "PSUBSCRIBE",
      "PTTL",
      "PUBLISH",
      "PUNSUBSCRIBE",
      "QUIT",
      "RENAME",
      "SADD",
      "SET",
      "SLOWLOG",
      "SMEMBERS",
      "SREM",
      "STRLEN",
      "SUBSCRIBE",
      "TTL",
      "TYPE",
      "UNSUBSCRIBE",
  };

  private final String[] unSupportedCommands = new String[] {
      "BITCOUNT",
      "BITOP",
      "BITPOS",
      "DBSIZE",
      "ECHO",
      "FLUSHALL",
      "FLUSHDB",
      "GETBIT",
      "GETRANGE",
      "GETSET",
      "INCR",
      "INCRBY",
      "MSET",
      "MSETNX",
      "PSETEX",
      "SCAN",
      "SCARD",
      "SDIFF",
      "SDIFFSTORE",
      "SELECT",
      "SETBIT",
      "SETEX",
      "SETNX",
      "SETRANGE",
      "SHUTDOWN",
      "SINTER",
      "SINTERSTORE",
      "SISMEMBER",
      "SMOVE",
      "SPOP",
      "SRANDMEMBER",
      "SSCAN",
      "SUNION",
      "SUNIONSTORE",
      "TIME",
      "UNLINK",
  };

  private final String[] unImplementedCommands = new String[] {
      "ACL",
      "BGREWRITEAOF",
      "BGSAVE",
      "BITFIELD",
      "BLPOP",
      "BRPOP",
      "BRPOPLPUSH",
      "BZPOPMIN",
      "BZPOPMAX",
      "CLIENT",
      "CLUSTER",
      "COMMAND",
      "CONFIG",
      "DEBUG",
      "DISCARD",
      "DUMP",
      "EVAL",
      "EVALSHA",
      "EXEC",
      "GEOADD",
      "GEOHASH",
      "GEOPOS",
      "GEODIST",
      "GEORADIUS",
      "GEORADIUSBYMEMBER",
      "LATENCY",
      "LASTSAVE",
      "LINDEX",
      "LINSERT",
      "LLEN",
      "LOLWUT",
      "LPOP",
      "LPUSH",
      "LPUSHX",
      "LRANGE",
      "LREM",
      "LSET",
      "LTRIM",
      "MEMORY",
      "MIGRATE",
      "MODULE",
      "MONITOR",
      "MOVE",
      "MULTI",
      "OBJECT",
      "PFADD",
      "PFCOUNT",
      "PFMERGE",
      "PSYNC",
      "PUBSUB",
      "RANDOMKEY",
      "READONLY",
      "READWRITE",
      "RENAMENX",
      "RESTORE",
      "ROLE",
      "RPOP",
      "RPOPLPUSH",
      "RPUSH",
      "RPUSHX",
      "SAVE",
      "SCRIPT",
      "SLAVEOF",
      "REPLICAOF",
      "SORT",
      "STRALGO",
      "SWAPDB",
      "SYNC",
      "TOUCH",
      "UNWATCH",
      "WAIT",
      "WATCH",
      "XINFO",
      "XADD",
      "XTRIM",
      "XDEL",
      "XRANGE",
      "XREVRANGE",
      "XLEN",
      "XREAD",
      "XGROUP",
      "XREADGROUP",
      "XACK",
      "XCLAIM",
      "XPENDING",
      "ZADD",
      "ZCARD",
      "ZCOUNT",
      "ZINCRBY",
      "ZINTERSTORE",
      "ZLEXCOUNT",
      "ZPOPMAX",
      "ZPOPMIN",
      "ZRANGE",
      "ZRANGEBYLEX",
      "ZREVRANGEBYLEX",
      "ZRANGEBYSCORE",
      "ZRANK",
      "ZREM",
      "ZREMRANGEBYLEX",
      "ZREMRANGEBYRANK",
      "ZREMRANGEBYSCORE",
      "ZREVRANGE",
      "ZREVRANGEBYSCORE",
      "ZREVRANK",
      "ZSCORE",
      "ZUNIONSCORE",
      "ZSCAN"
  };

  private final String[] unknownCommands = new String[] {
      "UNKNOWN"
  };

  private final String[] internalCommands = new String[] {
      "INTERNALPTTL",
      "INTERNALSMEMBERS",
      "INTERNALTYPE"
  };

  @Test
  public void crossCheckAllUnsupportedCommands_areMarkedUnsupported() {
    for (String commandName : unSupportedCommands) {
      List<byte[]> args = new ArrayList<>();
      args.add(commandName.getBytes());

      Command command = new Command(args);

      assertThat(command.isSupported())
          .as("Command " + commandName + " should be unsupported")
          .isFalse();
    }
  }

  @Test
  public void crossCheckAllSupportedCommands_areMarkedSupported() {
    for (String commandName : supportedCommands) {
      List<byte[]> args = new ArrayList<>();
      args.add(commandName.getBytes());

      Command command = new Command(args);

      assertThat(command.isSupported())
          .as("Command " + commandName + " should be supported")
          .isTrue();
    }
  }

  @Test
  public void crossCheckAllUnimplementedCommands_areMarkedUnimplemented() {
    for (String commandName : unImplementedCommands) {
      List<byte[]> args = new ArrayList<>();
      args.add(commandName.getBytes());

      Command command = new Command(args);

      assertThat(command.isUnimplemented())
          .as("Command " + commandName + " should be unimplemented")
          .isTrue();
    }
  }

  @Test
  public void checkAllImplementedCommands_areIncludedInBothSupportedAndUnsupportedLists() {
    List<String> allCommands = new ArrayList<>(asList(supportedCommands));
    allCommands.addAll(asList(unSupportedCommands));

    List<String> implementedCommands = getAllImplementedCommands().stream()
        .filter(c -> !c.isUnknown())
        .map(Enum::name).collect(Collectors.toList());

    assertThat(implementedCommands).containsExactlyInAnyOrderElementsOf(allCommands);
  }

  @Test
  public void checkAllDefinedCommands_areIncludedInAllLists() {
    List<String> allCommands = new ArrayList<>(asList(supportedCommands));
    allCommands.addAll(asList(unSupportedCommands));
    allCommands.addAll(asList(unImplementedCommands));
    allCommands.addAll(asList(unknownCommands));
    allCommands.addAll(asList(internalCommands));

    List<String> definedCommands =
        Arrays.stream(RedisCommandType.values()).map(Enum::name).collect(Collectors.toList());

    assertThat(definedCommands).containsExactlyInAnyOrderElementsOf(allCommands);
  }

  @Test
  public void checkAllInternalCommands_areIncludedInInternalLists() {
    List<String> allInternalCommands = new ArrayList<>(asList(internalCommands));

    List<String> internalCommands = getAllInternalCommands().stream()
        .filter(c -> !c.isUnknown())
        .map(Enum::name).collect(Collectors.toList());

    assertThat(internalCommands).containsExactlyInAnyOrderElementsOf(allInternalCommands);
  }

  private List<RedisCommandType> getAllImplementedCommands() {
    List<RedisCommandType> implementedCommands = new ArrayList<>(asList(RedisCommandType.values()));
    implementedCommands.removeIf(RedisCommandType::isUnimplemented);
    implementedCommands.removeIf(RedisCommandType::isInternal);
    return implementedCommands;
  }

  private List<RedisCommandType> getAllInternalCommands() {
    List<RedisCommandType> internalCommands = new ArrayList<>(asList(RedisCommandType.values()));
    internalCommands.removeIf(RedisCommandType::isUnimplemented);
    internalCommands.removeIf(RedisCommandType::isSupported);
    internalCommands.removeIf(RedisCommandType::isUnsupported);

    return internalCommands;
  }
}
