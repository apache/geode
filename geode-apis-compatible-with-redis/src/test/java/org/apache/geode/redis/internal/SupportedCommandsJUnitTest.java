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
      "GETRANGE",
      "HGET",
      "HDEL",
      "HEXISTS",
      "HGETALL",
      "HMSET",
      "HSET",
      "HSETNX",
      "HLEN",
      "HSCAN",
      "HMGET",
      "HSTRLEN",
      "HINCRBY",
      "HINCRBYFLOAT",
      "HVALS",
      "HKEYS",
      "INCR",
      "INCRBY",
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
      "SETNX",
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
      "GETSET",
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
    implementedCommands.removeIf(RedisCommandType::isInternal);
    return implementedCommands;
  }

  private List<RedisCommandType> getAllInternalCommands() {
    List<RedisCommandType> internalCommands = new ArrayList<>(asList(RedisCommandType.values()));
    internalCommands.removeIf(RedisCommandType::isSupported);
    internalCommands.removeIf(RedisCommandType::isUnsupported);

    return internalCommands;
  }
}
