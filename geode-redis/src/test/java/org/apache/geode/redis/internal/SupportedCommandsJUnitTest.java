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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

public class SupportedCommandsJUnitTest {

  private final String[] supportedCommands = new String[] {
      "APPEND",
      "AUTH",
      "DEL",
      "EXISTS",
      "EXPIRE",
      "EXPIREAT",
      "FLUSHALL",
      "GET",
      "HGETALL",
      "HMSET",
      "HSET",
      "KEYS",
      "PEXPIRE",
      "PEXPIREAT",
      "PING",
      "PSUBSCRIBE",
      "PUBLISH",
      "PUNSUBSCRIBE",
      "QUIT",
      "RENAME",
      "SADD",
      "SET",
      "SMEMBERS",
      "SREM",
      "SUBSCRIBE",
      "UNKNOWN",
      "UNSUBSCRIBE",
  };

  private final String[] unSupportedCommands = new String[] {
      "BITCOUNT",
      "BITOP",
      "BITPOS",
      "DBSIZE",
      "DECR",
      "DECRBY",
      "ECHO",
      "FLUSHDB",
      "GETBIT",
      "GETRANGE",
      "GETSET",
      "HDEL",
      "HEXISTS",
      "HGET",
      "HINCRBY",
      "HINCRBYFLOAT",
      "HKEYS",
      "HLEN",
      "HMGET",
      "HSCAN",
      "HSETNX",
      "HVALS",
      "INCR",
      "INCRBY",
      "INCRBYFLOAT",
      "MGET",
      "MSET",
      "MSETNX",
      "PERSIST",
      "PSETEX",
      "PTTL",
      "SCAN",
      "SCARD",
      "SDIFF",
      "SDIFFSTORE",
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
      "STRLEN",
      "SUNION",
      "SUNIONSTORE",
      "TIME",
      "TTL",
      "TYPE",
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
  public void checkAllDefinedCommands_areIncludedInBothLists() {
    List<String> allCommands = new ArrayList<>(Arrays.asList(supportedCommands));
    allCommands.addAll(Arrays.asList(unSupportedCommands));

    List<String> definedCommands =
        Arrays.stream(RedisCommandType.values()).map(Enum::name).collect(Collectors.toList());

    assertThat(definedCommands).containsExactlyInAnyOrderElementsOf(allCommands);
  }
}
