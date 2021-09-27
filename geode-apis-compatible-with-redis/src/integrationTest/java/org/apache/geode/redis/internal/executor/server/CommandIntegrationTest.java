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

package org.apache.geode.redis.internal.executor.server;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.NativeRedisTestRule;
import org.apache.geode.redis.GeodeRedisServerRule;

public class CommandIntegrationTest {

  @ClassRule
  public static NativeRedisTestRule redisServer = new NativeRedisTestRule("redis:6.2.4");

  @ClassRule
  public static GeodeRedisServerRule radishServer = new GeodeRedisServerRule();

  private RedisCommands<String, String> redisClient;
  private RedisCommands<String, String> radishClient;

  @Before
  public void setup() {
    redisClient =
        RedisClient.create(String.format("redis://%s:%d", BIND_ADDRESS, redisServer.getPort()))
            .connect().sync();

    radishClient =
        RedisClient.create(String.format("redis://%s:%d", BIND_ADDRESS, radishServer.getPort()))
            .connect().sync();
  }

  @After
  public void teardown() {}

  @Test
  public void commandReturnsResultsMatchingNativeRedis() {
    Map<String, CommandStructure> goldenResults = processRawCommands(redisClient.command());
    Map<String, CommandStructure> results = processRawCommands(radishClient.command());

    List<String> commands = new ArrayList<>(results.keySet());
    Collections.sort(commands);

    SoftAssertions softly = new SoftAssertions();
    for (String command : commands) {
      softly.assertThatCode(() -> compareCommands(results.get(command), goldenResults.get(command)))
          .as("command: " + command)
          .doesNotThrowAnyException();
    }
    softly.assertAll();
  }

  private void compareCommands(CommandStructure actual, CommandStructure expected) {
    assertThat(actual).as("no metadata for " + expected.name).isNotNull();
    SoftAssertions softly = new SoftAssertions();
    softly.assertThat(actual.arity).as(expected.name + ".arity").isEqualTo(expected.arity);
    softly.assertThat(actual.firstKey).as(expected.name + ".firstKey").isEqualTo(expected.firstKey);
    softly.assertThat(actual.lastKey).as(expected.name + ".lastKey").isEqualTo(expected.lastKey);
    softly.assertThat(actual.stepCount).as(expected.name + ".stepCount")
        .isEqualTo(expected.stepCount);
    softly.assertAll();
  }

  private Map<String, CommandStructure> processRawCommands(List<Object> rawCommands) {
    Map<String, CommandStructure> commands = new HashMap<>();

    for (Object rawEntry : rawCommands) {
      List<Object> entry = (List<Object>) rawEntry;
      String key = (String) entry.get(0);
      CommandStructure cmd = new CommandStructure(
          key,
          (Long) entry.get(1),
          (Long) entry.get(3),
          (Long) entry.get(4),
          (Long) entry.get(5));

      commands.put(key, cmd);
    }

    return commands;
  }

  private static class CommandStructure {
    String name;
    long arity;
    long firstKey;
    long lastKey;
    long stepCount;

    public CommandStructure(String name, long arity, long firstKey, long lastKey, long stepCount) {
      this.name = name;
      this.arity = arity;
      this.firstKey = firstKey;
      this.lastKey = lastKey;
      this.stepCount = stepCount;
    }
  }
}
