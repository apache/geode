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

package org.apache.geode.redis.internal.commands.executor.server;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_COMMAND_COMMAND_SUBCOMMAND;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.NativeRedisTestRule;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.redis.internal.commands.RedisCommandType;

public class CommandIntegrationTest {

  @ClassRule
  public static NativeRedisTestRule redisServer = new NativeRedisTestRule();

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
  public void teardown() {
    radishServer.setEnableUnsupportedCommands(true);
  }

  @Test
  public void commandReturnsResultsMatchingNativeRedis() {
    Map<String, CommandStructure> goldenResults = processRawCommands(redisClient.command());
    Map<String, CommandStructure> results = processRawCommands(radishClient.command());

    List<String> commands = new ArrayList<>(results.keySet());
    Collections.sort(commands);

    SoftAssertions softly = new SoftAssertions();
    for (String command : commands) {
      // TODO: remove special case once LPOP implements 6.2+ semantics
      if (command.equalsIgnoreCase("LPOP")) {
        continue;
      }
      softly.assertThatCode(() -> compareCommands(results.get(command), goldenResults.get(command)))
          .as("command: " + command)
          .doesNotThrowAnyException();
    }
    softly.assertAll();
  }

  @Test
  public void commandWithInvalidSubcommand_returnCommandError() {
    String invalidSubcommand = "fakeSubcommand";
    RedisCodec<String, String> codec = StringCodec.UTF8;

    CommandArgs<String, String> args =
        new CommandArgs<>(codec).add(CommandType.COMMAND).add(invalidSubcommand);
    assertThatThrownBy(
        () -> radishClient.dispatch(CommandType.COMMAND, new NestedMultiOutput<>(codec), args))
            .hasMessageContaining(
                String.format(ERROR_UNKNOWN_COMMAND_COMMAND_SUBCOMMAND, CommandType.COMMAND));
  }

  @Test
  public void commandDoesNotReturnUnsupported_whenUnsupportedCommandsAreDisabled() {
    radishServer.setEnableUnsupportedCommands(false);
    Map<String, CommandStructure> results = processRawCommands(radishClient.command());

    // Find an unsupported command
    RedisCommandType someUnsupported = Arrays.stream(RedisCommandType.values())
        .filter(RedisCommandType::isUnsupported).findFirst()
        .orElseThrow(() -> new AssertionError("Could not find any UNSUPPORTED commands"));

    for (CommandStructure meta : results.values()) {
      assertThat(meta.name).isNotEqualToIgnoringCase(someUnsupported.name());
    }
  }

  private void compareCommands(CommandStructure actual, CommandStructure expected) {
    assertThat(actual).as("no metadata for " + expected.name).isNotNull();
    SoftAssertions softly = new SoftAssertions();
    softly.assertThat(actual.arity).as(expected.name + ".arity").isEqualTo(expected.arity);
    softly.assertThat(actual.flags).as(expected.name + ".flags")
        .containsExactlyInAnyOrderElementsOf(expected.flags);
    softly.assertThat(actual.firstKey).as(expected.name + ".firstKey").isEqualTo(expected.firstKey);
    softly.assertThat(actual.lastKey).as(expected.name + ".lastKey").isEqualTo(expected.lastKey);
    softly.assertThat(actual.stepCount).as(expected.name + ".stepCount")
        .isEqualTo(expected.stepCount);
    if (!actual.categories.get(0).equals("@uncategorized")) {
      softly.assertThat(actual.categories.get(0)).as(expected.name + ".category")
          .isIn(expected.categories);
    } else {
      softly.assertThat(actual.name)
          .as("command " + actual.name + " is categorized as UNCATEGORIZED")
          .isIn("info", "lolwut", "time");
    }
    softly.assertAll();
  }

  @SuppressWarnings("unchecked")
  private Map<String, CommandStructure> processRawCommands(List<Object> rawCommands) {
    Map<String, CommandStructure> commands = new HashMap<>();

    for (Object rawEntry : rawCommands) {
      List<Object> entry = (List<Object>) rawEntry;
      String key = (String) entry.get(0);

      CommandStructure cmd = new CommandStructure(
          key,
          (long) entry.get(1),
          (List<String>) entry.get(2),
          (long) entry.get(3),
          (long) entry.get(4),
          (long) entry.get(5),
          (List<String>) entry.get(6));

      commands.put(key, cmd);
    }

    return commands;
  }

  private static class CommandStructure {
    final String name;
    final long arity;
    final long firstKey;
    final List<String> flags;
    final long lastKey;
    final long stepCount;
    final List<String> categories;

    public CommandStructure(String name, long arity, List<String> flags, long firstKey,
        long lastKey, long stepCount, List<String> categories) {
      this.name = name;
      this.arity = arity;
      this.flags = flags;
      this.firstKey = firstKey;
      this.lastKey = lastKey;
      this.stepCount = stepCount;
      this.categories = categories;
    }
  }
}
