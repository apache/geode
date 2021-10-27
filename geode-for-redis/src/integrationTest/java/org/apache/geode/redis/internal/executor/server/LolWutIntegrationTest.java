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

import static org.apache.geode.distributed.ConfigurationProperties.GEODE_FOR_REDIS_REPLICA_COUNT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;

import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class LolWutIntegrationTest implements RedisIntegrationTest {
  private static final int REDIS_CLIENT_TIMEOUT = RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
  private static final String GEODE_VERSION_STRING = KnownVersion.getCurrentVersion().toString();
  private static final int DEFAULT_MAZE_HEIGHT = 10 + 2; // Top & bottom walls, version string
  private static final int MAX_MAZE_HEIGHT = (1024 * 1024) + 2;
  private Jedis jedis;

  @ClassRule
  public static GeodeRedisServerRule server =
      new GeodeRedisServerRule().withProperty(GEODE_FOR_REDIS_REPLICA_COUNT, "0")
          .withProperty(LOG_LEVEL, "info");

  @Override
  public int getPort() {
    return server.getPort();
  }

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void shouldReturnGeodeVersion() {
    String actualResult =
        Coder.bytesToString((byte[]) jedis.sendCommand(() -> SafeEncoder.encode("lolwut")));

    assertThat(actualResult).contains(GEODE_VERSION_STRING);
  }

  @Test
  public void shouldReturnDefaultMazeSize_givenNoArgs() {
    String actualResult =
        Coder.bytesToString((byte[]) jedis.sendCommand(() -> SafeEncoder.encode("lolwut")));

    String[] lines = actualResult.split("\\n");
    assertThat(lines.length).isEqualTo(DEFAULT_MAZE_HEIGHT);
  }


  @Test
  public void shouldReturnSpecifiedMazeWidth_givenNumericArg() {
    String actualResult = Coder.bytesToString(
        (byte[]) jedis.sendCommand(() -> SafeEncoder.encode("lolwut"),
            SafeEncoder.encode("20")));

    String[] lines = actualResult.split("\\n");
    assertThat(lines[0].length()).isEqualTo((20 - 1) * 2);
  }

  @Test
  public void shouldReturnSpecifiedMazeHeight_givenNumericArgs() {
    String actualResult = Coder.bytesToString(
        (byte[]) jedis.sendCommand(() -> SafeEncoder.encode("lolwut"),
            SafeEncoder.encode("10"),
            SafeEncoder.encode("20")));

    String[] lines = actualResult.split("\\n");
    assertThat(lines.length).isEqualTo(20 + 2);
  }

  @Test
  public void shouldLimitMazeWidth() {
    String actualResult = Coder.bytesToString(
        (byte[]) jedis.sendCommand(() -> SafeEncoder.encode("lolwut"),
            SafeEncoder.encode("2048")));

    String[] lines = actualResult.split("\\n");
    assertThat(lines[0].length()).isEqualTo((1024 - 1) * 2);
  }

  @Test
  public void shouldLimitMazeHeight() {
    String actualResult = Coder.bytesToString(
        (byte[]) jedis.sendCommand(() -> SafeEncoder.encode("lolwut"),
            SafeEncoder.encode("10"),
            SafeEncoder.encode(Integer.toString(MAX_MAZE_HEIGHT * 2))));

    String[] lines = actualResult.split("\\n");
    assertThat(lines.length).isEqualTo(MAX_MAZE_HEIGHT);
  }

  @Test
  public void shouldNotError_givenVersionArg() {
    String actualResult = Coder.bytesToString(
        (byte[]) jedis.sendCommand(() -> SafeEncoder.encode("lolwut"),
            SafeEncoder.encode("version"),
            SafeEncoder.encode("ignored")));

    assertThat(actualResult).contains(GEODE_VERSION_STRING);
  }

  @Test
  public void shouldError_givenNonNumericArg() {
    assertThatThrownBy(() -> jedis.sendCommand(() -> SafeEncoder.encode("lolwut"),
        SafeEncoder.encode("notEvenCloseToANumber")))
            .hasMessage("ERR value is not an integer or out of range");
  }

}
