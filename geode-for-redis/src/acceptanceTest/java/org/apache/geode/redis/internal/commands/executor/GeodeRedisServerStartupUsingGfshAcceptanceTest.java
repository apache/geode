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

package org.apache.geode.redis.internal.commands.executor;

import static org.apache.geode.redis.internal.RedisConfiguration.DEFAULT_REDIS_PORT;
import static org.apache.geode.redis.internal.SystemPropertyBasedRedisConfiguration.GEODE_FOR_REDIS_BIND_ADDRESS;
import static org.apache.geode.redis.internal.SystemPropertyBasedRedisConfiguration.GEODE_FOR_REDIS_ENABLED;
import static org.apache.geode.redis.internal.SystemPropertyBasedRedisConfiguration.GEODE_FOR_REDIS_PORT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.test.dunit.rules.RequiresRedisHome;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class GeodeRedisServerStartupUsingGfshAcceptanceTest {

  @Rule
  public RequiresRedisHome redisHome = new RequiresRedisHome();

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Test
  public void shouldReturnErrorMessage_givenSamePortAndAddress() throws IOException {

    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    String startServerCommand = String.join(" ",
        "start server",
        "--server-port", "0",
        "--name", "same-port-and-address-server",
        "--J=-D" + GEODE_FOR_REDIS_BIND_ADDRESS + "=localhost",
        "--J=-D" + GEODE_FOR_REDIS_PORT + "=" + port,
        "--classpath=" + redisHome.getGeodeForRedisHome() + "/lib/*");
    GfshExecution execution;

    try (ServerSocket interferingSocket = new ServerSocket()) {
      interferingSocket.bind(new InetSocketAddress("localhost", port));
      execution = GfshScript.of(startServerCommand)
          .expectFailure()
          .execute(gfshRule);
    }

    assertThat(execution.getOutputText()).containsIgnoringCase("address already in use");
  }

  @Test
  public void shouldReturnErrorMessage_givenSamePortAndAllAddresses() throws IOException {

    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    String startServerCommand = String.join(" ",
        "start server",
        "--server-port", "0",
        "--name", "same-port-all-addresses-server",
        "--J=-D" + GEODE_FOR_REDIS_BIND_ADDRESS + "=0.0.0.0",
        "--J=-D" + GEODE_FOR_REDIS_PORT + "=" + port,
        "--classpath=" + redisHome.getGeodeForRedisHome() + "/lib/*");
    GfshExecution execution;

    try (ServerSocket interferingSocket = new ServerSocket()) {
      interferingSocket.bind(new InetSocketAddress("0.0.0.0", port));
      execution = GfshScript.of(startServerCommand)
          .expectFailure()
          .execute(gfshRule);
    }

    assertThat(execution.getOutputText()).containsIgnoringCase("address already in use");
  }

  @Test
  public void shouldReturnErrorMessage_givenInvalidBindAddress() {

    String startServerCommand = String.join(" ",
        "start server",
        "--server-port", "0",
        "--name", "invalid-bind-server",
        "--J=-D" + GEODE_FOR_REDIS_BIND_ADDRESS + "=1.1.1.1",
        "--classpath=" + redisHome.getGeodeForRedisHome() + "/lib/*");
    GfshExecution execution;

    execution = GfshScript.of(startServerCommand)
        .expectFailure()
        .execute(gfshRule);

    assertThat(execution.getOutputText()).containsIgnoringCase(
        "The geode-for-redis-bind-address 1.1.1.1 is not a valid address for this machine");
  }

  @Test
  public void gfshStartsRedisServer_whenRedisEnabled() {
    String command = "start server --server-port=0 "
        + "--J=-D" + GEODE_FOR_REDIS_ENABLED + "=true",
        + " --classpath=" + redisHome.getGeodeForRedisHome() + "/lib/*");
    gfshRule.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, DEFAULT_REDIS_PORT)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshStartsRedisServer_whenCustomPort() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String command = "start server --server-port=0 "
        + " --J=-D" + GEODE_FOR_REDIS_PORT + "=" + port
        + " --classpath=" + redisHome.getGeodeForRedisHome() + "/lib/*";

    gfshRule.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, port)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshStartsRedisServer_whenCustomPortAndBindAddress() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    Set<InetAddress> myAddresses = LocalHostUtil.getMyAddresses();
    String anyLocal = myAddresses.stream().findFirst().get().getHostAddress();

    String command = "start server --server-port=0 "
        + " --J=-D" + GEODE_FOR_REDIS_PORT + "=" + port
        + " --J=-D" + GEODE_FOR_REDIS_BIND_ADDRESS + "=" + anyLocal
        + " --classpath=" + redisHome.getGeodeForRedisHome() + "/lib/*";

    gfshRule.execute(command);

    try (Jedis jedis = new Jedis(anyLocal, port)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshStartsRedisServer_whenAllBindAddress() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String anyLocal = LocalHostUtil.getAnyLocalAddress().getHostAddress();

    String command = "start server --server-port=0 "
        + " --J=-D" + GEODE_FOR_REDIS_PORT + "=" + port
        + " --J=-D" + GEODE_FOR_REDIS_BIND_ADDRESS + "=" + anyLocal;

    gfshRule.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, port)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshDoesNotStartRedisServer_whenRedisDisabled() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String command = "start server --server-port=0 "
        + "--J=-D" + GEODE_FOR_REDIS_ENABLED + "=false"
        + " --classpath=" + redisHome.getGeodeForRedisHome() + "/lib/*";

    gfshRule.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, port)) {
      assertThatThrownBy(() -> jedis.ping()).isInstanceOf(JedisConnectionException.class);
    }
  }

}
