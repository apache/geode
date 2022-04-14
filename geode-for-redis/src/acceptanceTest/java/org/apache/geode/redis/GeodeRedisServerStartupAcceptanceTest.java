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

package org.apache.geode.redis;

import static org.apache.geode.redis.internal.SystemPropertyBasedRedisConfiguration.GEODE_FOR_REDIS_BIND_ADDRESS;
import static org.apache.geode.redis.internal.SystemPropertyBasedRedisConfiguration.GEODE_FOR_REDIS_ENABLED;
import static org.apache.geode.redis.internal.SystemPropertyBasedRedisConfiguration.GEODE_FOR_REDIS_PORT;
import static org.apache.geode.redis.internal.SystemPropertyBasedRedisConfiguration.GEODE_FOR_REDIS_REDUNDANT_COPIES;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.redis.internal.services.RegionProvider;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.categories.IgnoreInRepeatTestTasks;

public class GeodeRedisServerStartupAcceptanceTest {

  @Rule
  public RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @After
  public void cleanup() {
    VM.getVM(0).bounce();
  }

  @Category(IgnoreInRepeatTestTasks.class)
  @Test
  public void startupOnDefaultPort() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "6379")
        .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server)).isEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }

  @Test
  public void startupOnDefaultPort_whenPortIsNotSpecified() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server)).isEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }

  @Test
  public void startupOnRandomPort_whenPortIsZero() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "0")
        .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server))
        .isNotEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }

  @Test
  public void doNotStartup_whenRedisServiceIsNotEnabled() {
    MemberVM server = cluster.startServerVM(0);

    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getService(GeodeRedisService.class))
          .as("GeodeRedisService should not exist")
          .isNull();
    });
  }

  @Test
  public void startupFailsGivenIllegalPort() {
    assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "-1")
        .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true")))
            .hasStackTraceContaining(GEODE_FOR_REDIS_PORT + " is out of range");
  }

  @Test
  public void startupFailsGivenPortAlreadyInUse() throws Exception {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    addIgnoredException("Could not start server compatible with Redis");
    try (ServerSocket interferingSocket = new ServerSocket()) {
      interferingSocket.bind(new InetSocketAddress("localhost", port));

      assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
          .withSystemProperty(GEODE_FOR_REDIS_PORT, "" + port)
          .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, "localhost")
          .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true")))
              .hasRootCauseInstanceOf(BindException.class);
    }
  }

  @Test
  public void startupFailsGivenInvalidBindAddress() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    addIgnoredException("1.1.1.1 is not a valid address for this machine");
    assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "" + port)
        .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, "1.1.1.1")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true")))
            .hasStackTraceContaining(
                "The geode-for-redis-bind-address 1.1.1.1 is not a valid address for this machine");
  }

  @Test
  public void startupFailsGivenInvalidRedundantCopies() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    addIgnoredException("Redundant copies is out of range");
    assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "" + port)
        .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true")
        .withSystemProperty(GEODE_FOR_REDIS_REDUNDANT_COPIES, "4")))
            .hasStackTraceContaining(GEODE_FOR_REDIS_REDUNDANT_COPIES + " is out of range");
  }

  @Test
  public void startupFailsGivenNegativeRedundantCopies() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    addIgnoredException("Redundant copies is out of range");
    assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "" + port)
        .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true")
        .withSystemProperty(GEODE_FOR_REDIS_REDUNDANT_COPIES, "-1")))
            .hasStackTraceContaining(GEODE_FOR_REDIS_REDUNDANT_COPIES + " is out of range");
  }

  @Test
  public void startupOnSpecifiedPort() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "4242")
        .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server)).isEqualTo(4242);
  }

  @Test
  public void startupWorksGivenAnyLocalAddress() {
    String anyLocal = LocalHostUtil.getAnyLocalAddress().getHostAddress();
    MemberVM server = cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "0")
        .withSystemProperty(GEODE_FOR_REDIS_BIND_ADDRESS, anyLocal)
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server))
        .isNotEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }

  @Test
  public void startupWorksGivenNoBindAddress() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "0")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server))
        .isNotEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }

  @Test
  public void bindAddressDefaultsToServerBindAddress() throws Exception {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(DistributionConfig.SERVER_BIND_ADDRESS_NAME, "127.0.0.1")
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "0")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true"));

    int port = cluster.getRedisPort(server);
    try (Jedis jedis = new Jedis("127.0.0.1", port)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }

    String hostname = LocalHostUtil.getLocalHostName();
    try (Jedis jedis = new Jedis(hostname, port)) {
      assertThatThrownBy(jedis::ping)
          .hasStackTraceContaining("Connection refused ");
    }
  }

  @Test
  public void startupWorksGivenRedundantCopiesOfZero() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_REDUNDANT_COPIES, "0")
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "0")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true"));

    int RedundantCopies = cluster.getMember(0).invoke("getRedundantCopies", () -> {
      PartitionedRegion pr = (PartitionedRegion) RedisClusterStartupRule.getCache()
          .getRegion(RegionProvider.DEFAULT_REDIS_REGION_NAME);
      return pr.getPartitionAttributes().getRedundantCopies();
    });
    assertThat(RedundantCopies).isEqualTo(0);
  }

  @Test
  public void startupWorksGivenRedundantCopiesOfThree() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withSystemProperty(GEODE_FOR_REDIS_REDUNDANT_COPIES, "3")
        .withSystemProperty(GEODE_FOR_REDIS_PORT, "0")
        .withSystemProperty(GEODE_FOR_REDIS_ENABLED, "true"));

    int RedundantCopies = cluster.getMember(0).invoke("getRedundantCopies", () -> {
      PartitionedRegion pr = (PartitionedRegion) RedisClusterStartupRule.getCache()
          .getRegion(RegionProvider.DEFAULT_REDIS_REGION_NAME);
      return pr.getPartitionAttributes().getRedundantCopies();
    });
    assertThat(RedundantCopies).isEqualTo(3);
  }
}
