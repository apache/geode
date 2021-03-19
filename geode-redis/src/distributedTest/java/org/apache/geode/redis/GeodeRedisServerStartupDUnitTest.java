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

import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class GeodeRedisServerStartupDUnitTest {

  @Rule
  public RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void startupOnDefaultPort() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "6379")
        .withProperty(REDIS_BIND_ADDRESS, "localhost")
        .withProperty(REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server)).isEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }

  @Test
  public void startupOnRandomPort_whenPortIsZero() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_BIND_ADDRESS, "localhost")
        .withProperty(REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server))
        .isNotEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }

  @Test
  public void doNotStartup_whenRedisServiceIsNotEnabled() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_BIND_ADDRESS, "localhost"));

    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getService(GeodeRedisService.class))
          .as("GeodeRedisService should not exist")
          .isNull();
    });
  }

  @Test
  public void startupFailsGivenIllegalPort() {
    assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "-1")
        .withProperty(REDIS_BIND_ADDRESS, "localhost")
        .withProperty(REDIS_ENABLED, "true"))).hasRootCauseMessage(
            "Could not set \"redis-port\" to \"-1\" because its value can not be less than \"0\".");
  }

  @Test
  public void startupFailsGivenPortAlreadyInUse() throws Exception {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    addIgnoredException("Could not start Redis Server");
    try (Socket interferingSocket = new Socket()) {
      interferingSocket.bind(new InetSocketAddress("localhost", port));
      assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
          .withProperty(REDIS_PORT, "" + port)
          .withProperty(REDIS_BIND_ADDRESS, "localhost")
          .withProperty(REDIS_ENABLED, "true")))
              .hasRootCauseMessage("Address already in use");
    }
  }

  @Test
  public void startupFailsGivenInvalidBindAddress() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    addIgnoredException("Could not start Redis Server");
    assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "" + port)
        .withProperty(REDIS_BIND_ADDRESS, "1.1.1.1")
        .withProperty(REDIS_ENABLED, "true")))
            .hasStackTraceContaining(
                "The redis-bind-address 1.1.1.1 is not a valid address for this machine");
  }

  @Test
  public void startupWorksGivenAnyLocalAddress() {
    String anyLocal = LocalHostUtil.getAnyLocalAddress().getHostAddress();
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_BIND_ADDRESS, anyLocal)
        .withProperty(REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server))
        .isNotEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }

  @Test
  public void startupWorksGivenNoBindAddress() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server))
        .isNotEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }
}
