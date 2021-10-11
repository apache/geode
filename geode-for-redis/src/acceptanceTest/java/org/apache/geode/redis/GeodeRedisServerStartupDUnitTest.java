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
import static org.apache.geode.redis.internal.RedisConstants.DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS;
import static org.apache.geode.redis.internal.RedisConstants.DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS;
import static org.apache.geode.redis.internal.RedisConstants.DEFAULT_REDIS_WRITE_TIMEOUT_SECONDS;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.categories.IgnoreInRepeatTestTasks;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class GeodeRedisServerStartupDUnitTest {

  @Rule
  public RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Category(IgnoreInRepeatTestTasks.class)
  @Test
  public void startupOnDefaultPort() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "6379")
        .withProperty(REDIS_BIND_ADDRESS, "localhost")
        .withProperty(REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server)).isEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
  }

  @Test
  public void startupOnDefaultPort_whenPortIsNotSpecified() {
    MemberVM server = cluster.startServerVM(0, s -> s
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
            "Could not set \"geode-for-redis-port\" to \"-1\" because its value can not be less than \"0\".");
  }

  @Test
  public void startupFailsGivenPortAlreadyInUse() throws Exception {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    addIgnoredException("Could not start server compatible with Redis");
    try (Socket interferingSocket = new Socket()) {
      interferingSocket.bind(new InetSocketAddress("localhost", port));
      assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
          .withProperty(REDIS_PORT, "" + port)
          .withProperty(REDIS_BIND_ADDRESS, "localhost")
          .withProperty(REDIS_ENABLED, "true")))
              .hasRootCauseInstanceOf(BindException.class);
    }
  }

  @Test
  public void startupFailsGivenInvalidBindAddress() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    addIgnoredException("Could not start server compatible with Redis");
    assertThatThrownBy(() -> cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "" + port)
        .withProperty(REDIS_BIND_ADDRESS, "1.1.1.1")
        .withProperty(REDIS_ENABLED, "true")))
            .hasStackTraceContaining(
                "The geode-for-redis-bind-address 1.1.1.1 is not a valid address for this machine");
  }

  @Test
  public void startupOnSpecifiedPort() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "4242")
        .withProperty(REDIS_BIND_ADDRESS, "localhost")
        .withProperty(REDIS_ENABLED, "true"));

    assertThat(cluster.getRedisPort(server)).isEqualTo(4242);
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

  @Test
  public void shouldUseDefaultTimeout_whenConnectTimeoutNotSet() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_ENABLED, "true")
        .withProperty(REDIS_BIND_ADDRESS, "localhost"));

    assertThat(getRedisConnectTimeoutMillis(server))
        .isEqualTo(DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS);
  }

  @Test
  public void shouldUseDefaultConnectTimeout_whenNonIntegerTimeoutSet() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_ENABLED, "true")
        .withProperty(REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(RedisConstants.CONNECT_TIMEOUT_SECONDS, "nonInteger"));

    assertThat(getRedisConnectTimeoutMillis(server))
        .isEqualTo(DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS);
  }

  @Test
  public void shouldUseDefaultConnectTimeout_whenNegativeTimeoutSet() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_ENABLED, "true")
        .withProperty(REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(RedisConstants.CONNECT_TIMEOUT_SECONDS, "-42"));

    assertThat(getRedisConnectTimeoutMillis(server))
        .isEqualTo(DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS);
  }

  @Test
  public void shouldUseDefaultConnectTimeout_whenZeroTimeoutSet() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_ENABLED, "true")
        .withProperty(REDIS_BIND_ADDRESS, "localhost")
        .withSystemProperty(RedisConstants.CONNECT_TIMEOUT_SECONDS, "0"));

    assertThat(getRedisConnectTimeoutMillis(server))
        .isEqualTo(DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS);
  }

  @Test
  public void shouldUseSpecifiedConnectTimeoutMillis_whenSystemPropertySet() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.CONNECT_TIMEOUT_SECONDS, "4242"));

    assertThat(getRedisConnectTimeoutMillis(server)).isEqualTo(4242);
  }

  @Test
  public void shouldUseDefaultValue_whenWriteTimeoutNotSet() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true"));

    assertThat(getRedisWriteTimeoutSeconds(server))
        .isEqualTo(DEFAULT_REDIS_WRITE_TIMEOUT_SECONDS);
  }

  @Test
  public void shouldUseDefaultValue_whenWriteTimeoutSetToNonIntegerValue() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.WRITE_TIMEOUT_SECONDS, "nonInteger"));

    assertThat(getRedisWriteTimeoutSeconds(server))
        .isEqualTo(DEFAULT_REDIS_WRITE_TIMEOUT_SECONDS);
  }

  @Test
  public void shouldUseDefaultValue_whenWriteTimeoutSetToNegativeValue() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.WRITE_TIMEOUT_SECONDS, "-42"));

    assertThat(getRedisWriteTimeoutSeconds(server))
        .isEqualTo(DEFAULT_REDIS_WRITE_TIMEOUT_SECONDS);
  }

  @Test
  public void shouldUseDefaultValue_whenWriteTimeoutSetToZero() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.WRITE_TIMEOUT_SECONDS, "0"));

    assertThat(getRedisWriteTimeoutSeconds(server))
        .isEqualTo(DEFAULT_REDIS_WRITE_TIMEOUT_SECONDS);
  }

  @Test
  public void shouldUseSpecifiedWriteTimeout_whenSetToValidInteger() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.WRITE_TIMEOUT_SECONDS, "42"));

    assertThat(getRedisWriteTimeoutSeconds(server)).isEqualTo(42);
  }

  @Test
  public void shouldUseDefaultValue_whenInitialDelayNotSet() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true"));

    assertThat(getRedisInitialDelayMinutes(server))
        .isEqualTo(DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS);
  }

  @Test
  public void shouldUseDefaultValue_whenInitialDelaySetToNonIntegerValue() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.EXPIRATION_INTERVAL_SECONDS, "nonInteger"));

    assertThat(getRedisInitialDelayMinutes(server))
        .isEqualTo(DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS);
  }

  @Test
  public void shouldUseDefaultValue_whenInitialDelaySetToNegativeValue() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.EXPIRATION_INTERVAL_SECONDS, "-42"));

    assertThat(getRedisInitialDelayMinutes(server))
        .isEqualTo(DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS);
  }

  @Test
  public void shouldUseSpecifiedInitialDelay_whenSetToValidInteger() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.EXPIRATION_INTERVAL_SECONDS, "42"));

    assertThat(getRedisInitialDelayMinutes(server)).isEqualTo(42);
  }

  @Test
  public void shouldUseDefaultValue_whenDelayNotSet() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true"));

    assertThat(getRedisDelayMinutes(server))
        .isEqualTo(DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS);
  }

  @Test
  public void shouldUseDefaultValue_whenDelaySetToNonIntegerValue() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.EXPIRATION_INTERVAL_SECONDS, "nonInteger"));

    assertThat(getRedisDelayMinutes(server))
        .isEqualTo(DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS);
  }

  @Test
  public void shouldUseDefaultValue_whenDelaySetToNegativeValue() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.EXPIRATION_INTERVAL_SECONDS, "-42"));

    assertThat(getRedisDelayMinutes(server))
        .isEqualTo(DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS);
  }

  @Test
  public void shouldUseDefaultValue_whenWhenDelaySetToZero() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.EXPIRATION_INTERVAL_SECONDS, "0"));

    assertThat(getRedisDelayMinutes(server))
        .isEqualTo(DEFAULT_REDIS_EXPIRATION_INTERVAL_SECONDS);
  }

  @Test
  public void shouldUseSpecifiedDelay_whenSetToValidInteger() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(RedisConstants.EXPIRATION_INTERVAL_SECONDS, "42"));

    assertThat(getRedisDelayMinutes(server)).isEqualTo(42);
  }

  /** Helper Methods **/
  public int getRedisConnectTimeoutMillis(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getRedisServer().getConnectTimeoutMillis();
    });
  }

  public int getRedisWriteTimeoutSeconds(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getRedisServer().getWriteTimeoutSeconds();
    });
  }

  public int getRedisInitialDelayMinutes(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getRedisServer().getInitialDelayMinutes();
    });
  }

  public int getRedisDelayMinutes(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getRedisServer().getDelayMinutes();
    });
  }
}
