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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractInfoIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;
  private static int REDIS_CLIENT_TIMEOUT = 10000;

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  abstract int getExposedPort();

  @Test
  public void shouldReturnRedisVersion() {
    String expectedResult = "redis_version:5.0.6";

    String actualResult = jedis.info();

    assertThat(actualResult).contains(expectedResult);
  }

  @Test
  public void shouldReturnTCPPort() {
    int expectedPort = getExposedPort();
    String expectedResult = "tcp_port:" + expectedPort;

    String actualResult = jedis.info();

    assertThat(actualResult).contains(expectedResult);
  }

  @Test
  public void shouldReturnRedisMode() {
    String expectedResult = "redis_mode:standalone";

    String actualResult = jedis.info();

    assertThat(actualResult).contains(expectedResult);
  }

  @Test
  public void shouldReturnLoadingProperty() {
    String expectedResult = "loading:0";

    String actualResult = jedis.info();

    assertThat(actualResult).contains(expectedResult);
  }

  @Test
  public void shouldReturnClusterEnabledProperty() {
    String expectedResult = "cluster_enabled:0";

    String actualResult = jedis.info();

    assertThat(actualResult).contains(expectedResult);
  }

  final List<String> SERVER_PROPERTIES =
      Arrays.asList(
          "# Server",
          "redis_version:",
          "tcp_port:",
          "redis_mode:");

  final List<String> PERSISTENCE_PROPERTIES =
      Arrays.asList(
          "# Persistence",
          "loading:");

  final List<String> CLUSTER_PROPERTIES =
      Arrays.asList(
          "# Cluster",
          "cluster_enabled:");

  @Test
  public void shouldReturnServerSectionsGivenServerSectionParameter() {
    String actualResult = jedis.info("server");

    assertThat(actualResult).contains(SERVER_PROPERTIES);
    assertThat(actualResult).doesNotContain(CLUSTER_PROPERTIES);
    assertThat(actualResult).doesNotContain(PERSISTENCE_PROPERTIES);
  }

  @Test
  public void shouldReturnClusterSectionsGivenClusterSectionParameter() {
    String actualResult = jedis.info("cluster");

    assertThat(actualResult).contains(CLUSTER_PROPERTIES);
    assertThat(actualResult).doesNotContain(SERVER_PROPERTIES);
    assertThat(actualResult).doesNotContain(PERSISTENCE_PROPERTIES);
  }

  @Test
  public void shouldReturnPersistenceSectionsGivenPersistenceSectionParameter() {
    String actualResult = jedis.info("persistence");

    assertThat(actualResult).contains(PERSISTENCE_PROPERTIES);
    assertThat(actualResult).doesNotContain(SERVER_PROPERTIES);
    assertThat(actualResult).doesNotContain(CLUSTER_PROPERTIES);
  }

  @Test
  public void shouldReturnEmptyStringGivenUnknownParameter() {
    String actualResult = jedis.info("nonesuch");
    assertThat(actualResult).isEqualTo("");
  }


  @Test
  public void shouldReturnDefaultsGivenDefaultParameter() {
    String actualResult = jedis.info("default");
    assertThat(actualResult).contains(CLUSTER_PROPERTIES);
    assertThat(actualResult).contains(SERVER_PROPERTIES);
    assertThat(actualResult).contains(PERSISTENCE_PROPERTIES);
  }

  @Test
  public void shouldReturnDefaultsGivenAllParameter() {
    String actualResult = jedis.info("all");
    assertThat(actualResult).contains(CLUSTER_PROPERTIES);
    assertThat(actualResult).contains(SERVER_PROPERTIES);
    assertThat(actualResult).contains(PERSISTENCE_PROPERTIES);
  }

  @Test
  public void shouldThrowExceptionIfGivenMoreThanOneParameter() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            Protocol.Command.INFO, "Server", "Cluster")).hasMessageContaining("ERR syntax error");
  }
}
