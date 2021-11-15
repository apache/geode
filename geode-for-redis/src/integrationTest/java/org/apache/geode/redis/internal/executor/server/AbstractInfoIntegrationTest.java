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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.RedisTestHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractInfoIntegrationTest implements RedisIntegrationTest {

  private static final String KEYSPACE_START = "db0";
  protected Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  final List<String> SERVER_PROPERTIES =
      Arrays.asList(
          "# Server",
          "redis_version:",
          "redis_mode:",
          "tcp_port:",
          "uptime_in_days:",
          "uptime_in_seconds:");

  final List<String> PERSISTENCE_PROPERTIES =
      Arrays.asList(
          "# Persistence",
          "rdb_changes_since_last_save",
          "rdb_last_save_time",
          "loading:");

  final List<String> REPLICATION_PROPERTIES =
      Arrays.asList(
          "# Replication",
          "role:",
          "connected_slaves:");

  final List<String> CLUSTER_PROPERTIES =
      Arrays.asList(
          "# Cluster",
          "cluster_enabled:");

  final List<String> CLIENTS_PROPERTIES =
      Arrays.asList(
          "# Clients",
          "connected_clients:",
          "blocked_clients:");

  final List<String> MEMORY_PROPERTIES =
      Arrays.asList(
          "# Memory",
          "maxmemory:",
          "used_memory:",
          "mem_fragmentation_ratio:");

  final List<String> KEYSPACE_PROPERTIES =
      Arrays.asList(
          "# Keyspace",
          "db0:");

  final List<String> STATS_PROPERTIES =
      Arrays.asList(
          "# Stats",
          "total_commands_processed:",
          "instantaneous_ops_per_sec:",
          "total_net_input_bytes:",
          "instantaneous_input_kbps:",
          "total_connections_received:",
          "keyspace_hits:",
          "keyspace_misses:",
          "evicted_keys:",
          "rejected_connections:");


  final List<String> ALL_PROPERTIES =
      Stream.of(SERVER_PROPERTIES, PERSISTENCE_PROPERTIES, CLUSTER_PROPERTIES,
          MEMORY_PROPERTIES, CLIENTS_PROPERTIES, STATS_PROPERTIES, REPLICATION_PROPERTIES)
          .flatMap(Collection::stream)
          .collect(
              Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

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

  @Test
  public void shouldReturnServerSections_givenServerSectionParameter() {
    List<String> nonServerProperties = new ArrayList<>(ALL_PROPERTIES);
    nonServerProperties.removeAll(SERVER_PROPERTIES);

    String actualResult = jedis.info("server");

    assertThat(actualResult).contains(SERVER_PROPERTIES);
    assertThat(actualResult).doesNotContain(nonServerProperties);
  }

  @Test
  public void shouldReturnClusterSections_givenClusterSectionParameter() {
    List<String> nonClusterProperties = new ArrayList<>(ALL_PROPERTIES);
    nonClusterProperties.removeAll(CLUSTER_PROPERTIES);

    String actualResult = jedis.info("cluster");

    assertThat(actualResult).contains(CLUSTER_PROPERTIES);
    assertThat(actualResult).doesNotContain(nonClusterProperties);
  }

  @Test
  public void shouldReturnPersistenceSections_givenPersistenceSectionParameter() {
    List<String> nonPersistenceProperties = new ArrayList<>(ALL_PROPERTIES);
    nonPersistenceProperties.removeAll(PERSISTENCE_PROPERTIES);

    String actualResult = jedis.info("persistence");

    assertThat(actualResult).contains(PERSISTENCE_PROPERTIES);
    assertThat(actualResult).doesNotContain(nonPersistenceProperties);
  }

  @Test
  public void shouldReturnStatsSections_givenStatsSectionParameter() {
    List<String> nonStatsProperties = new ArrayList<>(ALL_PROPERTIES);
    nonStatsProperties.removeAll(STATS_PROPERTIES);

    String actualResult = jedis.info("stats");

    assertThat(actualResult).contains(STATS_PROPERTIES);
    assertThat(actualResult).doesNotContain(nonStatsProperties);
  }

  @Test
  public void shouldReturnClientsSections_givenClientsSectionParameter() {
    List<String> nonClientsProperties = new ArrayList<>(ALL_PROPERTIES);
    nonClientsProperties.removeAll(CLIENTS_PROPERTIES);

    String actualResult = jedis.info("clients");

    assertThat(actualResult).contains(CLIENTS_PROPERTIES);
    assertThat(actualResult).doesNotContain(nonClientsProperties);
  }

  @Test
  public void shouldReturnMemorySections_givenMemorySectionParameter() {
    List<String> nonMemoryProperties = new ArrayList<>(ALL_PROPERTIES);
    nonMemoryProperties.removeAll(MEMORY_PROPERTIES);

    String actualResult = jedis.info("memory");

    assertThat(actualResult).contains(MEMORY_PROPERTIES);
    assertThat(actualResult).doesNotContain(nonMemoryProperties);
  }

  @Test
  public void shouldReturnKeySpaceSections_givenKeySpaceKeyspaceParameter() {
    List<String> nonKeyspaceProperties = new ArrayList<>(ALL_PROPERTIES);
    nonKeyspaceProperties.removeAll(KEYSPACE_PROPERTIES);

    jedis.set("key", "value");
    String actualResult = jedis.info("keyspace");

    assertThat(actualResult).contains(KEYSPACE_PROPERTIES);
    assertThat(actualResult).doesNotContain(nonKeyspaceProperties);
  }

  @Test
  public void shouldReturnReplicationSections_givenKeySpaceReplicationParameter() {
    List<String> nonReplicationProperties = new ArrayList<>(ALL_PROPERTIES);
    nonReplicationProperties.removeAll(REPLICATION_PROPERTIES);

    String actualResult = jedis.info("replication");

    assertThat(actualResult).contains(REPLICATION_PROPERTIES);
    assertThat(actualResult).doesNotContain(nonReplicationProperties);
  }

  @Test
  public void shouldReturnEmptyString_givenUnknownParameter() {
    String actualResult = jedis.info("nonesuch");
    assertThat(actualResult).isEqualTo("");
  }

  @Test
  public void shouldReturnDefaults_givenDefaultParameter() {
    jedis.set("key", "value");
    String actualResult = jedis.info("default");
    assertThat(actualResult)
        .contains(ALL_PROPERTIES);
  }

  @Test
  public void shouldReturnDefaults_givenAllParameters() {
    jedis.set("key", "value"); // make sure keyspace is there
    String actualResult = jedis.info("all");
    assertThat(actualResult)
        .contains(ALL_PROPERTIES);
  }

  @Test
  public void shouldReturnKeySpaceSection_givenServerWithOneOrMoreKeys() {
    jedis.set("key", "value");

    Map<String, String> info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(KEYSPACE_START)).startsWith("keys=1");
  }

  @Test
  public void shouldNotReturnKeySpaceSection_givenServerWithNoKeys() {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(KEYSPACE_START)).isNull();
  }

  @Test
  public void shouldThrowException_ifGivenMoreThanOneParameter() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            Protocol.Command.INFO, "Server", "Cluster")).hasMessageContaining("ERR syntax error");
  }
}
