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

package org.apache.geode.redis.internal.executor.cluster;

import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.JedisClusterCRC16;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractClusterIntegrationTest implements RedisIntegrationTest {
  private static final int NUM_KEYS_TO_TEST = 1000;
  private static final int MAX_KEY_LENGTH = 255;

  private JedisCluster jedis;

  @Before
  public void setup() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()));
  }

  @After
  public void teardown() {
    jedis.close();
  }

  @Test
  public void testCluster_givenWrongNumberOfArguments() {
    final Jedis connection = jedis.getConnectionFromSlot(0);
    assertThatThrownBy(() -> connection.sendCommand(Protocol.Command.CLUSTER))
        .hasMessage("ERR wrong number of arguments for 'cluster' command");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "1", "2"))
            .hasMessage(
                "ERR Unknown subcommand or wrong number of arguments for '1'. Try CLUSTER HELP.");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "SLOTS", "1"))
            .hasMessage(
                "ERR Unknown subcommand or wrong number of arguments for 'SLOTS'. Try CLUSTER HELP.");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "NOTACOMMAND"))
            .hasMessage(
                "ERR Unknown subcommand or wrong number of arguments for 'NOTACOMMAND'. Try CLUSTER HELP.");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "KEYSLOT"))
            .hasMessage(
                "ERR Unknown subcommand or wrong number of arguments for 'KEYSLOT'. Try CLUSTER HELP.");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "KEYSLOT",
            "blah", "fo"))
                .hasMessage(
                    "ERR Unknown subcommand or wrong number of arguments for 'KEYSLOT'. Try CLUSTER HELP.");
  }

  @Test
  public void keyslot_ReturnsCorrectSlot() {
    final Jedis connection = jedis.getConnectionFromSlot(0);
    assertThat(connection.clusterKeySlot("nohash")).isEqualTo(9072);
    assertThat(connection.clusterKeySlot("with{hash}")).isEqualTo(238);
    assertThat(connection.clusterKeySlot("with{two}{hashes}")).isEqualTo(2127);
    assertThat(connection.clusterKeySlot("with{borked{hashes}")).isEqualTo(1058);
    assertThat(connection.clusterKeySlot("with{unmatched")).isEqualTo(10479);
    assertThat(connection.clusterKeySlot("aaa}bbb{tag}ccc")).isEqualTo(8338);
    assertThat(connection.clusterKeySlot("withunmatchedright}")).isEqualTo(10331);
    assertThat(connection.clusterKeySlot("somekey")).isEqualTo(11058L);
    assertThat(connection.clusterKeySlot("foo{hash_tag}")).isEqualTo(2515L);
    assertThat(connection.clusterKeySlot("bar{hash_tag}")).isEqualTo(2515L);
    assertThat(connection.clusterKeySlot("hash_tag")).isEqualTo(2515L);

    for (int i = 0; i < NUM_KEYS_TO_TEST; i++) {
      String key = RandomStringUtils.random(i % MAX_KEY_LENGTH + 1);
      assertThat(connection.clusterKeySlot(key))
          .withFailMessage("Failure for key %s", key)
          .isEqualTo(JedisClusterCRC16.getCRC16(key) % (long) REDIS_SLOTS);
    }
  }
}
