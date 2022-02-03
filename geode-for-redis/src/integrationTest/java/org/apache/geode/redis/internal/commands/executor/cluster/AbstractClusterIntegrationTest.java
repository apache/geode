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

package org.apache.geode.redis.internal.commands.executor.cluster;

import static org.apache.geode.redis.internal.services.RegionProvider.REDIS_SLOTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.Command.CLUSTER;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.JedisClusterCRC16;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractClusterIntegrationTest implements RedisIntegrationTest {
  private static final int NUM_KEYS_TO_TEST = 1000;
  private static final int MAX_KEY_LENGTH = 255;

  JedisCluster jedis;

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
    final Connection connection = jedis.getConnectionFromSlot(0);
    assertThatThrownBy(() -> connection.sendCommand(Protocol.Command.CLUSTER))
        .hasMessage("ERR wrong number of arguments for 'cluster' command");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "1", "2"))
            .hasMessageContaining(
                "ERR Unknown subcommand or wrong number of arguments for '1'.");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "SLOTS", "1"))
            .hasMessageContaining(
                "ERR Unknown subcommand or wrong number of arguments for 'SLOTS'.");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "NOTACOMMAND"))
            .hasMessageContaining(
                "ERR Unknown subcommand or wrong number of arguments for 'NOTACOMMAND'.");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "KEYSLOT"))
            .hasMessageContaining(
                "ERR Unknown subcommand or wrong number of arguments for 'KEYSLOT'.");
    assertThatThrownBy(
        () -> connection.sendCommand(Protocol.Command.CLUSTER, "KEYSLOT",
            "blah", "fo"))
                .hasMessageContaining(
                    "ERR Unknown subcommand or wrong number of arguments for 'KEYSLOT'.");
  }

  @Test
  public void keyslot_ReturnsCorrectSlot() {
    final Connection cxn = jedis.getConnectionFromSlot(0);

    assertThat(clusterKeySlot(cxn, "nohash")).isEqualTo(9072);
    assertThat(clusterKeySlot(cxn, "with{hash}")).isEqualTo(238);
    assertThat(clusterKeySlot(cxn, "with{two}{hashes}")).isEqualTo(2127);
    assertThat(clusterKeySlot(cxn, "with{borked{hashes}")).isEqualTo(1058);
    assertThat(clusterKeySlot(cxn, "with{unmatched")).isEqualTo(10479);
    assertThat(clusterKeySlot(cxn, "aaa}bbb{tag}ccc")).isEqualTo(8338);
    assertThat(clusterKeySlot(cxn, "withunmatchedright}")).isEqualTo(10331);
    assertThat(clusterKeySlot(cxn, "somekey")).isEqualTo(11058L);
    assertThat(clusterKeySlot(cxn, "foo{hash_tag}")).isEqualTo(2515L);
    assertThat(clusterKeySlot(cxn, "bar{hash_tag}")).isEqualTo(2515L);
    assertThat(clusterKeySlot(cxn, "hash_tag")).isEqualTo(2515L);

    for (int i = 0; i < NUM_KEYS_TO_TEST; i++) {
      String key = RandomStringUtils.random(i % MAX_KEY_LENGTH + 1);
      assertThat(clusterKeySlot(cxn, key))
          .withFailMessage("Failure for key %s", key)
          .isEqualTo(JedisClusterCRC16.getCRC16(key) % (long) REDIS_SLOTS);
    }
  }

  private Long clusterKeySlot(final Connection cxn, final String arg) {
    cxn.sendCommand(CLUSTER, Protocol.ClusterKeyword.KEYSLOT.toString(), arg);
    return cxn.getIntegerReply();
  }
}
