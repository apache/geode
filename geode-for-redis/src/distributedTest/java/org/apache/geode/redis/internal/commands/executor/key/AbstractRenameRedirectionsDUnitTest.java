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

package org.apache.geode.redis.internal.commands.executor.key;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.JedisClusterCRC16;

import org.apache.geode.redis.ClusterNode;
import org.apache.geode.redis.ClusterNodes;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.services.RegionProvider;

/**
 * Tests bug fix for GEODE-9665 Fix multiple redirections in RENAME and RENAMENX
 * Before bug was fixed, failed for Geode and succeeded for native Redis.
 */
public abstract class AbstractRenameRedirectionsDUnitTest implements RedisIntegrationTest {

  private JedisCluster jedis;

  @Before
  public void setup() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void teardown() {
    jedis.close();
    flushAll();
  }

  @Test
  public void renameReturnCrossslotsError_whenSlotsNotOnSameServer() {
    String oldKey = "key-0";
    String newKey = getKeyOnDifferentServerAs(oldKey, "key-");

    jedis.set(oldKey, "value");
    assertThatThrownBy(() -> jedis.sendCommand(oldKey, Protocol.Command.RENAME, oldKey, newKey))
        .hasMessage("CROSSSLOT " + RedisConstants.ERROR_WRONG_SLOT);
  }

  private String getKeyOnDifferentServerAs(String antiKey, String prefix) {
    ClusterNodes clusterNodes;
    try (Jedis j = jedis.getConnectionFromSlot(0)) {
      clusterNodes = ClusterNodes.parseClusterNodes(j.clusterNodes());
    }
    int antiSlot = JedisClusterCRC16.getCRC16(antiKey) % RegionProvider.REDIS_SLOTS;

    // First find any node not hosting the antiKey
    List<Pair<Long, Long>> possibleSlots = null;
    for (ClusterNode node : clusterNodes.getNodes()) {
      if (!node.primary) {
        continue;
      }

      if (!isSlotInSlots(antiSlot, node.slots)) {
        possibleSlots = node.slots;
        break;
      }
    }
    assertThat(possibleSlots).as("Could not find node NOT hosting slot " + antiSlot)
        .isNotNull();

    int x = 0;
    do {
      String newKey = prefix + x++;
      int newSlot = JedisClusterCRC16.getCRC16(newKey) % RegionProvider.REDIS_SLOTS;
      if (isSlotInSlots(newSlot, possibleSlots)) {
        return newKey;
      }
    } while (true);
  }

  private boolean isSlotInSlots(long candidate, List<Pair<Long, Long>> slots) {
    for (Pair<Long, Long> slot : slots) {
      if (candidate >= slot.getLeft() && candidate <= slot.getRight()) {
        return true;
      }
    }
    return false;
  }
}
