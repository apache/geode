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

package org.apache.geode.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisMovedDataException;

import org.apache.geode.test.dunit.rules.ClusterNode;
import org.apache.geode.test.dunit.rules.NativeRedisClusterTestRule;

/**
 * This class serves merely as an example of using the {@link NativeRedisClusterTestRule}.
 * Eventually it can be deleted since we'll end up with more comprehensive tests for various
 * {@code CLUSTER} commands.
 */
public class NativeRedisClusterTest {

  @ClassRule
  public static NativeRedisClusterTestRule cluster = new NativeRedisClusterTestRule();

  @Test
  public void testEachProxyReturnsExposedPorts() {
    for (Integer port : cluster.getExposedPorts()) {
      try (Jedis jedis = new Jedis("localhost", port)) {
        List<ClusterNode> nodes =
            NativeRedisClusterTestRule.parseClusterNodes(jedis.clusterNodes());
        List<Integer> ports = nodes.stream().map(f -> f.port).collect(Collectors.toList());
        assertThat(ports).containsExactlyInAnyOrderElementsOf(cluster.getExposedPorts());
      }
    }
  }

  @Test
  public void testClusterAwareClient() {
    try (JedisCluster jedis =
        new JedisCluster(new HostAndPort("localhost", cluster.getExposedPorts().get(0)))) {
      jedis.set("a", "0"); // slot 15495
      jedis.set("b", "1"); // slot 3300
      jedis.set("c", "2"); // slot 7365
      jedis.set("d", "3"); // slot 11298
      jedis.set("e", "4"); // slot 15363
      jedis.set("f", "5"); // slot 3168
      jedis.set("g", "6"); // slot 7233
      jedis.set("h", "7"); // slot 11694
      jedis.set("i", "8"); // slot 15759
      jedis.set("j", "9"); // slot 3564
    }
  }

  @Test
  public void testMoved() {
    try (Jedis jedis =
        new Jedis("localhost", cluster.getExposedPorts().get(0), 100000)) {
      assertThatThrownBy(() -> jedis.set("a", "A"))
          .isInstanceOf(JedisMovedDataException.class)
          .hasMessageContaining("127.0.0.1");
    }
  }

}
