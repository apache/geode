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

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;

import redis.clients.jedis.Jedis;

public interface RedisIntegrationTest {

  int getPort();

  default void flushAll() {
    ClusterNodes nodes;
    try (Jedis jedis = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT)) {
      nodes = ClusterNodes.parseClusterNodes(jedis.clusterNodes());
    }

    for (ClusterNode node : nodes.getNodes()) {
      if (!node.primary) {
        continue;
      }
      try (Jedis jedis = new Jedis(node.ipAddress, (int) node.port, REDIS_CLIENT_TIMEOUT)) {
        jedis.flushAll();
      }
    }
  }
}
