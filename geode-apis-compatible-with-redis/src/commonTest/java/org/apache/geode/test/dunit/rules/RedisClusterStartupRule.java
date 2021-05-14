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

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;

import java.util.Properties;

import redis.clients.jedis.Jedis;

import org.apache.geode.redis.ClusterNode;
import org.apache.geode.redis.ClusterNodes;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RedisClusterStartupRule extends ClusterStartupRule {

  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private static final String BIND_ADDRESS = "127.0.0.1";
  public static final String DEFAULT_MAX_WAIT_TIME_RECONNECT = "15000";

  public RedisClusterStartupRule() {
    super();
  }

  public RedisClusterStartupRule(int numVMs) {
    super(numVMs);
  }

  public MemberVM startRedisVM(int index, int... locatorPort) {
    return startServerVM(index, r -> withRedis(r)
        .withConnectionToLocator(locatorPort)
        .withSystemProperty("enable-unsupported-commands", "true"));
  }


  public MemberVM startRedisVM(int index, String redisPort, int... locatorPort) {
    return startServerVM(index, r -> withRedis(r, redisPort)
        .withConnectionToLocator(locatorPort));
  }


  public MemberVM startRedisVM(int index, Properties properties, int... locatorPort) {
    return startServerVM(index, x -> withRedis(x)
        .withProperties(properties)
        .withConnectionToLocator(locatorPort));
  }

  public MemberVM startRedisVM(int index, SerializableFunction<ServerStarterRule> ruleOperator) {
    return startServerVM(index, x -> ruleOperator.apply(withRedis(x)));
  }

  private ServerStarterRule withRedis(ServerStarterRule rule) {
    return rule.withProperty(REDIS_BIND_ADDRESS, BIND_ADDRESS)
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(GeodeRedisServer.ENABLE_UNSUPPORTED_COMMANDS_PARAM,
            "true");
  }

  private ServerStarterRule withRedis(ServerStarterRule rule, String redisPort) {
    return rule.withProperty(REDIS_BIND_ADDRESS, BIND_ADDRESS)
        .withProperty(REDIS_PORT, redisPort)
        .withProperty(REDIS_ENABLED, "true")
        .withSystemProperty(GeodeRedisServer.ENABLE_UNSUPPORTED_COMMANDS_PARAM,
            "true");
  }

  public int getRedisPort(int vmNumber) {
    return getRedisPort(getMember(vmNumber));
  }

  public int getRedisPort(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getPort();
    });
  }

  public void setEnableUnsupported(MemberVM vm, boolean enableUnsupported) {
    vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      service.setEnableUnsupported(enableUnsupported);
    });
  }

  public Long getDataStoreBytesInUseForDataRegion(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getDataStoreBytesInUseForDataRegion();
    });
  }

  public void flushAll(int redisPort) {
    ClusterNodes nodes;
    try (Jedis jedis = new Jedis("localhost", redisPort, REDIS_CLIENT_TIMEOUT)) {
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
