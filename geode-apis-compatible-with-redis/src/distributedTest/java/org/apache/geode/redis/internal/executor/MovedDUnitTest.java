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

package org.apache.geode.redis.internal.executor;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisMovedDataException;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.redis.internal.cluster.RedisMemberInfo;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class MovedDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  private static Jedis jedis1;
  private static Jedis jedis2;
  private static RedisAdvancedClusterCommands<String, String> lettuce;
  private static MemberVM locator;
  private static int redisServerPort1;
  private static final int ENTRIES = 200;

  @BeforeClass
  public static void classSetup() {
    locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());

    redisServerPort1 = clusterStartUp.getRedisPort(1);
    int redisServerPort2 = clusterStartUp.getRedisPort(2);

    jedis1 = new Jedis(BIND_ADDRESS, redisServerPort1, REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis(BIND_ADDRESS, redisServerPort2, REDIS_CLIENT_TIMEOUT);

    RedisClusterClient clusterClient =
        RedisClusterClient.create("redis://localhost:" + redisServerPort1);

    ClusterTopologyRefreshOptions refreshOptions =
        ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .build();

    clusterClient.setOptions(ClusterClientOptions.builder()
        .topologyRefreshOptions(refreshOptions)
        .autoReconnect(true)
        .validateClusterNodeMembership(false)
        .build());

    lettuce = clusterClient.connect().sync();
  }

  @Before
  public void testSetup() {
    clusterStartUp.flushAll();
  }

  @Test
  public void testMovedResponse_fromWrongServer() {
    int movedResponses = 0;
    Jedis jedis;

    for (int i = 0; i < ENTRIES; i++) {
      String key = "key-" + i;
      String value = "value-" + i;

      // Always pick the wrong connection to use
      RedisMemberInfo memberInfo = clusterStartUp.getMemberInfo(key);
      jedis = memberInfo.getRedisPort() == redisServerPort1 ? jedis2 : jedis1;

      try {
        jedis.set(key, value);
      } catch (JedisMovedDataException mex) {
        movedResponses++;
      }
    }

    assertThat(movedResponses).isEqualTo(ENTRIES);
  }

  @Test
  public void testNoMovedResponse_fromCorrectServer() {
    Jedis jedis;

    for (int i = 0; i < ENTRIES; i++) {
      String key = "key-" + i;
      String value = "value-" + i;

      // Always pick the right connection to use
      RedisMemberInfo memberInfo = clusterStartUp.getMemberInfo(key);
      jedis = memberInfo.getRedisPort() == redisServerPort1 ? jedis1 : jedis2;

      assertThat(jedis.set(key, value)).isEqualTo("OK");
    }
  }

  @Test
  @Ignore("GEODE-9368")
  public void movedResponseFollowsFailedServer() throws Exception {
    MemberVM server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    rebalanceAllRegions(server3);

    for (int i = 0; i < ENTRIES; i++) {
      lettuce.set("key-" + i, "value-" + i);
    }

    clusterStartUp.crashVM(3);

    for (int i = 0; i < ENTRIES; i++) {
      assertThat(lettuce.get("key-" + i)).isEqualTo("value-" + i);
    }
  }

  private static void rebalanceAllRegions(MemberVM vm) {
    vm.invoke("Running rebalance", () -> {
      ResourceManager manager = ClusterStartupRule.getCache().getResourceManager();
      RebalanceFactory factory = manager.createRebalanceFactory();
      try {
        factory.start().getResults();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

}
