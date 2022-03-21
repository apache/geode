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

package org.apache.geode.redis.internal.commands.executor.list;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class BLPopDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule(4);

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final String LIST_KEY = "key";
  private static JedisCluster jedis;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;
  private static int locatorPort;
  private static int redisServerPort;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = cluster.startLocatorVM(0);
    locatorPort = locator.getPort();

    server1 = cluster.startRedisVM(1, locatorPort);
    server2 = cluster.startRedisVM(2, locatorPort);

    redisServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    server3 = cluster.startRedisVM(3, Integer.toString(redisServerPort), locatorPort);

    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), REDIS_CLIENT_TIMEOUT,
        20);
  }

  @After
  public void tearDown() {
    cluster.flushAll();
  }

  @Test
  public void testClientRepeatsBLpopAfterServerCrash() throws Exception {
    cluster.moveBucketForKey(LIST_KEY, "server-3");
    Future<List<String>> blpopFuture = executor.submit(() -> jedis.blpop(0, LIST_KEY));

    try {
      cluster.crashVM(3);
      jedis.lpush(LIST_KEY, "value");
      List<String> result = blpopFuture.get();
      assertThat(result).containsExactly(LIST_KEY, "value");
    } finally {
      cluster.startRedisVM(3, Integer.toString(redisServerPort), locatorPort);
      cluster.rebalanceAllRegions();
    }
  }

  @Test
  public void testBLPopFollowsBucketMovement() throws Exception {
    Future<List<String>> blpopFuture = executor.submit(() -> jedis.blpop(0, LIST_KEY));

    for (int i = 0; i < 11; i++) {
      cluster.moveBucketForKey(LIST_KEY);
      Thread.sleep(500);
    }

    jedis.lpush(LIST_KEY, "value");

    assertThat(blpopFuture.get()).containsExactly(LIST_KEY, "value");

    int registeredListeners = getRegisteredListeners(server1);
    registeredListeners += getRegisteredListeners(server2);
    registeredListeners += getRegisteredListeners(server3);

    assertThat(registeredListeners).isEqualTo(0);
  }

  private int getRegisteredListeners(MemberVM vm) {
    return vm.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      return service.getRedisServer().getEventDistributor().getRegisteredKeys();
    });
  }


}
