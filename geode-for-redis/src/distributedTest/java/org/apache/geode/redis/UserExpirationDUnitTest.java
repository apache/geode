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
import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.dunit.rules.SerializableFunction;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class UserExpirationDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  private static MemberVM server1;
  private static MemberVM server2;
  private static JedisCluster jedis;
  private static final String USER = "data";
  private static int redisPort;

  @BeforeClass
  public static void setup() throws Exception {
    MemberVM locator = cluster.startLocatorVM(0, x -> x
        .withSecurityManager(ExpiringSecurityManager.class));

    int locatorPort = locator.getPort();

    SerializableFunction<ServerStarterRule> serverOperator = s -> s
        .withCredential("cluster", "cluster")
        .withConnectionToLocator(locatorPort);

    server1 = cluster.startRedisVM(1, serverOperator);
    server2 = cluster.startRedisVM(2, serverOperator);

    redisPort = cluster.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisPort),
        REDIS_CLIENT_TIMEOUT,
        REDIS_CLIENT_TIMEOUT,
        BinaryJedisCluster.DEFAULT_MAX_ATTEMPTS,
        USER, USER, "test-client",
        new GenericObjectPoolConfig<>());
  }

  @After
  public void cleanup() {
    resetExpiredUsers();
    cluster.flushAll(USER, USER);
  }

  @Test
  public void jedisRetriesExpiredConnections() {
    jedis.set("foo", "bar");

    expireUser(USER);

    assertThat(jedis.get("foo")).isEqualTo("bar");
  }

  @Test
  public void lettuceRetriesExpiredConnections() {
    RedisClusterClient client = RedisClusterClient.create(
        String.format("redis://%s:%s@localhost:%d", USER, USER, redisPort));
    RedisClusterCommands<String, String> commands = client.connect().sync();

    commands.set("foo", "bar");

    expireUser(USER);

    assertThat(commands.get("foo")).isEqualTo("bar");
  }

  private static void expireUser(String user) {
    server1.invoke(() -> getSecurityManager().addExpiredUser(user));
    server2.invoke(() -> getSecurityManager().addExpiredUser(user));
  }

  private static void resetExpiredUsers() {
    server1.invoke(() -> getSecurityManager().reset());
    server2.invoke(() -> getSecurityManager().reset());
  }

  private static ExpiringSecurityManager getSecurityManager() {
    return (ExpiringSecurityManager) RedisClusterStartupRule.getCache()
        .getSecurityService().getSecurityManager();
  }
}
