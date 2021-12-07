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

package org.apache.geode.redis.internal.executor.auth;

import static org.apache.geode.distributed.ConfigurationProperties.GEODE_FOR_REDIS_PORT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static redis.clients.jedis.BinaryJedisCluster.DEFAULT_MAX_ATTEMPTS;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.dunit.rules.SerializableFunction;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class AuthWhileServersRestartDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static MemberVM server1;
  private static int redisServerPort;
  private static SerializableFunction<ServerStarterRule> operatorForVM3;
  private static final String KEY = "key";
  private static final int SO_TIMEOUT = 10_000;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0,
        x -> x.withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();

    SerializableFunction<ServerStarterRule> serverOperator = s -> s
        .withCredential("cluster", "cluster")
        .withConnectionToLocator(locatorPort);

    server1 = clusterStartUp.startRedisVM(1, serverOperator);
    clusterStartUp.startRedisVM(2, serverOperator);

    int server3Port = AvailablePortHelper.getRandomAvailableTCPPort();
    String finalRedisPort = Integer.toString(server3Port);

    operatorForVM3 = serverOperator.compose(o -> o
        .withProperty(GEODE_FOR_REDIS_PORT, finalRedisPort));

    clusterStartUp.startRedisVM(3, operatorForVM3);

    redisServerPort = clusterStartUp.getRedisPort(1);
    // Make sure buckets get created
    new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort),
        REDIS_CLIENT_TIMEOUT, SO_TIMEOUT, DEFAULT_MAX_ATTEMPTS, "data", "data", "bootstrap",
        new GenericObjectPoolConfig<>());
  }

  @After
  public void after() {
    // Make sure that no buckets are moving before calling flushAll, otherwise we might get
    // a MOVED exception.
    clusterStartUp.rebalanceAllRegions();
    clusterStartUp.flushAll("data", "data");
  }

  @Test
  public void testReconnectionWithAuthAndServerRestarts() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicInteger loopCounter = new AtomicInteger(0);

    Future<?> future = executor.submit(() -> {
      try {
        for (int i = 0; i < 20 && running.get(); i++) {
          clusterStartUp.moveBucketForKey(KEY, "server-3");
          // Wait for a bit so that commands can execute
          int start = loopCounter.get();
          GeodeAwaitility.await().until(() -> loopCounter.get() - start > 1000);

          clusterStartUp.crashVM(3);
          clusterStartUp.startRedisVM(3, operatorForVM3);
        }
      } finally {
        running.set(false);
      }
    });

    new ConcurrentLoopingThreads(running,
        i -> doOps(1, i, loopCounter),
        i -> doOps(2, i, loopCounter))
            .run();

    running.set(false);
    future.get();
  }

  private void doOps(int id, int i, AtomicInteger counter) {
    String user = "data,data-" + i;
    try (JedisCluster jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort),
        REDIS_CLIENT_TIMEOUT, SO_TIMEOUT, DEFAULT_MAX_ATTEMPTS * 2, user, user,
        "client-" + id + "-" + i, new GenericObjectPoolConfig<>())) {

      jedis.sadd(KEY, "value-" + counter.getAndIncrement());
    }
  }
}
