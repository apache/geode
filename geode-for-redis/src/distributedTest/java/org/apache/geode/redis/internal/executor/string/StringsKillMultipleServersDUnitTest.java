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

package org.apache.geode.redis.internal.executor.string;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;

import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class StringsKillMultipleServersDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule(5);

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static JedisCluster jedisCluster;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    cluster.startRedisVM(1, locatorPort);
    cluster.startRedisVM(2, locatorPort);
    cluster.startRedisVM(3, locatorPort);

    int redisServerPort1 = cluster.getRedisPort(1);
    jedisCluster =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), REDIS_CLIENT_TIMEOUT);

    // This sequence ensures that servers 1, 2 and 3 are hosting all the buckets and server 4
    // has no buckets.
    cluster.startRedisVM(4, locatorPort);
  }

  @After
  public void after() {
    cluster.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedisCluster.close();
  }

  @Test
  public void operationsShouldContinueAfterMultipleServerFailures() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicInteger counter = new AtomicInteger(0);

    Future<Void> future1 = executor.submit(() -> doSetOps(running, counter));
    Future<Void> future2 = executor.submit(() -> doGetOps(running, counter));
    await().until(() -> counter.get() > 1000);

    cluster.crashVM(2);
    cluster.crashVM(3);

    int afterCrashCount = counter.get();
    await().alias("ensure that operations are continuing after multiple server failures")
        .until(() -> counter.get() > afterCrashCount + 10_000);

    running.set(false);

    // Any exception here would fail the test.
    future1.get();
    future2.get();
  }

  private Void doSetOps(AtomicBoolean running, AtomicInteger counter) {
    while (running.get()) {
      int i = counter.getAndIncrement();
      jedisCluster.set("key-" + i, "value-" + i);
    }
    return null;
  }

  private Void doGetOps(AtomicBoolean running, AtomicInteger counter) {
    Random random = new Random();
    while (running.get()) {
      jedisCluster.get("key-" + random.nextInt(counter.get() + 1));
    }
    return null;
  }

}
