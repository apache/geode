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
    cluster.startRedisVM(1, locator.getPort());
    cluster.startRedisVM(2, locator.getPort());
    cluster.startRedisVM(3, locator.getPort());
    cluster.startRedisVM(4, locator.getPort());

    cluster.enableDebugLogging(1);

    int redisServerPort1 = cluster.getRedisPort(1);
    jedisCluster =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), 10_000);
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

    Future<Void> future = executor.submit(() -> doOps(running, counter));
    await().until(() -> counter.get() > 100);

    cluster.crashVM(3);
    cluster.crashVM(4);
    int afterCrashCount = counter.get();

    long start = System.currentTimeMillis();
    await().alias("ensure that operations are continuing after multiple server failures")
        .until(() -> counter.get() > afterCrashCount + 10_000);

    long runTime = System.currentTimeMillis() - start;

    running.set(false);

    // Any exception here would fail the test.
    future.get();

    System.err.println("await runtime = " + runTime);
  }

  private Void doOps(AtomicBoolean running, AtomicInteger counter) {
    Random random = new Random();
    while (running.get()) {
      int i = counter.getAndIncrement();
      jedisCluster.set("key-" + i, "value-" + i);
      jedisCluster.get("key-" + random.nextInt(i + 1));
    }
    return null;
  }

}
