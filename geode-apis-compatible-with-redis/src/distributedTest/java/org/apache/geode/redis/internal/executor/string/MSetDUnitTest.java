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

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class MSetDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final String HASHTAG = "{tag}";
  private static JedisCluster jedis;
  private static MemberVM server1;
  private static int locatorPort;

  private static MemberVM locator;

  @BeforeClass
  public static void classSetup() {
    locator = clusterStartUp.startLocatorVM(0);
    locatorPort = locator.getPort();
    server1 = clusterStartUp.startRedisVM(1, locatorPort);
    clusterStartUp.startRedisVM(2, locatorPort);
    clusterStartUp.startRedisVM(3, locatorPort);

    int redisServerPort1 = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), 5000, 20);
  }

  @After
  public void after() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
  }

  @Test
  public void testMSet_concurrentInstancesHandleBucketMovement() {
    int KEY_COUNT = 5000;
    String[] keys = new String[KEY_COUNT];

    for (int i = 0; i < keys.length; i++) {
      keys[i] = HASHTAG + "key" + i;
    }
    String[] keysAndValues1 = makeKeysAndValues(keys, "valueOne");
    String[] keysAndValues2 = makeKeysAndValues(keys, "valueTwo");

    new ConcurrentLoopingThreads(100,
        i -> jedis.mset(keysAndValues1),
        i -> jedis.mset(keysAndValues2),
        i -> clusterStartUp.moveBucketForKey(keys[0]))
            .runWithAction(() -> {
              int count = 0;
              List<String> values = jedis.mget(keys);
              for (String v : values) {
                if (v == null) {
                  continue;
                }
                count += v.startsWith("valueOne") ? 1 : -1;
              }
              assertThat(Math.abs(count)).isEqualTo(KEY_COUNT);
            });
  }

  @Test
  public void testMSet_crashDoesNotLeaveInconsistencies() throws Exception {
    int KEY_COUNT = 1000;
    String[] keys = new String[KEY_COUNT];

    for (int i = 0; i < keys.length; i++) {
      keys[i] = HASHTAG + "key" + i;
    }
    String[] keysAndValues1 = makeKeysAndValues(keys, "valueOne");
    String[] keysAndValues2 = makeKeysAndValues(keys, "valueTwo");
    AtomicBoolean running = new AtomicBoolean(true);

    Future<?> future = executor.submit(() -> {
      for (int i = 0; i < 20 && running.get(); i++) {
        clusterStartUp.moveBucketForKey(keys[0], "server-3");
        // Sleep for a bit so that MSETs can execute
        Thread.sleep(2000);
        clusterStartUp.crashVM(3);
        clusterStartUp.startRedisVM(3, locatorPort);
        rebalanceAllRegions(server1);
      }
      running.set(false);
    });

    try {
      AtomicInteger loopCounter = new AtomicInteger(0);
      new ConcurrentLoopingThreads(running,
          i -> jedis.mset(keysAndValues1),
          i -> jedis.mset(keysAndValues2))
              .runWithAction(() -> {
                logger.info("--->>> Validation STARTING: Loop count = {}  {}",
                    loopCounter.incrementAndGet(), running.get());
                int count = 0;
                List<String> values = jedis.mget(keys);
                for (String v : values) {
                  if (v == null) {
                    continue;
                  }
                  count += v.startsWith("valueOne") ? 1 : -1;
                }
                assertThat(Math.abs(count)).isEqualTo(KEY_COUNT);
                logger.info("--->>> Validation DONE");
              });
    } finally {
      running.set(false);
      future.get();
    }
  }

  private String[] makeKeysAndValues(String[] keys, String valueBase) {
    String[] keysValues = new String[keys.length * 2];
    for (int i = 0; i < keys.length * 2; i += 2) {
      keysValues[i] = keys[i / 2];
      keysValues[i + 1] = valueBase + i;
    }

    return keysValues;
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
