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

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class InfoDUnitTest {
  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int NUM_ITERATIONS = 1000;
  private static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final String COMMANDS_PROCESSED = "total_commands_processed";
  private static final String REDIS_TCP_PORT = "tcp_port";

  private static Jedis jedis1;
  private static Jedis jedis2;

  private static MemberVM server1;
  private static MemberVM server2;

  private static int redisServerPort1;
  private static int redisServerPort2;

  @BeforeClass
  public static void classSetup() {
    Properties locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    MemberVM locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());

    redisServerPort1 = clusterStartUp.getRedisPort(1);
    redisServerPort2 = clusterStartUp.getRedisPort(2);

    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    jedis1.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
    jedis2.disconnect();

    server1.stop();
    server2.stop();
  }

  @Test
  public void testConcurrentInfoFromDifferentServers_doNotStepOnEachOther() {
    jedis1.set("key1", "value1");
    jedis1.set("key2", "value2");
    jedis1.set("key3", "value3");
    jedis2.set("key3", "value3");

    AtomicInteger previousCommandsProcessed1 = new AtomicInteger(4);
    AtomicInteger previousCommandsProcessed2 = new AtomicInteger(1);
    assertThat(Integer.valueOf(getInfo(jedis1).get(COMMANDS_PROCESSED))).isEqualTo(4);

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        i -> {
          Map<String, String> info1 = getInfo(jedis1);
          assertThat(Integer.valueOf(info1.get(REDIS_TCP_PORT))).isEqualTo(redisServerPort1);

          int commandsProcessed1 = Integer.parseInt(info1.get(COMMANDS_PROCESSED));
          assertThat(commandsProcessed1).isGreaterThanOrEqualTo(previousCommandsProcessed1.get());
          previousCommandsProcessed1.set(commandsProcessed1);
        },
        i -> {
          Map<String, String> info2 = getInfo(jedis2);
          assertThat(Integer.valueOf(info2.get(REDIS_TCP_PORT))).isEqualTo(redisServerPort2);

          int commandsProcessed2 = Integer.parseInt(info2.get(COMMANDS_PROCESSED));
          assertThat(commandsProcessed2).isGreaterThanOrEqualTo(previousCommandsProcessed2.get());
          previousCommandsProcessed2.set(commandsProcessed2);
        }).run();
  }

  /**
   * Convert the values returned by the INFO command into a basic param:value map.
   */
  static synchronized Map<String, String> getInfo(Jedis jedis) {
    Map<String, String> results = new HashMap<>();
    String rawInfo = jedis.info();

    for (String line : rawInfo.split("\r\n")) {
      int colonIndex = line.indexOf(":");
      if (colonIndex > 0) {
        String key = line.substring(0, colonIndex);
        String value = line.substring(colonIndex + 1);
        results.put(key, value);
      }
    }

    return results;
  }
}
