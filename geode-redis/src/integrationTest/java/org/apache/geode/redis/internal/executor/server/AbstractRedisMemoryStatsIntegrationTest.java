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
package org.apache.geode.redis.internal.executor.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.statistics.EnabledStatisticsClock;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractRedisMemoryStatsIntegrationTest implements RedisPortSupplier {

  private static final int TIMEOUT = (int) GeodeAwaitility.getTimeout().toMillis();
  private static final String EXISTING_HASH_KEY = "Existing_Hash";
  private static final String EXISTING_STRING_KEY = "Existing_String";
  private static final String EXISTING_SET_KEY_1 = "Existing_Set_1";
  private static final String EXISTING_SET_KEY_2 = "Existing_Set_2";

  private Jedis jedis;
  private static long START_TIME;
  private static StatisticsClock statisticsClock;

  private long preTestConnectionsReceived = 0;
  private long preTestConnectedClients = 0;

  private static final String MAX_MEMORY = "maxmemory";
  private static final String USED_MEMORY = "used_memory";
  private static final String MEM_FRAGMENTATION_RATIO = "mem_fragmentation_ratio";

  public void configureMemoryAndEvictionPolicy(Jedis jedis) {}

  // ------------------- Setup -------------------------- //
  @BeforeClass
  public static void beforeClass() {
    statisticsClock = new EnabledStatisticsClock();
    START_TIME = statisticsClock.getTime();
  }

  @Before
  public void before() {
    jedis = new Jedis("localhost", getPort(), TIMEOUT);
    configureMemoryAndEvictionPolicy(jedis);
  }

  @After
  public void after() {
    jedis.flushAll();
    jedis.close();
  }

  // ------------------- Memory Section -------------------------- //

  @Test
  public void maxMemory_shouldBeASensibleValue() {
    long maxMemory = Long.valueOf(getInfo(jedis).get(MAX_MEMORY));
    assertThat(maxMemory).isGreaterThan(0L);
  }

  @Test
  public void memoryFragmentationRatio_shouldBeGreaterThanZero() {
    double memoryFragmentationRatio = Double.valueOf(getInfo(jedis).get(MEM_FRAGMENTATION_RATIO));
    assertThat(memoryFragmentationRatio).isGreaterThan(0.0);
  }

  @Test
  public void usedMemory_shouldReflectActualMemoryUsage() {
    long initialUsedMemory = Long.valueOf(getInfo(jedis).get(USED_MEMORY));

    jedis.set(EXISTING_STRING_KEY, "A_Value");
    jedis.hset(EXISTING_HASH_KEY, "Field1", "Value1");
    jedis.sadd(EXISTING_SET_KEY_1, "m1", "m2", "m3");
    jedis.sadd(EXISTING_SET_KEY_2, "m4", "m5", "m6");

    long finalUsedMemory = Long.valueOf(getInfo(jedis).get(USED_MEMORY));
    assertThat(finalUsedMemory).isGreaterThan(initialUsedMemory);
  }

  // ------------------- Helper Methods ----------------------------- //

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
