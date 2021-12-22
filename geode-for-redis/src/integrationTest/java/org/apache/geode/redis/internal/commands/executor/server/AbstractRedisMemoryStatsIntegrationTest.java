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
package org.apache.geode.redis.internal.commands.executor.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.RedisTestHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractRedisMemoryStatsIntegrationTest implements RedisIntegrationTest {

  private static final int TIMEOUT = (int) GeodeAwaitility.getTimeout().toMillis();
  private static final String EXISTING_HASH_KEY = "Existing_Hash";

  private Jedis jedis;

  private static final String MAX_MEMORY = "maxmemory";
  private static final String USED_MEMORY = "used_memory";
  private static final String MEM_FRAGMENTATION_RATIO = "mem_fragmentation_ratio";

  public void configureMemoryAndEvictionPolicy(Jedis jedis) {}

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
    long maxMemory = Long.parseLong(RedisTestHelper.getInfo(jedis).get(MAX_MEMORY));
    assertThat(maxMemory).isGreaterThan(0L);
  }

  @Test
  public void memoryFragmentationRatio_shouldBeGreaterThanZero() {
    double memoryFragmentationRatio =
        Double.parseDouble(RedisTestHelper.getInfo(jedis).get(MEM_FRAGMENTATION_RATIO));
    assertThat(memoryFragmentationRatio).isGreaterThan(0.0);
  }

  @Test
  public void usedMemory_shouldIncrease_givenAdditionalValuesAdded() {
    long initialUsedMemory = Long.parseLong(RedisTestHelper.getInfo(jedis).get(USED_MEMORY));
    long finalUsedMemory = 0;

    for (int i = 0; i < 1_000_000; i++) {
      jedis.set("key-" + i, "some kinda long value that just wastes a bunch of memory - " + i);

      // Check every 50,000 entries to see if we've increased in memory.
      if (i % 50_000 == 0) {
        finalUsedMemory = Long.parseLong(RedisTestHelper.getInfo(jedis).get(USED_MEMORY));
        if (finalUsedMemory > initialUsedMemory) {
          return;
        }
      }
    }

    fail(String.format(
        "Memory did not increase. Expected final size to be greater than initial size - %d > %d",
        finalUsedMemory, initialUsedMemory));
  }

}
