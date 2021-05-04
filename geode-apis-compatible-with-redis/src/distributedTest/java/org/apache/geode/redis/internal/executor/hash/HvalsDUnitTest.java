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

package org.apache.geode.redis.internal.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class HvalsDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());

    int redisServerPort = clusterStartUp.getRedisPort(1);

    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);

  }

  @Before
  public void testSetup() {
    try (Jedis conn = jedis.getConnectionFromSlot(0)) {
      conn.flushAll();
    }
  }

  @Test
  public void hvalsWorks_whileAlsoUpdatingHash() {
    String key = "key";
    int fieldCount = 100;
    int iterations = 10000;
    Random rand = new Random();

    for (int i = 0; i < fieldCount; i++) {
      jedis.hset(key, "field-" + i, "" + i);
    }

    new ConcurrentLoopingThreads(iterations,
        (i) -> {
          int x = rand.nextInt(fieldCount);
          String field = "field-" + x;
          String currentValue = jedis.hget(key, field);
          jedis.hset(key, field, "" + (Long.parseLong(currentValue) + i));
        },
        (i) -> assertThat(jedis.hvals(key)).hasSize(fieldCount),
        (i) -> assertThat(jedis.hvals(key)).hasSize(fieldCount))
            .run();

    List<String> values = jedis.hvals(key);
    long finalTotal = values.stream().mapToLong(Long::valueOf).sum();

    // Spell out the formula for a sum of an arithmetic sequence which is: (n / 2) * (start + end)
    long sumOfBothSequenceSums = (fieldCount / 2) * ((fieldCount - 1) + 0) +
        (iterations / 2) * ((iterations - 1) + 0);
    assertThat(finalTotal).isEqualTo(sumOfBothSequenceSums);
  }

}
