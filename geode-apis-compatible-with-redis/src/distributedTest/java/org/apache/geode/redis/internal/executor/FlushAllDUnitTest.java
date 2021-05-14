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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class FlushAllDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int ENTRIES = 1000;

  private static MemberVM locator;
  private static MemberVM server1;

  private static Jedis jedis1;
  private static Jedis jedis2;
  private static JedisCluster jedisCluster;

  @BeforeClass
  public static void classSetup() {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startRedisVM(1, locator.getPort());
    cluster.startRedisVM(2, locator.getPort());

    int redisServerPort1 = cluster.getRedisPort(1);
    int redisServerPort2 = cluster.getRedisPort(2);

    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
    jedisCluster = new JedisCluster(new HostAndPort("localhost", redisServerPort1), JEDIS_TIMEOUT);
  }

  @AfterClass
  public static void teardown() {
    jedis1.close();
    jedis2.close();
  }

  @Test
  public void flushAllOnlyDeletesOnOneMember() {
    int ENTRIES = 1000;
    for (int i = 0; i < ENTRIES; i++) {
      jedisCluster.set("key-" + i, "value-" + i);
    }

    jedis1.flushAll();

    int keysRemaining = keyCount();

    assertThat(keysRemaining).isLessThan(ENTRIES);
    assertThat(keysRemaining).isGreaterThan(0);

    jedis2.flushAll();

    keysRemaining = keyCount();

    assertThat(keysRemaining).isEqualTo(0);
  }

  private int keyCount() {
    int keys = 0;
    for (int i = 0; i < ENTRIES; i++) {
      keys += jedisCluster.get("key-" + i) != null ? 1 : 0;
    }

    return keys;
  }
}
