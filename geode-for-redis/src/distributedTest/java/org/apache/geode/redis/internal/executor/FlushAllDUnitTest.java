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

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class FlushAllDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

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

    jedis1 = new Jedis(BIND_ADDRESS, redisServerPort1, REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis(BIND_ADDRESS, redisServerPort2, REDIS_CLIENT_TIMEOUT);
    jedisCluster = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1),
        REDIS_CLIENT_TIMEOUT);
  }

  @AfterClass
  public static void teardown() {
    jedis1.close();
    jedis2.close();
  }

  @Before
  public void setup() {
    int ENTRIES = 1000;
    for (int i = 0; i < ENTRIES; i++) {
      jedisCluster.set("key-" + i, "value-" + i);
    }
  }

  @Test
  public void flushAllOnlyDeletesOnOneMember() {
    jedis1.flushAll();

    int keysRemaining = keyCount();

    assertThat(keysRemaining).isLessThan(ENTRIES);
    assertThat(keysRemaining).isGreaterThan(0);

    jedis2.flushAll();

    keysRemaining = keyCount();

    assertThat(keysRemaining).isEqualTo(0);
  }

  @Test
  public void redisClusterStartupRule_flushAll_works() {
    cluster.flushAll();

    assertThat(keyCount()).isEqualTo(0);
  }

  private int keyCount() {
    int keys = 0;
    for (int i = 0; i < ENTRIES; i++) {
      keys += jedisCluster.get("key-" + i) != null ? 1 : 0;
    }

    return keys;
  }
}
