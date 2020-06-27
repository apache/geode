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
package org.apache.geode.redis.internal.executor.key;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class ExistsDUnitTest implements Serializable {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule(3);

  private static String LOCALHOST = "localhost";

  private static int server1Port;
  private static int server2Port;

  private static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @BeforeClass
  public static void setup() {
    MemberVM locator = cluster.startLocatorVM(0);

    cluster.startRedisVM(1, locator.getPort());
    cluster.startRedisVM(2, locator.getPort());

    server1Port = cluster.getRedisPort(1);
    server2Port = cluster.getRedisPort(2);
  }

  @Test
  public void testDistributedExistsOperations() {
    Jedis jedis = new Jedis(LOCALHOST, server1Port, JEDIS_TIMEOUT);
    Jedis jedis2 = new Jedis(LOCALHOST, server2Port, JEDIS_TIMEOUT);

    jedis.set("key", "value");
    assertThat(jedis2.exists("key")).isTrue();

    jedis2.del("key");
    assertThat(jedis.exists("key")).isFalse();
  }
}
