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
 *
 */

package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class RedisLockServiceIntegrationTest {

  private static final int REDIS_CLIENT_TIMEOUT = 100000;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static Jedis jedis;
  private static int port = 6379;

  @BeforeClass
  public static void setUp() {
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "warn");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();

    System.setProperty(GeodeRedisServer.DEFAULT_REGION_SYS_PROP_NAME, "REPLICATE");
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);
    server.start();

    jedis = new Jedis("localhost", port, REDIS_CLIENT_TIMEOUT);
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    cache.close();
    server.shutdown();
  }

  @Test
  public void onceLocksAreFreed_thenTheyAreAutomaticallyCleanedUp() {
    for (int i = 0; i < 1000; i++) {
      jedis.sadd("key-" + i, "value-" + i);
    }

    System.gc();
    System.runFinalization();

    GeodeAwaitility.await().until(() -> server.getLockService().getLockCount() == 0);
    assertThat(server.getLockService().getLockCount()).isEqualTo(0);
  }
}
