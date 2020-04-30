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
package org.apache.geode.redis.sets;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class SPopIntegrationTest {
  static Jedis jedis;
  static Jedis jedis2;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static int port = 6379;

  @BeforeClass
  public static void setUp() {
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
    jedis2 = new Jedis("localhost", port, 10000000);
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
    cache.close();
    server.shutdown();
  }

  @After
  public void cleanup() {
    jedis.flushAll();
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testSPop() {
    int ENTRIES = 10;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));
    String poppy = jedis.spop("master");

    masterSet.remove(poppy);
    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());

    assertThat(jedis.spop("spopnonexistent")).isNull();
  }

  @Test
  public void testSPopAll() {
    int ENTRIES = 10;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));
    Set<String> popped = jedis.spop("master", ENTRIES);

    assertThat(jedis.smembers("master").toArray()).isEmpty();
    assertThat(popped.toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void testSPopAllPlusOne() {
    int ENTRIES = 10;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));
    Set<String> popped = jedis.spop("master", ENTRIES + 1);

    assertThat(jedis.smembers("master").toArray()).isEmpty();
    assertThat(popped.toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void testSPopAllMinusOne() {
    int ENTRIES = 10;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));
    Set<String> popped = jedis.spop("master", ENTRIES - 1);

    assertThat(jedis.smembers("master").toArray()).hasSize(1);
    assertThat(popped).hasSize(ENTRIES - 1);
    assertThat(masterSet).containsAll(popped);
  }

  @Test
  public void testManySPops() {
    int ENTRIES = 100;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    List<String> popped = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      popped.add(jedis.spop("master"));
    }

    assertThat(jedis.smembers("master")).isEmpty();
    assertThat(popped.toArray()).containsExactlyInAnyOrder(masterSet.toArray());

    assertThat(jedis.spop("master")).isNull();
  }

  @Test
  public void testConcurrentSPops() throws InterruptedException {
    int ENTRIES = 1000;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    List<String> popped1 = new ArrayList<>();
    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES / 2; i++) {
        popped1.add(jedis.spop("master"));
      }
    };

    List<String> popped2 = new ArrayList<>();
    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES / 2; i++) {
        popped2.add(jedis2.spop("master"));
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master")).isEmpty();

    popped1.addAll(popped2);
    assertThat(popped1.toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }
}
