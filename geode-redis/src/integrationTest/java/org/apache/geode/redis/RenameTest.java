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

package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class RenameTest {
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static Random rand;
  private static Jedis client;
  private static int port = 6379;
  private static ThreePhraseGenerator generator = new ThreePhraseGenerator();

  @BeforeClass
  public static void setUp() {
    rand = new Random();
    CacheFactory cacheFactory = new CacheFactory();
    cacheFactory.set(LOG_LEVEL, "info");
    cacheFactory.set(MCAST_PORT, "0");
    cacheFactory.set(LOCATORS, "");
    cache = cacheFactory.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);
    client = new Jedis("localhost", port, 100000);

    server.start();
  }

  @AfterClass
  public static void tearDown() {
    client.close();
    cache.close();
    server.shutdown();
  }

  @After
  public void flush() {
    client.flushAll();
  }

  @Test
  public void testNewKey() {
    client.set("foo", "bar");
    client.rename("foo", "newfoo");
    assertThat(client.get("newfoo")).isEqualTo("bar");
  }

  @Test
  public void testOldKeyIsDeleted() {
    client.set("foo", "bar");
    client.rename("foo", "newfoo");
    assertThat(client.get("foo")).isNull();
  }

  @Test
  public void testRenameKeyThatDoesNotExist() {
    String renameResult = client.rename("foo", "newfoo");
    assertThat(renameResult).isNull();
  }

  @Test
  public void testProtectedString() {
    client.set("foo", "bar");
    assertThatThrownBy(() -> client.rename("foo", GeodeRedisServer.STRING_REGION));
  }

  @Test
  public void testHashMap() {
    client.hset("foo", "field", "va");
    client.rename("foo", "newfoo");
    assertThat(client.hget("newfoo", "field")).isEqualTo("va");
  }

  @Test
  public void testSet() {
    client.sadd("foo", "data");
    client.rename("foo", "newfoo");
    assertThat(client.smembers("newfoo")).contains("data");
  }

  @Test
  public void testSortedSet() {
    client.zadd("foo", 1.0, "data");
    assertThatThrownBy(() -> client.rename("foo", "newfoo"));
  }

  @Test
  public void testList() {
    client.lpush("person", "Bern");
    assertThatThrownBy(() -> client.rename("person", "newPerson"));
  }

  @Test
  public void testConcurrentSets() throws ExecutionException, InterruptedException {
    Set<String> stringsForK1 = new HashSet<String>();
    Set<String> stringsForK2 = new HashSet<String>();

    Jedis client2 = new Jedis("localhost", port, 10000000);
    Jedis client3 = new Jedis("localhost", port, 10000000);

    int numOfStrings = 500000;
    Callable<Long> callable1 = () -> {
      return addStringsToKeys(stringsForK1, "k1", numOfStrings, client);
    };
    int numOfStringsForSecondKey = 30000;
    Callable<Long> callable2 =
        () -> addStringsToKeys(stringsForK2, "k2", numOfStringsForSecondKey, client2);
    Callable<String> callable3 = () -> renameKeys(client3);

    ExecutorService pool = Executors.newFixedThreadPool(4);
    Future<Long> future1 = pool.submit(callable1);
    Future<Long> future2 = pool.submit(callable2);
    Future<String> future3 = pool.submit(callable3);

    future1.get();
    future2.get();
    String renameResult = future3.get();

    if (renameResult == null) {
      assertThat(client.scard("k1")).isEqualTo(numOfStrings);
      assertThat(client.scard("k2")).isEqualTo(numOfStringsForSecondKey);
    } else {
      assertThat(client.scard("k2")).isEqualTo(numOfStrings);
      assertThat(client.get("k1")).isEqualTo(null);
    }
  }


  private Long addStringsToKeys(Set<String> strings, String key, int numOfStrings, Jedis client) {
    generateStrings(numOfStrings, strings);
    String[] stringArray = strings.toArray(new String[strings.size()]);
    return client.sadd(key, stringArray);
  }

  private String renameKeys(Jedis client) {
    return client.rename("k1", "k2");
  }

  private Set<String> generateStrings(int elements, Set<String> strings) {
    for (int i = 0; i < elements; i++) {
      String elem = String.valueOf(i);
      strings.add(elem);
    }
    return strings;
  }
}
