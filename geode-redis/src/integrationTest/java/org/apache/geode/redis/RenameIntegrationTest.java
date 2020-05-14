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
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class RenameIntegrationTest {
  static Jedis jedis;
  static final int REDIS_CLIENT_TIMEOUT = 100000;
  static Random rand;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
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
    jedis = new Jedis("localhost", port, REDIS_CLIENT_TIMEOUT);

    server.start();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    cache.close();
    server.shutdown();
  }

  public int getPort() {
    return port;
  }

  @After
  public void flush() {
    jedis.flushAll();
  }

  @Test
  public void testNewKey() {
    jedis.set("foo", "bar");
    jedis.rename("foo", "newfoo");
    assertThat(jedis.get("newfoo")).isEqualTo("bar");
  }

  @Test
  public void testOldKeyIsDeleted() {
    jedis.set("foo", "bar");
    jedis.rename("foo", "newfoo");
    assertThat(jedis.get("foo")).isNull();
  }

  @Test
  public void testRenameKeyThatDoesNotExist() {
    try {
      jedis.rename("foo", "newfoo");
    } catch (JedisDataException e) {
      assertThat(e.getMessage()).contains(RedisConstants.ERROR_NO_SUCH_KEY);
    }
  }

  @Test
  public void testProtectedString() {
    jedis.set("foo", "bar");
    assertThatThrownBy(() -> jedis.rename("foo", GeodeRedisServer.STRING_REGION));
  }

  @Test
  public void testHashMap() {
    jedis.hset("foo", "field", "va");
    jedis.rename("foo", "newfoo");
    assertThat(jedis.hget("newfoo", "field")).isEqualTo("va");
  }

  @Test
  public void testSet() {
    jedis.sadd("foo", "data");
    jedis.rename("foo", "newfoo");
    assertThat(jedis.smembers("newfoo")).contains("data");
  }

  @Test
  public void testSortedSet() {
    jedis.zadd("foo", 1.0, "data");
    assertThatThrownBy(() -> jedis.rename("foo", "newfoo"));
  }

  @Test
  public void testList() {
    jedis.lpush("person", "Bern");
    assertThatThrownBy(() -> jedis.rename("person", "newPerson"));
  }

  @Test
  public void testConcurrentSets() throws ExecutionException, InterruptedException {
    Set<String> stringsForK1 = new HashSet<String>();
    Set<String> stringsForK2 = new HashSet<String>();

    Jedis jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    Jedis jedis3 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);

    int numOfStrings = 500000;
    Callable<Long> callable1 =
        () -> addStringsToKeys(stringsForK1, "k1", numOfStrings, jedis);
    int numOfStringsForSecondKey = 30000;
    Callable<Long> callable2 =
        () -> addStringsToKeys(stringsForK2, "k2", numOfStringsForSecondKey, jedis2);
    Callable<String> callable3 = () -> renameKeys(jedis3);

    ExecutorService pool = Executors.newFixedThreadPool(4);
    Future<Long> future1 = pool.submit(callable1);
    Future<Long> future2 = pool.submit(callable2);
    Thread.sleep(rand.nextInt(1000));
    Future<String> future3 = pool.submit(callable3);

    future1.get();
    future2.get();
    try {
      future3.get();
      assertThat(jedis.scard("k2")).isEqualTo(numOfStrings);
      assertThat(jedis.get("k1")).isEqualTo(null);
    } catch (Exception e) {
      assertThat(e.getMessage()).contains(RedisConstants.ERROR_NO_SUCH_KEY);
      assertThat(jedis.scard("k1")).isEqualTo(numOfStrings);
      assertThat(jedis.scard("k2")).isEqualTo(numOfStringsForSecondKey);
    }

    jedis2.close();
    jedis3.close();
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
