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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class SetsIntegrationTest {
  static Jedis jedis;
  static Jedis jedis2;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static ThreePhraseGenerator generator = new ThreePhraseGenerator();
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
  public void testSAddSCard() {
    int elements = 10;
    String key = generator.generate('x');
    String[] members = generateStrings(elements, 'x');

    Long response = jedis.sadd(key, members);
    assertThat(response).isEqualTo(members.length);

    Long response2 = jedis.sadd(key, members);
    assertThat(response2).isEqualTo(0L);

    assertThat(jedis.scard(key)).isEqualTo(members.length);
  }

  @Test
  public void testSAdd_withExistingKey_ofWrongType_shouldReturnError() {
    String key = "key";
    String stringValue = "preexistingValue";
    String[] setValue = new String[1];
    setValue[0] = "set value that should never get added";

    exceptionRule.expect(JedisDataException.class);
    exceptionRule.expectMessage(RedisConstants.ERROR_WRONG_TYPE);

    jedis.set(key, stringValue);
    jedis.sadd(key, setValue);
  }

  @Test
  public void testSAdd_withExistingKey_ofWrongType_shouldNotOverWriteExistingKey() {
    String key = "key";
    String stringValue = "preexistingValue";
    String[] setValue = new String[1];
    setValue[0] = "set value that should never get added";

    jedis.set(key, stringValue);

    try {
      jedis.sadd(key, setValue);
    } catch (JedisDataException exception) {
    }

    String result = jedis.get(key);

    assertThat(result).isEqualTo(stringValue);
  }

  @Test
  public void testConcurrentSAddSCard_sameKeyPerClient()
      throws InterruptedException, ExecutionException {
    int elements = 1000;

    String key = generator.generate('x');
    String[] members1 = generateStrings(elements, 'y');
    String[] members2 = generateStrings(elements, 'z');

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfSAdds(key, members1, jedis);
    Callable<Integer> callable2 = () -> doABunchOfSAdds(key, members2, jedis2);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    assertThat(future1.get()).isEqualTo(members1.length);
    assertThat(future2.get()).isEqualTo(members2.length);

    assertThat(jedis.scard(key)).isEqualTo(members1.length + members2.length);

    pool.shutdown();
  }

  @Test
  public void testConcurrentSAddSCard_differentKeyPerClient()
      throws InterruptedException, ExecutionException {
    int elements = 1000;
    String key1 = generator.generate('x');
    String key2 = generator.generate('y');

    String[] strings = generateStrings(elements, 'y');

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfSAdds(key1, strings, jedis);
    Callable<Integer> callable2 = () -> doABunchOfSAdds(key2, strings, jedis2);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    assertThat(future1.get()).isEqualTo(strings.length);
    assertThat(future2.get()).isEqualTo(strings.length);

    assertThat(jedis.scard(key1)).isEqualTo(strings.length);
    assertThat(jedis.scard(key2)).isEqualTo(strings.length);

    pool.shutdown();
  }

  private int doABunchOfSAdds(String key, String[] strings,
      Jedis jedis) {
    int successes = 0;

    for (int i = 0; i < strings.length; i++) {
      Long reply = jedis.sadd(key, strings[i]);
      if (reply == 1L) {
        successes++;
        Thread.yield();
      }
    }
    return successes;
  }

  @Test
  public void testSMembersSIsMember() {
    int elements = 10;
    String key = generator.generate('x');

    String[] strings = generateStrings(elements, 'y');
    jedis.sadd(key, strings);

    Set<String> returnedSet = jedis.smembers(key);

    assertThat(returnedSet).containsExactlyInAnyOrder(strings);

    for (String entry : strings) {
      assertThat(jedis.sismember(key, entry)).isTrue();
    }

    assertThat(jedis.smembers("doesNotExist")).isEmpty();

    assertThat(jedis.sismember("nonExistentKey", "nonExistentMember")).isFalse();
    assertThat(jedis.sismember(key, "nonExistentMember")).isFalse();
  }

  private String[] generateStrings(int elements, char uniqueElement) {
    Set<String> strings = new HashSet<>();
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate(uniqueElement);
      strings.add(elem);
    }
    return strings.toArray(new String[strings.size()]);
  }
}
