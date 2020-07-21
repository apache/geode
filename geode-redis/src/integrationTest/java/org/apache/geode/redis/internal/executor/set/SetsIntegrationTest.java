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
package org.apache.geode.redis.internal.executor.set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class SetsIntegrationTest {
  static Jedis jedis;
  static Jedis jedis2;
  private static ThreePhraseGenerator generator = new ThreePhraseGenerator();

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), 10000000);
    jedis2 = new Jedis("localhost", server.getPort(), 10000000);
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
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

    jedis.set(key, stringValue);
    assertThatThrownBy(() -> jedis.sadd(key, setValue))
        .hasMessageContaining("Operation against a key holding the wrong kind of value");
  }

  @Test
  public void testSAdd_withExistingKey_ofWrongType_shouldNotOverWriteExistingKey() {
    String key = "key";
    String stringValue = "preexistingValue";
    String[] setValue = new String[1];
    setValue[0] = "set value that should never get added";

    jedis.set(key, stringValue);

    assertThatThrownBy(() -> jedis.sadd(key, setValue)).isInstanceOf(JedisDataException.class);

    String result = jedis.get(key);

    assertThat(result).isEqualTo(stringValue);
  }

  @Test
  public void testSAdd_canStoreBinaryData() {
    byte[] blob = new byte[256];
    for (int i = 0; i < 256; i++) {
      blob[i] = (byte) i;
    }

    jedis.sadd("key".getBytes(), blob, blob);
    Set<byte[]> result = jedis.smembers("key".getBytes());

    assertThat(result).containsExactly(blob);
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
  public void srandmember_withStringFails() {
    jedis.set("string", "value");
    assertThatThrownBy(() -> jedis.srandmember("string")).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void srandmember_withNonExistentKeyReturnsNull() {
    assertThat(jedis.srandmember("non existent")).isNull();
  }

  @Test
  public void srandmemberCount_withNonExistentKeyReturnsEmptyArray() {
    assertThat(jedis.srandmember("non existent", 3)).isEmpty();
  }

  @Test
  public void srandmember_returnsOneMember() {
    jedis.sadd("key", "m1", "m2");
    String result = jedis.srandmember("key");
    assertThat(result).isIn("m1", "m2");
  }

  @Test
  public void srandmemberCount_returnsTwoUniqueMembers() {
    jedis.sadd("key", "m1", "m2", "m3");
    List<String> results = jedis.srandmember("key", 2);
    assertThat(results).hasSize(2);
    assertThat(results).containsAnyOf("m1", "m2", "m3");
    assertThat(results.get(0)).isNotEqualTo(results.get(1));
  }

  @Test
  public void srandmemberCount_returnsTwoMembers() {
    jedis.sadd("key", "m1", "m2", "m3");
    List<String> results = jedis.srandmember("key", -3);
    assertThat(results).hasSize(3);
    assertThat(results).containsAnyOf("m1", "m2", "m3");
  }

  @Test
  public void testSMembers() {
    int elements = 10;
    String key = generator.generate('x');

    String[] strings = generateStrings(elements, 'y');
    jedis.sadd(key, strings);

    Set<String> returnedSet = jedis.smembers(key);

    assertThat(returnedSet).containsExactlyInAnyOrder(strings);
  }

  @Test
  public void testSMembersWithNonexistentKey_returnsEmptySet() {
    assertThat(jedis.smembers("doesNotExist")).isEmpty();
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
