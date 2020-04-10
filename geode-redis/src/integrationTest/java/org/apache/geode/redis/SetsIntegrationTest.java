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
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

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

  @Test
  public void testSAddSCard() {
    int elements = 10;
    Set<String> strings = new HashSet<String>();
    String key = generator.generate('x');
    generateStrings(elements, strings, 'x');
    String[] stringArray = strings.toArray(new String[strings.size()]);

    Long response = jedis.sadd(key, stringArray);
    assertThat(response).isEqualTo(strings.size());

    Long response2 = jedis.sadd(key, stringArray);
    assertThat(response2).isEqualTo(0L);

    assertThat(jedis.scard(key)).isEqualTo(strings.size());
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

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
    Set<String> strings1 = new HashSet<String>();
    Set<String> strings2 = new HashSet<String>();
    String key = generator.generate('x');
    generateStrings(elements, strings1, 'y');
    generateStrings(elements, strings2, 'z');

    String[] stringArray1 = strings1.toArray(new String[strings1.size()]);
    String[] stringArray2 = strings2.toArray(new String[strings2.size()]);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfSAdds(key, stringArray1, jedis);
    Callable<Integer> callable2 = () -> doABunchOfSAdds(key, stringArray2, jedis2);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    assertThat(future1.get()).isEqualTo(strings1.size());
    assertThat(future2.get()).isEqualTo(strings2.size());

    assertThat(jedis.scard(key)).isEqualTo(strings1.size() + strings2.size());

    pool.shutdown();
  }

  @Test
  public void testConcurrentSAddSCard_differentKeyPerClient()
      throws InterruptedException, ExecutionException {
    int elements = 1000;
    Set<String> strings = new HashSet<String>();
    String key1 = generator.generate('x');
    String key2 = generator.generate('y');
    generateStrings(elements, strings, 'y');

    String[] stringArray = strings.toArray(new String[strings.size()]);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfSAdds(key1, stringArray, jedis);
    Callable<Integer> callable2 = () -> doABunchOfSAdds(key2, stringArray, jedis2);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    assertThat(future1.get()).isEqualTo(strings.size());
    assertThat(future2.get()).isEqualTo(strings.size());

    assertThat(jedis.scard(key1)).isEqualTo(strings.size());
    assertThat(jedis.scard(key2)).isEqualTo(strings.size());

    pool.shutdown();
  }

  private void generateStrings(int elements, Set<String> strings, char separator) {
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate(separator);
      strings.add(elem);
    }
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
    Set<String> strings = new HashSet<String>();
    String key = generator.generate('x');
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate('x');
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.sadd(key, stringArray);

    Set<String> returnedSet = jedis.smembers(key);

    assertThat(returnedSet).isEqualTo(strings);

    for (String entry : strings) {
      assertThat(jedis.sismember(key, entry)).isTrue();
    }

    assertThat(jedis.smembers("doesNotExist")).isEmpty();

    assertThat(jedis.sismember("nonExistentKey", "nonExistentMember")).isFalse();
    assertThat(jedis.sismember(key, "nonExistentMember")).isFalse();
  }

  @Test
  public void testSMove() {
    String source = generator.generate('x');
    String dest = generator.generate('x');
    String test = generator.generate('x');
    int elements = 10;
    Set<String> strings = new HashSet<String>();
    generateStrings(elements, strings, 'x');
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.sadd(source, stringArray);

    long i = 1;
    for (String entry : strings) {
      long results = jedis.smove(source, dest, entry);
      assertThat(results).isEqualTo(1);
      assertThat(jedis.sismember(dest, entry)).isTrue();

      results = jedis.scard(source);
      assertThat(results).isEqualTo(strings.size() - i);
      assertThat(jedis.scard(dest)).isEqualTo(i);
      i++;
    }

    assertThat(jedis.smove(test, dest, generator.generate('x'))).isEqualTo(0);
  }

  @Test
  public void testSMoveNegativeCases() {
    String source = "source";
    String dest = "dest";
    jedis.sadd(source, "sourceField");
    jedis.sadd(dest, "destField");
    String nonexistentField = "nonexistentField";

    assertThat(jedis.smove(source, dest, nonexistentField)).isEqualTo(0);
    assertThat(jedis.sismember(dest, nonexistentField)).isFalse();
    assertThat(jedis.smove(source, "nonexistentDest", nonexistentField)).isEqualTo(0);
    assertThat(jedis.smove("nonExistentSource", dest, nonexistentField)).isEqualTo(0);
  }

  @Test
  public void testConcurrentSMove() throws ExecutionException, InterruptedException {
    String source = generator.generate('x');
    String dest = generator.generate('y');
    int elements = 1000;
    Set<String> strings = new HashSet<String>();
    generateStrings(elements, strings, 'x');
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.sadd(source, stringArray);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Long> callable1 = () -> moveSetElements(source, dest, strings, jedis);
    Callable<Long> callable2 = () -> moveSetElements(source, dest, strings, jedis2);
    Future<Long> future1 = pool.submit(callable1);
    Future<Long> future2 = pool.submit(callable2);

    assertThat(future1.get() + future2.get()).isEqualTo(new Long(strings.size()));
    assertThat(jedis.smembers(dest).toArray()).containsExactlyInAnyOrder(strings.toArray());
    assertThat(jedis.scard(source)).isEqualTo(0L);
  }

  private long moveSetElements(String source, String dest, Set<String> strings,
      Jedis jedis) {
    long results = 0;
    for (String entry : strings) {
      results += jedis.smove(source, dest, entry);
      Thread.yield();
    }
    return results;
  }

  @Test
  public void testSDiff() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Set<String> result = jedis.sdiff("set1", "set2", "set3", "doesNotExist");
    String[] expected = new String[] {"pear", "plum", "orange"};
    assertThat(result).containsExactlyInAnyOrder(expected);

    Set<String> shouldNotChange = jedis.smembers("set1");
    assertThat(shouldNotChange).containsExactlyInAnyOrder(firstSet);

    Set<String> shouldBeEmpty = jedis.sdiff("doesNotExist", "set1", "set2", "set3");
    assertThat(shouldBeEmpty).isEmpty();

    Set<String> copySet = jedis.sdiff("set1");
    assertThat(copySet).containsExactlyInAnyOrder(firstSet);
  }

  @Test
  public void testSDiffStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Long resultSize = jedis.sdiffstore("result", "set1", "set2", "set3");
    Set<String> resultSet = jedis.smembers("result");

    String[] expected = new String[] {"pear", "plum", "orange"};
    assertThat(resultSize).isEqualTo(expected.length);
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long otherResultSize = jedis.sdiffstore("set1", "set1", "result");
    Set<String> otherResultSet = jedis.smembers("set1");
    String[] otherExpected = new String[] {"apple", "peach"};
    assertThat(otherResultSize).isEqualTo(otherExpected.length);
    assertThat(otherResultSet).containsExactlyInAnyOrder(otherExpected);

    Long emptySetSize = jedis.sdiffstore("newEmpty", "nonexistent", "set2", "set3");
    Set<String> emptyResultSet = jedis.smembers("newEmpty");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    emptySetSize = jedis.sdiffstore("set1", "nonexistent", "set2", "set3");
    emptyResultSet = jedis.smembers("set1");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    Long copySetSize = jedis.sdiffstore("copySet", "set2");
    Set<String> copyResultSet = jedis.smembers("copySet");
    assertThat(copySetSize).isEqualTo(secondSet.length);
    assertThat(copyResultSet.toArray()).containsExactlyInAnyOrder((Object[]) secondSet);
  }

  @Test
  public void testConcurrentSDiffStore() throws InterruptedException {
    int ENTRIES = 100;
    int SUBSET_SIZE = 100;

    Set<String> masterSet = new HashSet<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    List<Set<String>> otherSets = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      Set<String> oneSet = new HashSet<>();
      for (int j = 0; j < SUBSET_SIZE; j++) {
        oneSet.add("set-" + i + "-" + j);
      }
      otherSets.add(oneSet);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("set-" + i, otherSets.get(i).toArray(new String[] {}));
      jedis.sadd("master", otherSets.get(i).toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sdiffstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis2.sdiffstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void testSInter() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Set<String> resultSet = jedis.sinter("set1", "set2", "set3");

    String[] expected = new String[] {"peach"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Set<String> emptyResultSet = jedis.sinter("nonexistent", "set2", "set3");
    assertThat(emptyResultSet).isEmpty();

    jedis.sadd("newEmpty", "born2die");
    jedis.srem("newEmpty", "born2die");
    Set<String> otherEmptyResultSet = jedis.sinter("set2", "newEmpty");
    assertThat(otherEmptyResultSet).isEmpty();
  }

  @Test
  public void testSInterStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Long resultSize = jedis.sinterstore("result", "set1", "set2", "set3");
    Set<String> resultSet = jedis.smembers("result");

    String[] expected = new String[] {"peach"};
    assertThat(resultSize).isEqualTo(expected.length);
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long otherResultSize = jedis.sinterstore("set1", "set1", "set2");
    Set<String> otherResultSet = jedis.smembers("set1");
    String[] otherExpected = new String[] {"apple", "peach"};
    assertThat(otherResultSize).isEqualTo(otherExpected.length);
    assertThat(otherResultSet).containsExactlyInAnyOrder(otherExpected);

    Long emptySetSize = jedis.sinterstore("newEmpty", "nonexistent", "set2", "set3");
    Set<String> emptyResultSet = jedis.smembers("newEmpty");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    emptySetSize = jedis.sinterstore("set1", "nonexistent", "set2", "set3");
    emptyResultSet = jedis.smembers("set1");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    Long copySetSize = jedis.sinterstore("copySet", "set2", "newEmpty");
    Set<String> copyResultSet = jedis.smembers("copySet");
    assertThat(copySetSize).isEqualTo(0);
    assertThat(copyResultSet).isEmpty();
  }

  @Test
  public void testConcurrentSInterStore() throws InterruptedException {
    int ENTRIES = 100;
    int SUBSET_SIZE = 100;

    Set<String> masterSet = new HashSet<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    List<Set<String>> otherSets = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      Set<String> oneSet = new HashSet<>();
      for (int j = 0; j < SUBSET_SIZE; j++) {
        oneSet.add("set-" + i + "-" + j);
      }
      otherSets.add(oneSet);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("set-" + i, otherSets.get(i).toArray(new String[] {}));
      jedis.sadd("set-" + i, masterSet.toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sinterstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis2.sinterstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void testSUnion() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Set<String> resultSet = jedis.sunion("set1", "set2");
    String[] expected =
        new String[] {"pear", "apple", "plum", "orange", "peach", "microsoft", "linux"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Set<String> otherResultSet = jedis.sunion("nonexistent", "set1");
    assertThat(otherResultSet).containsExactlyInAnyOrder(firstSet);

    jedis.sadd("newEmpty", "born2die");
    jedis.srem("newEmpty", "born2die");
    Set<String> yetAnotherResultSet = jedis.sunion("set2", "newEmpty");
    assertThat(yetAnotherResultSet).containsExactlyInAnyOrder(secondSet);
  }

  @Test
  public void testSUnionStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Long resultSize = jedis.sunionstore("result", "set1", "set2");
    assertThat(resultSize).isEqualTo(7);

    Set<String> resultSet = jedis.smembers("result");
    String[] expected =
        new String[] {"pear", "apple", "plum", "orange", "peach", "microsoft", "linux"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long notEmptyResultSize = jedis.sunionstore("notempty", "nonexistent", "set1");
    Set<String> notEmptyResultSet = jedis.smembers("notempty");
    assertThat(notEmptyResultSize).isEqualTo(firstSet.length);
    assertThat(notEmptyResultSet).containsExactlyInAnyOrder(firstSet);

    jedis.sadd("newEmpty", "born2die");
    jedis.srem("newEmpty", "born2die");
    Long newNotEmptySize = jedis.sunionstore("newNotEmpty", "set2", "newEmpty");
    Set<String> newNotEmptySet = jedis.smembers("newNotEmpty");
    assertThat(newNotEmptySize).isEqualTo(secondSet.length);
    assertThat(newNotEmptySet).containsExactlyInAnyOrder(secondSet);
  }

  @Test
  public void testConcurrentSUnionStore() throws InterruptedException {
    int ENTRIES = 100;
    int SUBSET_SIZE = 100;

    Set<String> masterSet = new HashSet<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    List<Set<String>> otherSets = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      Set<String> oneSet = new HashSet<>();
      for (int j = 0; j < SUBSET_SIZE; j++) {
        oneSet.add("set-" + i + "-" + j);
      }
      otherSets.add(oneSet);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("set-" + i, otherSets.get(i).toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sunionstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis2.sunionstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    otherSets.forEach(masterSet::addAll);

    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

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
  public void testSPopWithCount() {
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

  @Test
  public void testSRem_should_ReturnTheNumberOfElementsRemoved() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    Long removedElementsCount = jedis.srem(key, field1, field2);

    assertThat(removedElementsCount).isEqualTo(2);
  }

  @Test
  public void testSRem_should_RemoveSpecifiedElementsFromSet() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    jedis.srem(key, field2);

    Set<String> membersInSet = jedis.smembers(key);
    assertThat(membersInSet).doesNotContain(field2);
  }

  @Test
  public void testSRem_should_ThrowError_givenOnlyKey() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    Throwable caughtException = catchThrowable(
        () -> jedis.srem(key));

    assertThat(caughtException).hasMessageContaining("wrong number of arguments");
  }

  @Test
  public void testSRem_should_notRemoveMembersOfSetNotSpecified() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    jedis.srem(key, field1);

    Set<String> membersInSet = jedis.smembers(key);
    assertThat(membersInSet).containsExactly(field2);
  }

  @Test
  public void testSRem_should_ignoreMembersNotInSpecifiedSet() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    String unkownField = "random-guy";
    jedis.sadd(key, field1, field2);

    long result = jedis.srem(key, unkownField);

    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testSRem_should_throwError_givenKeyThatIsNotASet() {
    String key = "key";
    String value = "value";
    jedis.set(key, value);

    Throwable caughtException = catchThrowable(
        () -> jedis.srem(key, value));

    assertThat(caughtException)
        .hasMessageContaining(
            "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  @Test
  public void testSRem_shouldReturnZero_givenNonExistingKey() {
    String key = "key";
    String field = "field";

    long result = jedis.srem(key, field);

    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testConcurrentSRems() throws InterruptedException {
    int ENTRIES = 1000;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    AtomicLong sremmed1 = new AtomicLong(0);
    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        sremmed1.addAndGet(jedis.srem("master", masterSet.get(i)));
        Thread.yield();
      }
    };

    AtomicLong sremmed2 = new AtomicLong(0);
    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        sremmed2.addAndGet(jedis2.srem("master", masterSet.get(i)));
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master")).isEmpty();
    assertThat(sremmed1.get() + sremmed2.get()).isEqualTo(ENTRIES);
  }
}
