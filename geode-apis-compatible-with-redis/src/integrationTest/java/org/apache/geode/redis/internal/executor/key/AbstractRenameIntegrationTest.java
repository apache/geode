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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.executor.SynchronizedStripedExecutor;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractRenameIntegrationTest implements RedisPortSupplier {
  private Jedis jedis;
  private Jedis jedis2;
  private Jedis jedis3;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static Random rand;

  @Before
  public void setUp() {
    rand = new Random();
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    jedis3 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
    jedis2.close();
    jedis3.close();
  }

  @Test
  public void errors_GivenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.RENAME, 2);
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
  public void testRenameSameKey() {
    jedis.set("blue", "moon");
    assertThat(jedis.rename("blue", "blue")).isEqualTo("OK");
    assertThat(jedis.get("blue")).isEqualTo("moon");
  }

  @Test
  public void testConcurrentSets() throws ExecutionException, InterruptedException {
    Set<String> stringsForK1 = new HashSet<String>();
    Set<String> stringsForK2 = new HashSet<String>();

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

  @Test
  public void should_succeed_givenTwoKeysOnDifferentStripes() {
    List<String> listOfKeys = getKeysOnDifferentStripes();
    String oldKey = listOfKeys.get(0);
    String newKey = listOfKeys.get(1);

    jedis.sadd(oldKey, "value1");
    jedis.sadd(newKey, "value2");

    assertThat(jedis.rename(oldKey, newKey)).isEqualTo("OK");
    assertThat(jedis.smembers(newKey)).containsExactly("value1");
    assertThat(jedis.exists(oldKey)).isFalse();
  }

  @Test
  public void should_succeed_givenTwoKeysOnSameStripe() {
    List<String> listOfKeys = new ArrayList<>(getKeysOnSameRandomStripe(2));
    String oldKey = listOfKeys.get(0);
    String newKey = listOfKeys.get(1);

    jedis.sadd(oldKey, "value1");
    jedis.sadd(newKey, "value2");

    assertThat(jedis.rename(oldKey, newKey)).isEqualTo("OK");
    assertThat(jedis.smembers(newKey)).containsExactly("value1");
    assertThat(jedis.exists(oldKey)).isFalse();
  }

  @Test
  public void shouldNotDeadlock_concurrentRenames_givenStripeContention()
      throws ExecutionException, InterruptedException {
    List<String> keysOnStripe1 = new ArrayList<>(getKeysOnSameRandomStripe(2));
    List<String> keysOnStripe2 = getKeysOnSameRandomStripe(2, keysOnStripe1.get(0));

    for (int i = 0; i < 5; i++) {
      doConcurrentRenamesDifferentKeys(
          Arrays.asList(keysOnStripe1.get(0), keysOnStripe2.get(0)),
          Arrays.asList(keysOnStripe2.get(1), keysOnStripe1.get(1)));
    }
  }

  @Test
  public void shouldThrowError_givenKeyDeletedDuringRename()
      throws ExecutionException, InterruptedException {
    CyclicBarrier startCyclicBarrier = new CyclicBarrier(2);
    ExecutorService pool = Executors.newFixedThreadPool(2);

    for (int i = 0; i < 100; i++) {
      jedis.set("oldKey", "foo");

      Runnable renameOldKeyToNewKey = () -> {
        cyclicBarrierAwait(startCyclicBarrier);

        jedis.rename("oldKey", "newKey");
      };

      Runnable deleteOldKey = () -> {
        cyclicBarrierAwait(startCyclicBarrier);

        jedis2.del("oldKey");
      };

      Future<?> future1 = pool.submit(renameOldKeyToNewKey);
      Future<?> future2 = pool.submit(deleteOldKey);

      try {
        future1.get();
        assertThat(jedis.get("newKey")).isEqualTo("foo");
      } catch (Exception e) {
        assertThat(e).hasMessageContaining("no such key");
      }
      future2.get();

      assertThat(jedis.get("oldKey")).isNull();
    }
  }

  @Test
  public void shouldNotDeadlock_concurrentRenames_givenTwoKeysOnDifferentStripe()
      throws ExecutionException, InterruptedException {
    doConcurrentRenamesSameKeys(getKeysOnDifferentStripes());
  }

  @Test
  public void shouldNotDeadlock_concurrentRenames_givenTwoKeysOnSameStripe()
      throws ExecutionException, InterruptedException {
    doConcurrentRenamesSameKeys(new ArrayList<>(getKeysOnSameRandomStripe(2)));
  }

  private List<String> getKeysOnDifferentStripes() {
    String key1 = "keyz" + new Random().nextInt();

    RedisKey key1ByteArrayWrapper = new RedisKey(key1.getBytes());
    StripedExecutor stripedExecutor = new SynchronizedStripedExecutor();
    int iterator = 0;
    String key2;
    do {
      key2 = "key" + iterator;
      iterator++;
    } while (stripedExecutor.compareStripes(key1ByteArrayWrapper,
        new ByteArrayWrapper(key2.getBytes())) == 0);

    return Arrays.asList(key1, key2);
  }

  private Set<String> getKeysOnSameRandomStripe(int numKeysNeeded) {
    Random random = new Random();
    String key1 = "keyz" + random.nextInt();
    RedisKey key1ByteArrayWrapper = new RedisKey(key1.getBytes());
    StripedExecutor stripedExecutor = new SynchronizedStripedExecutor();
    Set<String> keys = new HashSet<>();
    keys.add(key1);

    do {
      String key2 = "key" + random.nextInt();
      if (stripedExecutor.compareStripes(key1ByteArrayWrapper,
          new ByteArrayWrapper(key2.getBytes())) == 0) {
        keys.add(key2);
      }
    } while (keys.size() < numKeysNeeded);

    return keys;
  }

  public void doConcurrentRenamesDifferentKeys(List<String> listOfKeys1, List<String> listOfKeys2)
      throws ExecutionException, InterruptedException {
    CyclicBarrier startCyclicBarrier = new CyclicBarrier(2);

    String oldKey1 = listOfKeys1.get(0);
    String newKey1 = listOfKeys1.get(1);
    String oldKey2 = listOfKeys2.get(0);
    String newKey2 = listOfKeys2.get(1);

    jedis.sadd(oldKey1, "foo", "bar");
    jedis.sadd(oldKey2, "bar3", "back3");

    ExecutorService pool = Executors.newFixedThreadPool(2);

    Runnable renameOldKey1ToNewKey1 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);

      jedis.rename(oldKey1, newKey1);
    };

    Runnable renameOldKey2ToNewKey2 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);

      jedis2.rename(oldKey2, newKey2);
    };

    Future<?> future1 = pool.submit(renameOldKey1ToNewKey1);
    Future<?> future2 = pool.submit(renameOldKey2ToNewKey2);

    future1.get();
    future2.get();
  }

  private void cyclicBarrierAwait(CyclicBarrier startCyclicBarrier) {
    try {
      startCyclicBarrier.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> getKeysOnSameRandomStripe(int numKeysNeeded, Object toAvoid) {

    StripedExecutor stripedExecutor = new SynchronizedStripedExecutor();

    List<String> keys = new ArrayList<>();

    String key1;
    RedisKey key1ByteArrayWrapper;
    do {
      key1 = "keyz" + new Random().nextInt();
      key1ByteArrayWrapper = new RedisKey(key1.getBytes());
    } while (stripedExecutor.compareStripes(key1ByteArrayWrapper, toAvoid) == 0 && keys.add(key1));

    do {
      String key2 = "key" + new Random().nextInt();

      if (stripedExecutor.compareStripes(key1ByteArrayWrapper,
          new ByteArrayWrapper(key2.getBytes())) == 0) {
        keys.add(key2);
      }
    } while (keys.size() < numKeysNeeded);

    return keys;
  }

  public void doConcurrentRenamesSameKeys(List<String> listOfKeys)
      throws ExecutionException, InterruptedException {
    String key1 = listOfKeys.get(0);
    String key2 = listOfKeys.get(1);

    CyclicBarrier startCyclicBarrier = new CyclicBarrier(2);

    jedis.sadd(key1, "foo", "bar");
    jedis.sadd(key2, "bar", "back");

    ExecutorService pool = Executors.newFixedThreadPool(2);

    Runnable renameKey1ToKey2 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis.rename(key1, key2);
    };

    Runnable renameKey2ToKey1 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis2.rename(key2, key1);
    };

    Future<?> future1 = pool.submit(renameKey1ToKey2);
    Future<?> future2 = pool.submit(renameKey2ToKey1);

    future1.get();
    future2.get();
  }

  private Long addStringsToKeys(
      Set<String> strings,
      String key,
      int numOfStrings,
      Jedis client) {
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
