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

package org.apache.geode.redis.internal.commands.executor.key;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NO_SUCH_KEY;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.services.locking.LockingStripedCoordinator;
import org.apache.geode.redis.internal.services.locking.StripedCoordinator;

public abstract class AbstractRenameNXIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static Random rand;

  @Before
  public void setUp() {
    rand = new Random();
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void errors_GivenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.RENAMENX, 2);
  }

  @Test
  public void shouldRename_givenNewKey() {
    jedis.set("{user1}foo", "bar");
    long result = jedis.renamenx("{user1}foo", "{user1}newfoo");
    assertThat(result).isEqualTo(1L);
    assertThat(jedis.get("{user1}newfoo")).isEqualTo("bar");
  }

  @Test
  public void shouldDeleteOldKey_whenRenamed() {
    jedis.set("{user1}foo", "bar");
    long result = jedis.renamenx("{user1}foo", "{user1}newfoo");
    assertThat(result).isEqualTo(1L);
    assertThat(jedis.get("{user1}foo")).isNull();
  }

  @Test
  public void shouldReturnError_givenNonexistantKey() {
    assertThatThrownBy(() -> jedis.renamenx("{user1}foo", "{user1}newfoo"))
        .hasMessageContaining(ERROR_NO_SUCH_KEY);
  }

  @Test
  public void shouldRename_withHash() {
    jedis.hset("{user1}foo", "field", "va");
    long result = jedis.renamenx("{user1}foo", "{user1}newfoo");
    assertThat(result).isEqualTo(1L);
    assertThat(jedis.hget("{user1}newfoo", "field")).isEqualTo("va");
  }

  @Test
  public void shouldRename_withSet() {
    jedis.sadd("{user1}foo", "data");
    long result = jedis.renamenx("{user1}foo", "{user1}newfoo");
    assertThat(result).isEqualTo(1L);
    assertThat(jedis.smembers("{user1}newfoo")).containsExactly("data");
  }

  @Test
  public void shouldReturnZero_withSameSourceAndTargetKey() {
    jedis.set("{user1}blue", "moon");
    assertThat(jedis.renamenx("{user1}blue", "{user1}blue")).isEqualTo(0L);
    assertThat(jedis.get("{user1}blue")).isEqualTo("moon");
  }

  @Test
  public void shouldNotRename_withExistingTargetKey() {
    jedis.set("{user1}foo1", "bar1");
    jedis.set("{user1}foo12", "bar2");
    long result = jedis.renamenx("{user1}foo1", "{user1}foo12");
    assertThat(result).isEqualTo(0L);
    assertThat(jedis.get("{user1}foo12")).isEqualTo("bar2");
    assertThat(jedis.get("{user1}foo1")).isEqualTo("bar1");
  }

  @Test
  public void repeatedRename_shouldReturnNoSuchKeyError() {
    String oldKey = "{1}key";
    String newKey = "{1}newKey";
    jedis.set(oldKey, "value");
    jedis.renamenx(oldKey, newKey);
    assertThatThrownBy(() -> jedis.renamenx(oldKey, newKey))
        .hasMessageContaining(ERROR_NO_SUCH_KEY);
  }

  @Test
  public void error_whenNeitherKeyExists() {
    String oldKey = "{1}key";
    String newKey = "{1}newKey";
    assertThatThrownBy(() -> jedis.renamenx(oldKey, newKey))
        .hasMessageContaining(ERROR_NO_SUCH_KEY);
  }

  @Test
  public void shouldRenameAtomically() {
    int numIterations = 100;
    int numStringsFirstKey = 500000;
    int numStringsSecondKey = 30000;

    String k1 = "{user1}k1";
    String k2 = "{user1}k2";

    Runnable initAction = () -> {
      flushAll();
      jedis.sadd(k1, "initialEntry");
      assertThat(jedis.exists(k1)).isTrue();
      assertThat(jedis.exists(k2)).isFalse();
      assertThat(jedis.scard(k1)).isEqualTo(1);
    };

    initAction.run();
    new ConcurrentLoopingThreads(numIterations,
        i -> addStringsToKeys(k1, numStringsFirstKey, jedis),
        i -> addStringsToKeys(k2, numStringsSecondKey, jedis),
        i -> {
          // introduce more variability as to when the rename happens
          try {
            Thread.sleep(rand.nextInt(1000));
          } catch (InterruptedException ignored) {
          }
          jedis.renamenx(k1, k2);
        })
            .runWithAction(() -> {
              // three possible results depending on when the rename is executed
              // relative to the other actions
              assertThat(jedis.scard(k2)).satisfiesAnyOf(
                  sizeK2 -> {
                    // rename ran before adds to key1 and before adds to key2
                    assertThat(jedis.exists(k1)).isTrue();
                    assertThat(jedis.scard(k1)).isEqualTo(numStringsFirstKey);
                    assertThat(sizeK2).isEqualTo(numStringsSecondKey + 1);
                    assertThat(jedis.sismember(k2, "initialEntry")).isTrue();
                  },
                  sizeK2 -> {
                    // rename ran after adds to key2
                    assertThat(jedis.exists(k1)).isTrue();
                    assertThat(jedis.scard(k1)).isEqualTo(numStringsFirstKey + 1);
                    assertThat(sizeK2).isEqualTo(numStringsSecondKey);
                    assertThat(jedis.sismember(k1, "initialEntry")).isTrue();
                  },
                  sizeK2 -> {
                    // rename ran after adds to key1 and before adds to key2
                    assertThat(jedis.exists(k1)).isFalse();
                    assertThat(sizeK2).isEqualTo(numStringsSecondKey);
                    assertThat(jedis.sismember(k2, "initialEntry")).isFalse();
                  });
              initAction.run();
            });
  }

  @Test
  public void should_succeed_givenTwoKeysOnDifferentStripes() {
    List<String> listOfKeys = getKeysOnDifferentStripes();
    String oldKey = listOfKeys.get(0);
    String newKey = listOfKeys.get(1);

    jedis.sadd(oldKey, "value1");

    assertThat(jedis.renamenx(oldKey, newKey)).isEqualTo(1L);
    assertThat(jedis.smembers(newKey)).containsExactly("value1");
    assertThat(jedis.exists(oldKey)).isFalse();
  }

  @Test
  public void should_succeed_givenTwoKeysOnSameStripe() {
    List<String> listOfKeys = new ArrayList<>(getKeysOnSameRandomStripe(2));
    String oldKey = listOfKeys.get(0);
    String newKey = listOfKeys.get(1);

    jedis.sadd(oldKey, "value1");

    assertThat(jedis.renamenx(oldKey, newKey)).isEqualTo(1L);
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
  public void shouldError_givenKeyDeletedDuringRename() {
    int iterations = 2000;

    final AtomicReference<RuntimeException> renameException = new AtomicReference<>(null);

    jedis.set("{user1}oldKey", "foo");

    try {
      new ConcurrentLoopingThreads(iterations,
          i -> {
            try {
              jedis.renamenx("{user1}oldKey", "{user1}newKey");
            } catch (RuntimeException e) {
              renameException.set(e);
            }
          },
          i -> jedis.del("{user1}oldKey"))
              .runWithAction(() -> {
                RuntimeException e = renameException.get();
                if (e != null) {
                  throw e;
                }
                assertThat(jedis.get("{user1}newKey")).isEqualTo("foo");
                assertThat(jedis.exists("{user1}oldKey")).isFalse();
                flushAll();
                jedis.set("{user1}oldKey", "foo");
              });
    } catch (RuntimeException e) {
      assertThat(e).hasMessageContaining(ERROR_NO_SUCH_KEY);
      return;
    }

    fail("Did not hit exception after " + iterations + " iterations");
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
    String key1 = "{user1}keyz" + new Random().nextInt();

    RedisKey key1RedisKey = new RedisKey(key1.getBytes());
    StripedCoordinator stripedCoordinator = new LockingStripedCoordinator();
    int iterator = 0;
    String key2;
    do {
      key2 = "{user1}key" + iterator;
      iterator++;
    } while (stripedCoordinator.compareStripes(key1RedisKey,
        new RedisKey(key2.getBytes())) == 0);

    return Arrays.asList(key1, key2);
  }

  private Set<String> getKeysOnSameRandomStripe(int numKeysNeeded) {
    Random random = new Random();
    String key1 = "{user1}keyz" + random.nextInt();
    RedisKey key1RedisKey = new RedisKey(key1.getBytes());
    StripedCoordinator stripedCoordinator = new LockingStripedCoordinator();
    Set<String> keys = new HashSet<>();
    keys.add(key1);

    do {
      String key2 = "{user1}key" + random.nextInt();
      if (stripedCoordinator.compareStripes(key1RedisKey,
          new RedisKey(key2.getBytes())) == 0) {
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

      jedis.renamenx(oldKey1, newKey1);
    };

    Runnable renameOldKey2ToNewKey2 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis.renamenx(oldKey2, newKey2);
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

    StripedCoordinator stripedCoordinator = new LockingStripedCoordinator();

    List<String> keys = new ArrayList<>();

    String key1;
    RedisKey key1RedisKey;
    do {
      key1 = "{user1}keyz" + new Random().nextInt();
      key1RedisKey = new RedisKey(key1.getBytes());
    } while (stripedCoordinator.compareStripes(key1RedisKey, toAvoid) == 0 && keys.add(key1));

    do {
      String key2 = "{user1}key" + new Random().nextInt();

      if (stripedCoordinator.compareStripes(key1RedisKey,
          new RedisKey(key2.getBytes())) == 0) {
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
      jedis.renamenx(key1, key2);
    };

    Runnable renameKey2ToKey1 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis.renamenx(key2, key1);
    };

    Future<?> future1 = pool.submit(renameKey1ToKey2);
    Future<?> future2 = pool.submit(renameKey2ToKey1);

    future1.get();
    future2.get();
  }

  private Long addStringsToKeys(String key, int numOfStrings,
      JedisCluster client) {
    Set<String> strings = new HashSet<>();
    generateStrings(numOfStrings, strings);
    String[] stringArray = strings.toArray(new String[strings.size()]);
    return client.sadd(key, stringArray);
  }

  private Set<String> generateStrings(int elements, Set<String> strings) {
    for (int i = 0; i < elements; i++) {
      String elem = String.valueOf(i);
      strings.add(elem);
    }
    return strings;
  }

}
