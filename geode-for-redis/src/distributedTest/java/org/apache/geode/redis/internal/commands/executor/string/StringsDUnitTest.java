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

package org.apache.geode.redis.internal.commands.executor.string;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.SetParams;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class StringsDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final int LIST_SIZE = 1000;
  private static final int NUM_ITERATIONS = 1000;
  private static JedisCluster jedisCluster;

  private static MemberVM locator;

  @BeforeClass
  public static void classSetup() {
    locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());
    clusterStartUp.startRedisVM(3, locator.getPort());

    int redisServerPort1 = clusterStartUp.getRedisPort(1);
    jedisCluster =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void after() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedisCluster.close();
  }

  @Test
  public void get_shouldAllowClientToLocateDataForGivenKey() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedisCluster.set(keys.get(i), values.get(i))).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedisCluster.get(keys.get(i))).isEqualTo(values.get(i))).run();
  }

  @Test
  public void setnx_shouldOnlySucceedOnceForAParticularKey_givenMultipleClientsSettingSameKey() {

    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");
    AtomicInteger successes1 = new AtomicInteger(0);
    AtomicInteger successes2 = new AtomicInteger(0);

    new ConcurrentLoopingThreads(LIST_SIZE,
        makeSetNXConsumer(keys, values, successes1, jedisCluster),
        makeSetNXConsumer(keys, values, successes2, jedisCluster)).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedisCluster.get(keys.get(i))).isEqualTo(values.get(i))).run();

    assertThat(successes1.get())
        .as("Apparently ConcurrentLoopingThread did not run")
        .isGreaterThan(0);

    assertThat(successes2.get())
        .as("Apparently ConcurrentLoopingThread did not run")
        .isGreaterThan(0);

    assertThat(successes1.get() + successes2.get()).isEqualTo(LIST_SIZE);
  }

  @Test
  public void setxx_shouldAlwaysSucceed_givenMultipleClientsSettingSameKeyThatAlreadyExists() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");

    AtomicLong successes1 = new AtomicLong(0);
    AtomicLong successes2 = new AtomicLong(0);

    for (int i = 0; i < LIST_SIZE; i++) {
      jedisCluster.set(keys.get(i), values.get(i));
    }

    new ConcurrentLoopingThreads(LIST_SIZE,
        makeSetXXConsumer(keys, values, successes1, jedisCluster),
        makeSetXXConsumer(keys, values, successes2, jedisCluster)).run();

    assertThat(successes2.get() + successes1.get()).isEqualTo(LIST_SIZE * 2);
  }

  @Test
  public void set_shouldAllowMultipleClientsToSetValuesOnDifferentKeysConcurrently() {
    List<String> keys1 = makeStringList(LIST_SIZE, "key1-");
    List<String> values1 = makeStringList(LIST_SIZE, "values1-");
    List<String> keys2 = makeStringList(LIST_SIZE, "key2-");
    List<String> values2 = makeStringList(LIST_SIZE, "values2-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedisCluster.set(keys1.get(i), values1.get(i)),
        (i) -> jedisCluster.set(keys2.get(i), values2.get(i))).run();

    for (int i = 0; i < LIST_SIZE; i++) {
      assertThat(jedisCluster.get(keys1.get(i))).isEqualTo(values1.get(i));
      assertThat(jedisCluster.get(keys2.get(i))).isEqualTo(values2.get(i));
    }
  }

  @Test
  public void set_shouldAllowMultipleClientsToSetValuesOnTheSameKeysConcurrently() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedisCluster.set(keys.get(i), values.get(i)),
        (i) -> jedisCluster.set(keys.get(i), values.get(i))).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedisCluster.get(keys.get(i))).isEqualTo(values.get(i))).run();
  }


  @Test
  public void append_shouldAllowMultipleClientsToAppendDifferentValueToSameKeyConcurrently() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values1 = makeStringList(LIST_SIZE, "values1-");
    List<String> values2 = makeStringList(LIST_SIZE, "values2-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedisCluster.append(keys.get(i), values1.get(i)),
        (i) -> jedisCluster.append(keys.get(i), values2.get(i))).runInLockstep();

    for (int i = 0; i < LIST_SIZE; i++) {
      assertThat(jedisCluster.get(keys.get(i))).contains(values1.get(i));
      assertThat(jedisCluster.get(keys.get(i))).contains(values2.get(i));
    }
  }

  @Test
  public void append_shouldAllowMultipleClientsToSetrangeDifferentValueToSameKeyConcurrently() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values1 = makeStringList(LIST_SIZE, "values1-");
    List<String> values2 = makeStringList(LIST_SIZE, "values2-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedisCluster.setrange(keys.get(i), 0, values1.get(i)),
        (i) -> jedisCluster.setrange(keys.get(i), values1.get(i).length(),
            values2.get(i))).runInLockstep();

    for (int i = 0; i < LIST_SIZE; i++) {
      assertThat(jedisCluster.get(keys.get(i))).contains(values1.get(i));
      assertThat(jedisCluster.get(keys.get(i))).contains(values2.get(i));
    }
  }

  @Test
  public void decr_shouldDecrementWhileDoingConcurrentDecr() {
    String key = "key";
    int initialValue = NUM_ITERATIONS * 2;
    jedisCluster.set(key, String.valueOf(initialValue));

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        (i) -> jedisCluster.decr(key),
        (i) -> jedisCluster.decr(key))
            .run();

    assertThat(jedisCluster.get(key)).isEqualTo("0");
  }

  @Test
  public void decrby_shouldDecrementReliably_givenConcurrentThreadsPerformingDecrby() {
    String key = "key";
    int initialValue = NUM_ITERATIONS * 6;
    jedisCluster.set(key, String.valueOf(initialValue));

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        (i) -> jedisCluster.decrBy(key, 4),
        (i) -> jedisCluster.decrBy(key, 2)).runInLockstep();

    assertThat(jedisCluster.get(key)).isEqualTo("0");
  }

  @Test
  public void strLen_returnsStringLengthWhileConcurrentlyUpdatingValues() {
    for (int i = 0; i < LIST_SIZE; i++) {
      jedisCluster.set("key-" + i, "value-" + i);
    }

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedisCluster.set("key-" + i, "changedValue-" + i),
        (i) -> {
          long stringLength = jedisCluster.strlen("key-" + i);
          assertThat(
              stringLength == ("changedValue-" + i).length()
                  || stringLength == ("value-" + i).length()).isTrue();
        }).runInLockstep();

    for (int i = 0; i < LIST_SIZE; i++) {
      String key = "key-" + i;
      String value = jedisCluster.get(key);
      String expectedValue = "changedValue-" + i;

      assertThat(value.length()).isEqualTo(expectedValue.length());
      assertThat(value).isEqualTo(expectedValue);
    }
  }

  @Test
  public void givenBucketsMoveDuringAppend_thenDataIsNotLost() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);

    List<String> hashtags = new ArrayList<>();
    hashtags.add(clusterStartUp.getKeyOnServer("append", 1));
    hashtags.add(clusterStartUp.getKeyOnServer("append", 2));
    hashtags.add(clusterStartUp.getKeyOnServer("append", 3));

    Runnable task1 = () -> appendPerformAndVerify(1, 10000, hashtags.get(0), running);
    Runnable task2 = () -> appendPerformAndVerify(2, 10000, hashtags.get(1), running);
    Runnable task3 = () -> appendPerformAndVerify(3, 10000, hashtags.get(2), running);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);

    for (int i = 0; i < 100 && running.get(); i++) {
      clusterStartUp.moveBucketForKey(hashtags.get(i % hashtags.size()));
      GeodeAwaitility.await().during(Duration.ofMillis(200)).until(() -> true);
    }

    for (int i = 0; i < 100 && running.get(); i++) {
      clusterStartUp.moveBucketForKey(hashtags.get(i % hashtags.size()));
      GeodeAwaitility.await().during(Duration.ofMillis(200)).until(() -> true);
    }

    running.set(false);

    future1.get();
    future2.get();
    future3.get();
  }

  private void appendPerformAndVerify(int index, int minimumIterations, String hashtag,
      AtomicBoolean isRunning) {
    String key = "{" + hashtag + "}-key-" + index;
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String appendString = "-" + key + "-" + iterationCount + "-";
      try {
        jedisCluster.append(key, appendString);
      } catch (Exception ex) {
        isRunning.set(false);
        throw new RuntimeException("Exception performing APPEND " + appendString, ex);
      }
      iterationCount += 1;
    }

    String storedString = jedisCluster.get(key);

    int idx = 0;
    int i = 0;
    while (i < iterationCount) {
      String expectedValue = "-" + key + "-" + i + "-";
      String foundValue = storedString.substring(idx, idx + expectedValue.length());
      if (!foundValue.equals(expectedValue)) {
        Assert.fail("unexpected " + foundValue + " at index " + i + " iterationCount="
            + iterationCount + " in string "
            + storedString);
        break;
      }
      idx += expectedValue.length();
      i++;
    }
  }

  private List<String> makeStringList(int setSize, String baseString) {
    List<String> strings = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      strings.add(baseString + i);
    }
    return strings;
  }

  private Consumer<Integer> makeSetXXConsumer(List<String> keys, List<String> values,
      AtomicLong counter, JedisCluster jedis) {
    return (i) -> {
      SetParams setParams = new SetParams();
      setParams.xx();
      String result = jedis.set(keys.get(i), values.get(i), setParams);
      if (result != null) {
        counter.getAndIncrement();
      }
    };
  }

  private Consumer<Integer> makeSetNXConsumer(List<String> keys, List<String> values,
      AtomicInteger counter, JedisCluster jedis) {
    return (i) -> {
      SetParams setParams = new SetParams();
      setParams.nx();
      String result = jedis.set(keys.get(i), values.get(i), setParams);
      if (result != null) {
        counter.getAndIncrement();
      }
    };
  }
}
