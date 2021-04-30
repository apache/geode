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

package org.apache.geode.redis.internal.executor.string;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.SetParams;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class StringsDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int LIST_SIZE = 1000;
  private static final int NUM_ITERATIONS = 1000;
  private static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis1;
  private static JedisCluster jedis2;
  private static JedisCluster jedis3;

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static int redisServerPort1;
  private static int redisServerPort2;
  private static int redisServerPort3;

  @BeforeClass
  public static void classSetup() {
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    redisServerPort1 = clusterStartUp.getRedisPort(1);
    redisServerPort2 = clusterStartUp.getRedisPort(2);
    redisServerPort3 = clusterStartUp.getRedisPort(3);

    jedis1 = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort1), JEDIS_TIMEOUT);
    jedis2 = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort2), JEDIS_TIMEOUT);
    jedis3 = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort3), JEDIS_TIMEOUT);
  }


  @AfterClass
  public static void tearDown() {
    jedis1.close();
    jedis2.close();
    jedis3.close();

    server1.stop();
    server2.stop();
    server3.stop();
  }

  @Test
  public void get_shouldAllowClientToLocateDataForGivenKey() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.set(keys.get(i), values.get(i))).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedis2.get(keys.get(i))).isEqualTo(values.get(i))).run();
  }

  @Test
  public void setnx_shouldOnlySucceedOnceForAParticularKey_givenMultipleClientsSettingSameKey() {
    JedisCluster jedis1B =
        new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort1), JEDIS_TIMEOUT);
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");
    AtomicInteger successes1 = new AtomicInteger(0);
    AtomicInteger successes2 = new AtomicInteger(0);

    new ConcurrentLoopingThreads(LIST_SIZE,
        makeSetNXConsumer(keys, values, successes1, jedis1),
        makeSetNXConsumer(keys, values, successes2, jedis1B)).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedis3.get(keys.get(i))).isEqualTo(values.get(i))).run();

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
      jedis1.set(keys.get(i), values.get(i));
    }

    new ConcurrentLoopingThreads(LIST_SIZE,
        makeSetXXConsumer(keys, values, successes1, jedis1),
        makeSetXXConsumer(keys, values, successes2, jedis2)).run();

    assertThat(successes2.get() + successes1.get()).isEqualTo(LIST_SIZE * 2);
  }

  @Test
  public void set_shouldAllowMultipleClientsToSetValuesOnDifferentKeysConcurrently() {
    List<String> keys1 = makeStringList(LIST_SIZE, "key1-");
    List<String> values1 = makeStringList(LIST_SIZE, "values1-");
    List<String> keys2 = makeStringList(LIST_SIZE, "key2-");
    List<String> values2 = makeStringList(LIST_SIZE, "values2-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.set(keys1.get(i), values1.get(i)),
        (i) -> jedis2.set(keys2.get(i), values2.get(i))).run();

    for (int i = 0; i < LIST_SIZE; i++) {
      assertThat(jedis3.get(keys1.get(i))).isEqualTo(values1.get(i));
      assertThat(jedis3.get(keys2.get(i))).isEqualTo(values2.get(i));
    }
  }

  @Test
  public void set_shouldAllowMultipleClientsToSetValuesOnTheSameKeysConcurrently() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.set(keys.get(i), values.get(i)),
        (i) -> jedis2.set(keys.get(i), values.get(i))).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedis3.get(keys.get(i))).isEqualTo(values.get(i))).run();
  }

  @Test
  public void set_shouldAllowTwoSetsOfClientsPerServerOperatingOnTheSameStringConcurrently() {
    JedisCluster jedis1B =
        new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort1), JEDIS_TIMEOUT);
    JedisCluster jedis2B =
        new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort2), JEDIS_TIMEOUT);

    List<String> keys = makeStringList(LIST_SIZE, "keys-");
    List<String> values = makeStringList(LIST_SIZE, "values-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.set(keys.get(i), values.get(i)),
        (i) -> jedis1B.set(keys.get(i), values.get(i)),
        (i) -> jedis2.set(keys.get(i), values.get(i)),
        (i) -> jedis2B.set(keys.get(i), values.get(i))).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedis3.get(keys.get(i))).isEqualTo(values.get(i))).run();

    jedis1B.close();
    jedis2B.close();
  }

  @Test
  public void append_shouldAllowMultipleClientsToAppendDifferentValueToSameKeyConcurrently() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values1 = makeStringList(LIST_SIZE, "values1-");
    List<String> values2 = makeStringList(LIST_SIZE, "values2-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.append(keys.get(i), values1.get(i)),
        (i) -> jedis2.append(keys.get(i), values2.get(i))).runInLockstep();

    for (int i = 0; i < LIST_SIZE; i++) {
      assertThat(jedis3.get(keys.get(i))).contains(values1.get(i));
      assertThat(jedis3.get(keys.get(i))).contains(values2.get(i));
    }
  }

  @Test
  public void decr_shouldDecrementWhileDoingConcurrentDecr() {
    String key = "key";
    int initialValue = NUM_ITERATIONS * 2;
    jedis1.set(key, String.valueOf(initialValue));

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        (i) -> jedis1.decr(key),
        (i) -> jedis2.decr(key))
            .run();

    assertThat(jedis1.get(key)).isEqualTo("0");
  }

  @Test
  public void decrby_shouldDecrementReliably_givenConcurrentThreadsPerformingDecrby() {
    String key = "key";
    int initialValue = NUM_ITERATIONS * 6;
    jedis1.set(key, String.valueOf(initialValue));

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        (i) -> jedis1.decrBy(key, 4),
        (i) -> jedis2.decrBy(key, 2)).runInLockstep();

    assertThat(jedis1.get(key)).isEqualTo("0");
  }

  @Test
  public void strLen_returnsStringLengthWhileConcurrentlyUpdatingValues() {
    for (int i = 0; i < LIST_SIZE; i++) {
      jedis1.set("key-" + i, "value-" + i);
    }

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.set("key-" + i, "changedValue-" + i),
        (i) -> {
          long stringLength = jedis2.strlen("key-" + i);
          assertThat(stringLength == ("changedValue-" + i).length()
              || stringLength == ("value-" + i).length()).isTrue();
        }).runInLockstep();

    for (int i = 0; i < LIST_SIZE; i++) {
      String key = "key-" + i;
      String value = jedis1.get(key);
      String expectedValue = "changedValue-" + i;

      assertThat(value.length()).isEqualTo(expectedValue.length());
      assertThat(value).isEqualTo(expectedValue);
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
