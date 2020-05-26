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

package org.apache.geode.redis.executors.string;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class StringsDUnitTest {

  @ClassRule
  public static ClusterStartupRule clusterStartUp = new ClusterStartupRule(4);

  static final String LOCAL_HOST = "127.0.0.1";
  static final int LIST_SIZE = 1000;
  static int[] availablePorts;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  static Jedis jedis1;
  static Jedis jedis2;
  static Jedis jedis3;

  static Properties locatorProperties;
  static Properties serverProperties1;
  static Properties serverProperties2;
  static Properties serverProperties3;

  static MemberVM locator;
  static MemberVM server1;
  static MemberVM server2;
  static MemberVM server3;

  @BeforeClass
  public static void classSetup() {

    availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);

    locatorProperties = new Properties();
    serverProperties1 = new Properties();
    serverProperties2 = new Properties();
    serverProperties3 = new Properties();

    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    serverProperties1.setProperty(REDIS_PORT, Integer.toString(availablePorts[0]));
    serverProperties1.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);
    serverProperties1.setProperty(REDIS_ENABLED, "true");

    serverProperties2.setProperty(REDIS_PORT, Integer.toString(availablePorts[1]));
    serverProperties2.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);
    serverProperties2.setProperty(REDIS_ENABLED, "true");

    serverProperties3.setProperty(REDIS_PORT, Integer.toString(availablePorts[2]));
    serverProperties3.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);
    serverProperties3.setProperty(REDIS_ENABLED, "true");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startServerVM(1, serverProperties1, locator.getPort());
    server2 = clusterStartUp.startServerVM(2, serverProperties2, locator.getPort());
    server3 = clusterStartUp.startServerVM(3, serverProperties3, locator.getPort());

    jedis1 = new Jedis(LOCAL_HOST, availablePorts[0], JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, availablePorts[1], JEDIS_TIMEOUT);
    jedis3 = new Jedis(LOCAL_HOST, availablePorts[2], JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    jedis1.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
    jedis2.disconnect();
    jedis3.disconnect();

    server1.stop();
    server2.stop();
    server3.stop();
  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenMultipleClients() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.set(keys.get(i), values.get(i))).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedis2.get(keys.get(i))).isEqualTo(values.get(i))).run();
  }

  @Test
  public void setnxShould_onlySucceedOnceForAParticularKey_givenMultipleClientsSettingSameKey() {
    Jedis jedis1B = new Jedis(LOCAL_HOST, availablePorts[0]);
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

  private Consumer<Integer> makeSetNXConsumer(List<String> keys, List<String> values,
      AtomicInteger counter, Jedis jedis) {
    return (i) -> {
      SetParams setParams = new SetParams();
      setParams.nx();
      String result = jedis.set(keys.get(i), values.get(i), setParams);
      if (result != null) {
        counter.getAndIncrement();
      }
    };
  }

  @Ignore("Fix me please")
  @Test
  public void setxxShould_onlySucceedOnceForAParticularKey_givenMultipleClientsSettingSameKey() {
    Jedis jedis1B = new Jedis(LOCAL_HOST, availablePorts[0]);
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");

    AtomicLong successes1 = new AtomicLong(0);
    AtomicLong successes2 = new AtomicLong(0);

    for (int i = 0; i < LIST_SIZE; i++) {
      jedis1.set(keys.get(i), values.get(i));
    }

    new ConcurrentLoopingThreads(LIST_SIZE,
        makeSetXXConsumer(keys, values, successes1, jedis1),
        (i) -> successes2.addAndGet(jedis1B.del(keys.get(i))));

    assertThat(successes1.get())
        .as("Apparently 'SET XX' ConcurrentLoopingThread did not run")
        .isGreaterThan(0);
    assertThat(successes2.get())
        .as("Apparently 'DEL' ConcurrentLoopingThread did not run")
        .isGreaterThan(0);
    assertThat(successes1.get() + successes2.get()).isEqualTo(LIST_SIZE);
  }

  private Consumer<Integer> makeSetXXConsumer(List<String> keys, List<String> values,
      AtomicLong counter, Jedis jedis) {
    return (i) -> {
      SetParams setParams = new SetParams();
      setParams.xx();
      String result = jedis.set(keys.get(i), values.get(i), setParams);
      if (result != null) {
        counter.getAndIncrement();
      }
    };
  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenMultipleClients_AddingDifferentDataToSameStringConcurrently() {
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
  public void should_distributeDataAmongMultipleServers_givenMultipleClients_AddingSameDataToSameStringConcurrently() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values = makeStringList(LIST_SIZE, "values1-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.set(keys.get(i), values.get(i)),
        (i) -> jedis2.set(keys.get(i), values.get(i))).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedis3.get(keys.get(i))).isEqualTo(values.get(i))).run();
  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenTwoSetsOfClients_OperatingOnTheSameStringConcurrently() {
    Jedis jedis1B = new Jedis(LOCAL_HOST, availablePorts[0]);
    Jedis jedis2B = new Jedis(LOCAL_HOST, availablePorts[1]);

    List<String> keys = makeStringList(LIST_SIZE, "keys-");
    List<String> values = makeStringList(LIST_SIZE, "values-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.set(keys.get(i), values.get(i)),
        (i) -> jedis1B.set(keys.get(i), values.get(i)),
        (i) -> jedis2.set(keys.get(i), values.get(i)),
        (i) -> jedis2B.set(keys.get(i), values.get(i))).run();

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> assertThat(jedis3.get(keys.get(i))).isEqualTo(values.get(i))).run();

    jedis1B.disconnect();
    jedis2B.disconnect();
  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenMultipleClients_AppendingDifferentDataToSameStringConcurrently() {
    List<String> keys = makeStringList(LIST_SIZE, "key1-");
    List<String> values1 = makeStringList(LIST_SIZE, "values1-");
    List<String> values2 = makeStringList(LIST_SIZE, "values2-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis1.append(keys.get(i), values1.get(i)),
        (i) -> jedis2.append(keys.get(i), values2.get(i))).run();

    for (int i = 0; i < LIST_SIZE; i++) {
      assertThat(jedis3.get(keys.get(i))).contains(values1.get(i));
      assertThat(jedis3.get(keys.get(i))).contains(values2.get(i));
    }
  }

  private List<String> makeStringList(int setSize, String baseString) {
    List<String> strings = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      strings.add(baseString + i);
    }
    return strings;
  }
}
