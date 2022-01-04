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

package org.apache.geode.redis.internal.commands.executor.list;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class LPopDUnitTest {
  public static final int MINIMUM_ITERATIONS = 10000;
  private static MemberVM locator;

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static JedisCluster jedis;

  @BeforeClass
  public static void classSetup() {
    locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(2, locator.getPort());
    clusterStartUp.startRedisVM(3, locator.getPort());
  }

  @Before
  public void testSetup() {
    clusterStartUp.startRedisVM(1, locator.getPort());
    int redisServerPort = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), REDIS_CLIENT_TIMEOUT);
    clusterStartUp.flushAll();
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void shouldDistributeDataAmongCluster_andRetainDataAfterServerCrash() {
    String key = makeListKeyWithHashtag(1, clusterStartUp.getKeyOnServer("lpop", 1));
    List<String> elementList = makeElementList(key, MINIMUM_ITERATIONS);
    lpushPerformAndVerify(key, elementList);

    // Remove all but last element
    for (int i = MINIMUM_ITERATIONS - 1; i > 0; i--) {
      assertThat(jedis.lpop(key)).isEqualTo(makeElementString(key, i));
    }
    clusterStartUp.crashVM(1); // kill primary server

    assertThat(jedis.lpop(key)).isEqualTo(makeElementString(key, 0));
    assertThat(jedis.exists(key)).isFalse();
  }

  @Test
  public void givenBucketsMoveDuringLpop_thenOperationsAreNotLost() throws Exception {
    AtomicLong runningCount = new AtomicLong(3);
    List<String> listHashtags = makeListHashtags();
    List<String> keys = makeListKeys(listHashtags);

    List<String> elementList1 = makeElementList(keys.get(0), MINIMUM_ITERATIONS);
    List<String> elementList2 = makeElementList(keys.get(1), MINIMUM_ITERATIONS);
    List<String> elementList3 = makeElementList(keys.get(2), MINIMUM_ITERATIONS);

    lpushPerformAndVerify(keys.get(0), elementList1);
    lpushPerformAndVerify(keys.get(1), elementList2);
    lpushPerformAndVerify(keys.get(2), elementList3);

    Runnable task1 =
        () -> lpopPerformAndVerify(keys.get(0), runningCount);
    Runnable task2 =
        () -> lpopPerformAndVerify(keys.get(1), runningCount);
    Runnable task3 =
        () -> lpopPerformAndVerify(keys.get(2), runningCount);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);

    for (int i = 0; i < 50 && runningCount.get() > 0; i++) {
      clusterStartUp.moveBucketForKey(listHashtags.get(i % listHashtags.size()));
      GeodeAwaitility.await().during(Duration.ofMillis(500)).until(() -> true);
    }

    runningCount.set(0);

    future1.get();
    future2.get();
    future3.get();
  }

  private List<String> makeListHashtags() {
    List<String> listHashtags = new ArrayList<>();
    listHashtags.add(clusterStartUp.getKeyOnServer("lpop", 1));
    listHashtags.add(clusterStartUp.getKeyOnServer("lpop", 2));
    listHashtags.add(clusterStartUp.getKeyOnServer("lpop", 3));
    return listHashtags;
  }

  private List<String> makeListKeys(List<String> listHashtags) {
    List<String> keys = new ArrayList<>();
    keys.add(makeListKeyWithHashtag(1, listHashtags.get(0)));
    keys.add(makeListKeyWithHashtag(2, listHashtags.get(1)));
    keys.add(makeListKeyWithHashtag(3, listHashtags.get(2)));
    return keys;
  }


  private void lpushPerformAndVerify(String key, List<String> elementList) {
    jedis.lpush(key, elementList.toArray(new String[] {}));

    Long listLength = jedis.llen(key);
    assertThat(listLength).isEqualTo(elementList.size())
        .as("Initial list lengths not equal for key %s'", key);
  }

  private void lpopPerformAndVerify(String key, AtomicLong runningCount) {
    assertThat(jedis.llen(key)).isGreaterThanOrEqualTo(MINIMUM_ITERATIONS);
    assertThat(jedis.llen(key)).isGreaterThanOrEqualTo(MINIMUM_ITERATIONS);

    int elementCount = MINIMUM_ITERATIONS - 1;
    while (jedis.llen(key) > 0 && runningCount.get() > 0) {
      String expected = key + "-" + (elementCount - 1) + "-";
      String element = expected;
      try {
        element = jedis.lpop(key);
        if (!element.equals(expected)) {
          int expectedCount = elementCount - 1;
          String[] chunks = element.split("-");
          elementCount = Integer.parseInt(chunks[4]);
          assertThat(expectedCount - elementCount).isLessThanOrEqualTo(1);
        }
      } catch (Exception ex) {
        runningCount.set(0); // test is over
        throw new RuntimeException("Exception performing LPOP for list '"
            + key + "' at element " + element + ": " + ex.getMessage());
      }
    }
    if (runningCount.get() > 0) {
      assertThat(jedis.llen(key)).isZero();
    }
    runningCount.decrementAndGet(); // this thread done
  }

  private String makeListKeyWithHashtag(int index, String hashtag) {
    return "{" + hashtag + "}-key-" + index;
  }

  private String makeElementString(String key, int iterationCount) {
    return "-" + key + "-" + iterationCount + "-";
  }

  private List<String> makeElementList(String key, int listSize) {
    List<String> elementList = new ArrayList<>();
    for (int i = 0; i < listSize; i++) {
      elementList.add(makeElementString(key, i));
    }
    return elementList;
  }
}
