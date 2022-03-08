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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class LRemDUnitTest {
  private static final int MAX_MATCHING_ELEMENTS = 141; // Summation of 145 is 10011
  private static final String ELEMENT_TO_CHECK = "insertedValue";

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static JedisCluster jedis;

  @Before
  public void testSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());
    clusterStartUp.startRedisVM(3, locator.getPort());
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
    String key = makeListKeyWithHashtag(1, clusterStartUp.getKeyOnServer("lrem", 1));
    List<String> elementList = makeListWithRepeatingElements(key);
    elementList.add(ELEMENT_TO_CHECK);

    lpushPerformAndVerify(key, elementList);

    Random rand = new Random();

    // Check for an element that doesn't exist in the list
    Collections.reverse(elementList);
    assertThat(jedis.lrem(key, getCount(rand, 1), "nonExistentValue")).isEqualTo(0);
    assertThat(jedis.lrange(key, 0, elementList.size())).isEqualTo(elementList);

    // Remove all elements except for ELEMENT_TO_CHECK
    for (int i = MAX_MATCHING_ELEMENTS - 1; i >= 0; i--) {
      assertThat(jedis.lrem(key, getCount(rand, i), makeElementString(key, i))).isEqualTo(i);
    }

    clusterStartUp.crashVM(1); // kill primary server

    assertThat(jedis.lrem(key, getCount(rand, 1), ELEMENT_TO_CHECK)).isEqualTo(1);
    assertThat(jedis.exists(key)).isFalse();
  }

  // TODO: Need to remove ignore and modify once GEODE-10108 is done
  @Ignore
  @Test
  public void givenBucketsMoveDuringLrem_thenOperationsAreNotLost() throws Exception {
    AtomicLong runningCount = new AtomicLong(3);
    List<String> listHashtags = makeListHashtags();
    List<String> keys = makeListKeys(listHashtags);

    List<String> elementList1 = makeListWithRepeatingElements(keys.get(0));
    List<String> elementList2 = makeListWithRepeatingElements(keys.get(1));
    List<String> elementList3 = makeListWithRepeatingElements(keys.get(2));

    lpushPerformAndVerify(keys.get(0), elementList1);
    lpushPerformAndVerify(keys.get(1), elementList2);
    lpushPerformAndVerify(keys.get(2), elementList3);

    int initialListSize = elementList1.size();
    Runnable task1 =
        () -> lremPerformAndVerify(keys.get(0), runningCount, initialListSize);
    Runnable task2 =
        () -> lremPerformAndVerify(keys.get(1), runningCount, initialListSize);
    Runnable task3 =
        () -> lremPerformAndVerify(keys.get(2), runningCount, initialListSize);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);

    for (int i = 0; i < 50 && runningCount.get() > 0; i++) {
      clusterStartUp.moveBucketForKey(listHashtags.get(i % listHashtags.size()));
      Thread.sleep(500);
    }

    runningCount.set(0);

    future1.get();
    future2.get();
    future3.get();
  }

  private void lremPerformAndVerify(String key, AtomicLong runningCount, int listSize) {
    assertThat(jedis.llen(key)).isEqualTo(listSize);

    Random rand = new Random();
    long elementCount = MAX_MATCHING_ELEMENTS - 1;
    while (jedis.llen(key) > 0 && runningCount.get() > 0) {
      String element = makeElementString(key, (int) elementCount);
      assertThat(jedis.lrem(key, getCount(rand, elementCount), element)).isEqualTo(elementCount);
      elementCount--;
    }

    if (runningCount.get() > 0) {
      assertThat(jedis.llen(key)).isZero();
    }

    runningCount.decrementAndGet(); // this thread done
  }

  /**
   * Generates a count that is either 0, negative, or positive.
   * A negative or positive count is matches the number of elements in the list.
   */
  private long getCount(Random rand, long numberOfElementsInList) {
    long countDirection = rand.nextInt(3) - 1;
    return countDirection * numberOfElementsInList;
  }

  /**
   * Creates a list that contains MAX_MATCHING_ELEMENTS (150) elements.
   * Almost all the elements in the list are added more than once.
   */
  private List<String> makeListWithRepeatingElements(String key) {
    List<String> elementList = new ArrayList<>();
    for (int i = 0; i < MAX_MATCHING_ELEMENTS; i++) {
      String element = makeElementString(key, i);

      // Adds i number of the element to the list
      for (int j = 0; j < i; j++) {
        elementList.add(element);
      }
    }

    return elementList;
  }

  private void lpushPerformAndVerify(String key, List<String> elementList) {
    jedis.lpush(key, elementList.toArray(new String[] {}));

    Long listLength = jedis.llen(key);
    assertThat(listLength).as("Initial list lengths not equal for key %s'", key)
        .isEqualTo(elementList.size());
  }

  private List<String> makeListHashtags() {
    List<String> listHashtags = new ArrayList<>();
    listHashtags.add(clusterStartUp.getKeyOnServer("lrem", 1));
    listHashtags.add(clusterStartUp.getKeyOnServer("lrem", 2));
    listHashtags.add(clusterStartUp.getKeyOnServer("lrem", 3));
    return listHashtags;
  }

  private List<String> makeListKeys(List<String> listHashtags) {
    List<String> keys = new ArrayList<>();
    keys.add(makeListKeyWithHashtag(1, listHashtags.get(0)));
    keys.add(makeListKeyWithHashtag(2, listHashtags.get(1)));
    keys.add(makeListKeyWithHashtag(3, listHashtags.get(2)));
    return keys;
  }

  private String makeListKeyWithHashtag(int index, String hashtag) {
    return "{" + hashtag + "}-key-" + index;
  }

  private String makeElementString(String key, int iterationCount) {
    return "-" + key + "-" + iterationCount + "-";
  }
}
