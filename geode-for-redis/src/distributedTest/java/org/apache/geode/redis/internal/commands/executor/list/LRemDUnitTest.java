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
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class LRemDUnitTest {
  private static final int LIST_SIZE_FOR_BUCKET_TEST = 10000;
  private static final int UNIQUE_ELEMENTS = 5000;
  private static final int COUNT_OF_UNIQUE_ELEMENT = 2; // How many times a unique element is
                                                        // repeated in list

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

    // Create initial list and push it
    final int initialListSize = 30;
    final int uniqueElements = 3;
    List<String> elementList = new ArrayList<>();
    for (int i = 0; i < initialListSize; i++) {
      elementList.add(makeElementString(key, i % uniqueElements));
    }
    jedis.lpush(key, elementList.toArray(new String[] {}));

    // Remove all elements except for ELEMENT_TO_CHECK
    final int uniqueElementsCount = initialListSize / uniqueElements;
    assertThat(jedis.lrem(key, 0, makeElementString(key, 0))).isEqualTo(uniqueElementsCount);
    assertThat(jedis.lrem(key, -uniqueElementsCount, makeElementString(key, 1)))
        .isEqualTo(uniqueElementsCount);

    clusterStartUp.crashVM(1); // kill primary server

    assertThat(jedis.llen(key)).isEqualTo(uniqueElementsCount);
    assertThat(jedis.lrem(key, uniqueElementsCount, makeElementString(key, 2)))
        .isEqualTo(uniqueElementsCount);
    assertThat(jedis.exists(key)).isFalse();
  }


  @Test
  public void givenBucketsMoveDuringLrem_thenOperationsAreNotLost() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);

    List<String> listHashtags = makeListHashtags();
    String key1 = makeListKeyWithHashtag(1, listHashtags.get(0));
    String key2 = makeListKeyWithHashtag(2, listHashtags.get(1));
    String key3 = makeListKeyWithHashtag(3, listHashtags.get(2));

    List<String> elementList1 = makeListWithRepeatingElements(key1);
    List<String> elementList2 = makeListWithRepeatingElements(key2);
    List<String> elementList3 = makeListWithRepeatingElements(key3);

    jedis.lpush(key1, elementList1.toArray(new String[] {}));
    jedis.lpush(key2, elementList2.toArray(new String[] {}));
    jedis.lpush(key3, elementList3.toArray(new String[] {}));

    Future<Integer> future1 = executor.submit(() -> performLremAndVerify(key1, running, elementList1));
    Future<Integer> future2 = executor.submit(() -> performLremAndVerify(key2, running, elementList2));
    Future<Integer> future3 = executor.submit(() -> performLremAndVerify(key3, running, elementList3));

    for (int i = 0; i < 50; i++) {
      clusterStartUp.moveBucketForKey(listHashtags.get(i % listHashtags.size()));
      Thread.sleep(500);
    }

    running.set(false);

    verifyLremResult(key1, future1.get());
    verifyLremResult(key2, future2.get());
    verifyLremResult(key3, future3.get());
  }

  private void verifyLremResult(String key, int iterationCount) {
    for (int i = UNIQUE_ELEMENTS - 1; i >= iterationCount; i--) {
      String element = makeElementString(key, i);
      assertThat(jedis.lrem(key, COUNT_OF_UNIQUE_ELEMENT, element))
          .isEqualTo(COUNT_OF_UNIQUE_ELEMENT);
    }
    assertThat(jedis.exists(key)).isFalse();
  }

  private Integer performLremAndVerify(String key, AtomicBoolean isRunning, List<String> list) {
    assertThat(jedis.llen(key)).isEqualTo(LIST_SIZE_FOR_BUCKET_TEST);
    int count = COUNT_OF_UNIQUE_ELEMENT;

    int iterationCount = 0;
    while (isRunning.get()) {
      count = -count;
      String element = makeElementString(key, iterationCount);
      assertThat(jedis.lrem(key, count, element)).isEqualTo(COUNT_OF_UNIQUE_ELEMENT);
      iterationCount++;

      if (iterationCount == UNIQUE_ELEMENTS) {
        iterationCount = 0;
        jedis.lpush(key, list.toArray(new String[] {}));
      }
    }

    return iterationCount;
  }

  private List<String> makeListWithRepeatingElements(String key) {
    List<String> elementList = new ArrayList<>();
    for (int i = 0; i < LIST_SIZE_FOR_BUCKET_TEST; i++) {
      elementList.add(makeElementString(key, i % UNIQUE_ELEMENTS));
    }
    return elementList;
  }

  private List<String> makeListHashtags() {
    List<String> listHashtags = new ArrayList<>();
    listHashtags.add(clusterStartUp.getKeyOnServer("lrem", 1));
    listHashtags.add(clusterStartUp.getKeyOnServer("lrem", 2));
    listHashtags.add(clusterStartUp.getKeyOnServer("lrem", 3));
    return listHashtags;
  }


  private String makeListKeyWithHashtag(int index, String hashtag) {
    return "{" + hashtag + "}-key-" + index;
  }

  private String makeElementString(String key, int iterationCount) {
    return "-" + key + "-" + iterationCount + "-";
  }
}
