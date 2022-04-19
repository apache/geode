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

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RPopDUnitTest {
  public static final int INITIAL_LIST_SIZE = 10000;

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static JedisCluster jedis;

  @Before
  public void testSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    int redisServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    clusterStartUp.startRedisVM(1, Integer.toString(redisServerPort), locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());
    clusterStartUp.startRedisVM(3, locator.getPort());

    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), 10_000);
    clusterStartUp.flushAll();
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void shouldPropagateDeltasAmongCluster_andRetainDataAfterServerCrash() {
    String key = makeListKeyWithHashtag(1, clusterStartUp.getKeyOnServer("rpop", 1));
    List<String> elementList = makeElementList(key, INITIAL_LIST_SIZE);
    lpushPerformAndVerify(key, elementList);

    // Remove all but first element
    for (int i = 0; i < INITIAL_LIST_SIZE - 1; i++) {
      assertThat(jedis.rpop(key)).isEqualTo(makeElementString(key, i));
    }

    clusterStartUp.crashVM(1); // kill primary server

    assertThat(jedis.llen(key)).isEqualTo(1);
    assertThat(jedis.rpop(key)).isEqualTo(elementList.get(elementList.size() - 1));
    assertThat(jedis.exists(key)).isFalse();
  }

  @Test
  public void givenBucketsMoveDuringRpop_thenOperationsAreNotLost() throws Exception {
    AtomicBoolean continueRunningRpop = new AtomicBoolean(true);
    List<String> listHashtags = makeListHashtags();
    List<String> keys = makeListKeys(listHashtags);

    List<String> elementList1 = makeElementList(keys.get(0), INITIAL_LIST_SIZE);
    List<String> elementList2 = makeElementList(keys.get(1), INITIAL_LIST_SIZE);
    List<String> elementList3 = makeElementList(keys.get(2), INITIAL_LIST_SIZE);

    lpushPerformAndVerify(keys.get(0), elementList1);
    lpushPerformAndVerify(keys.get(1), elementList2);
    lpushPerformAndVerify(keys.get(2), elementList3);

    Runnable task1 =
        () -> rpopPerformAndVerify(keys.get(0), continueRunningRpop, elementList1);
    Runnable task2 =
        () -> rpopPerformAndVerify(keys.get(1), continueRunningRpop, elementList2);
    Runnable task3 =
        () -> rpopPerformAndVerify(keys.get(2), continueRunningRpop, elementList3);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);

    int BUCKET_MOVES = 20;
    for (int i = 0; i < BUCKET_MOVES; i++) {
      clusterStartUp.moveBucketForKey(listHashtags.get(i % listHashtags.size()));
      Thread.sleep(500);
    }

    continueRunningRpop.set(false);

    future1.get();
    future2.get();
    future3.get();
  }

  private List<String> makeListHashtags() {
    List<String> listHashtags = new ArrayList<>();
    listHashtags.add(clusterStartUp.getKeyOnServer("rpop", 1));
    listHashtags.add(clusterStartUp.getKeyOnServer("rpop", 2));
    listHashtags.add(clusterStartUp.getKeyOnServer("rpop", 3));
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
    assertThat(listLength).as("Initial list lengths not equal for key %s'", key)
        .isEqualTo(elementList.size());
  }

  private void rpopPerformAndVerify(String key, AtomicBoolean continueRunning,
      List<String> elementList) {
    assertThat(jedis.llen(key)).isEqualTo(INITIAL_LIST_SIZE);

    while (continueRunning.get()) {
      int currentSize = INITIAL_LIST_SIZE;
      for (String element : elementList) {
        try {
          assertThat(jedis.rpop(key)).isEqualTo(element);
          currentSize--;
          assertThat(jedis.llen(key)).isEqualTo(currentSize);
        } catch (Exception ex) {
          continueRunning.set(false);
          throw new RuntimeException("Exception performing RPOP for list '"
              + key + "' at element " + element + ": " + ex.getMessage());
        }
      }
      assertThat(jedis.exists(key)).isFalse();
      jedis.lpush(key, elementList.toArray(new String[0]));
    }
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
