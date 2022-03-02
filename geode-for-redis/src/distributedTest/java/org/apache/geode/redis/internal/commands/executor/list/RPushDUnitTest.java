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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RPushDUnitTest {
  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  public static final int PUSHER_COUNT = 6;
  public static final int PUSH_LIST_SIZE = 3;
  private static final int MINIMUM_ITERATIONS = 10000;

  private static final AtomicLong runningCount = new AtomicLong(PUSHER_COUNT);
  private static List<String> listHashtags;
  private static List<String> keys;
  private static HashMap<String, List<String>> keyToElementListMap;
  private static List<Runnable> taskList;

  private JedisCluster jedis;

  @Before
  public void testSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());
    clusterStartUp.startRedisVM(3, locator.getPort());
    int redisServerPort = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), 10_000);
    listHashtags = makeListHashtags();
    keys = makeListKeys(listHashtags);
    keyToElementListMap = new HashMap<>();
    for (String key : keys) {
      keyToElementListMap.put(key, makeElementList(PUSH_LIST_SIZE, key));
    }

    taskList = new ArrayList<>();
    taskList.add(() -> rpushPerformAndVerify(keys.get(0), keyToElementListMap.get(keys.get(0)),
        runningCount));
    taskList.add(() -> rpushPerformAndVerify(keys.get(0), keyToElementListMap.get(keys.get(0)),
        runningCount));
    taskList.add(() -> rpushPerformAndVerify(keys.get(1), keyToElementListMap.get(keys.get(1)),
        runningCount));
    taskList.add(() -> rpushPerformAndVerify(keys.get(1), keyToElementListMap.get(keys.get(1)),
        runningCount));
    taskList.add(() -> rpushPerformAndVerify(keys.get(2), keyToElementListMap.get(keys.get(2)),
        runningCount));
    taskList.add(() -> rpushPerformAndVerify(keys.get(2), keyToElementListMap.get(keys.get(2)),
        runningCount));
    taskList.add(() -> verifyListLengthCondition(keys.get(0), runningCount));
    taskList.add(() -> verifyListLengthCondition(keys.get(1), runningCount));
    taskList.add(() -> verifyListLengthCondition(keys.get(2), runningCount));
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void givenBucketsMovedDuringRPush_elementsAreAddedAtomically()
      throws ExecutionException, InterruptedException {

    List<Future<Void>> futureList = new ArrayList<>();
    for (Runnable task : taskList) {
      futureList.add(executor.runAsync(task));
    }

    for (int i = 0; i < 10 && runningCount.get() > 0; i++) {
      clusterStartUp.moveBucketForKey(listHashtags.get(i % listHashtags.size()));
      Thread.sleep(500);
    }

    for (Future<Void> future : futureList) {
      future.get();
    }

    for (String key : keys) {
      long length = jedis.llen(key);
      assertThat(length).isGreaterThanOrEqualTo(MINIMUM_ITERATIONS * 2 * PUSH_LIST_SIZE);
      validateListContents(key, keyToElementListMap);
    }
  }

  @Test
  public void shouldNotLoseData_givenPrimaryServerCrashesDuringOperations()
      throws ExecutionException, InterruptedException {
    List<Future<Void>> futureList = new ArrayList<>();
    for (Runnable task : taskList) {
      futureList.add(executor.runAsync(task));
    }

    Thread.sleep(200);
    clusterStartUp.crashVM(1); // kill primary server

    for (Future<Void> future : futureList) {
      future.get();
    }

    long length;
    for (String key : keys) {
      length = jedis.llen(key);
      assertThat(length).isGreaterThanOrEqualTo(MINIMUM_ITERATIONS * 2 * PUSH_LIST_SIZE);
      assertThat(length % 3).isEqualTo(0);
      validateListContents(key, keyToElementListMap);
    }
  }

  private void rpushPerformAndVerify(String key, List<String> elementList,
      AtomicLong runningCount) {
    for (int i = 0; i < MINIMUM_ITERATIONS; i++) {
      long listLength = jedis.llen(key);
      long newLength = jedis.rpush(key, elementList.toArray(new String[] {}));
      assertThat((newLength - listLength) % 3).as("RPUSH, list length %s not multiple of 3",
          newLength).isEqualTo(0);
    }
    runningCount.decrementAndGet();
  }

  private void validateListContents(String key,
      HashMap<String, List<String>> keyToElementListMap) {
    while (jedis.llen(key) > 0) {
      List<String> elementList = keyToElementListMap.get(key);
      assertThat(jedis.lpop(key)).isEqualTo(elementList.get(0));
      assertThat(jedis.lpop(key)).isEqualTo(elementList.get(1));
      assertThat(jedis.lpop(key)).isEqualTo(elementList.get(2));
    }
  }

  private List<String> makeListHashtags() {
    List<String> listHashtags = new ArrayList<>();
    listHashtags.add(clusterStartUp.getKeyOnServer("rpush", 1));
    listHashtags.add(clusterStartUp.getKeyOnServer("rpush", 2));
    listHashtags.add(clusterStartUp.getKeyOnServer("rpush", 3));
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

  private void verifyListLengthCondition(String key, AtomicLong runningCount) {
    while (runningCount.get() > 0) {
      long listLength = jedis.llen(key);
      assertThat(listLength % 3).as("List length not a multiple of three: %s", listLength)
          .isEqualTo(0);
    }
  }

  private List<String> makeElementList(int listSize, String baseString) {
    List<String> elements = new ArrayList<>();
    for (int i = 0; i < listSize; i++) {
      elements.add(baseString + i);
    }
    return elements;
  }
}
