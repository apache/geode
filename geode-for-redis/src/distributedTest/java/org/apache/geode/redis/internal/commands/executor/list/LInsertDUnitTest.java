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
import static redis.clients.jedis.args.ListPosition.AFTER;
import static redis.clients.jedis.args.ListPosition.BEFORE;

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
import redis.clients.jedis.args.ListPosition;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class LInsertDUnitTest {
  public static final int INITIAL_LIST_SIZE = 500;

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static JedisCluster jedis;
  private static final String insertedValue = "insertedValue";

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
    String key = makeListKeyWithHashtag(1, clusterStartUp.getKeyOnServer("linsert", 1));
    List<String> elementList = makeElementList(key, INITIAL_LIST_SIZE);
    lpushPerformAndVerify(key, elementList);

    assertThat(jedis.linsert(key, BEFORE, jedis.lindex(key, 2), insertedValue))
        .isEqualTo(elementList.size() + 1);

    assertThat(jedis.lindex(key, 2)).isEqualTo(insertedValue);

    clusterStartUp.crashVM(1); // kill primary

    assertThat(jedis.llen(key)).isEqualTo(elementList.size() + 1);
    assertThat(jedis.lindex(key, 2)).isEqualTo(insertedValue);
    assertThat(jedis.lindex(key, 3)).isEqualTo(elementList.get(INITIAL_LIST_SIZE - 3));
  }

  @Test
  public void givenBucketsMoveDuringLInsert_operationsAreNotLost() throws Exception {
    AtomicBoolean continueInserting = new AtomicBoolean(true);
    List<String> listHashtags = makeListHashtags();
    List<String> keys = makeListKeys(listHashtags);

    List<String> elementList1 = makeElementList(keys.get(0), INITIAL_LIST_SIZE);
    List<String> elementList2 = makeElementList(keys.get(1), INITIAL_LIST_SIZE);
    List<String> elementList3 = makeElementList(keys.get(2), INITIAL_LIST_SIZE);

    lpushPerformAndVerify(keys.get(0), elementList1);
    lpushPerformAndVerify(keys.get(1), elementList2);
    lpushPerformAndVerify(keys.get(2), elementList3);

    Runnable task1 =
        () -> linsertPerformAndVerify(keys.get(0), BEFORE, 2, insertedValue, continueInserting);
    Runnable task2 =
        () -> linsertPerformAndVerify(keys.get(1), AFTER, 2, insertedValue, continueInserting);
    Runnable task3 =
        () -> linsertPerformAndVerify(keys.get(2), AFTER, 2, insertedValue, continueInserting);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);

    for (int i = 0; i < 20; i++) {
      clusterStartUp.moveBucketForKey(listHashtags.get(i % listHashtags.size()));
      Thread.sleep(500);
    }
    continueInserting.set(false);

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

  private void linsertPerformAndVerify(String key, ListPosition pos, int pivotIndex,
      String valueBase, AtomicBoolean continueInserting) {
    int counter = 0;
    while (continueInserting.get()) {
      String insertedValue = valueBase + counter;
      long startLength = jedis.llen(key);
      String pivot = jedis.lindex(key, pivotIndex);
      assertThat(jedis.linsert(key, pos, pivot, insertedValue)).isEqualTo(startLength + 1);

      if (pos == BEFORE) {
        // Increment the pivot index as we just inserted a new value before it
        pivotIndex++;
        assertThat(jedis.lindex(key, pivotIndex - 1)).isEqualTo(insertedValue);
        assertThat(jedis.lindex(key, pivotIndex)).isEqualTo(pivot);
      } else {
        assertThat(jedis.lindex(key, pivotIndex + 1)).isEqualTo(insertedValue);
        assertThat(jedis.lindex(key, pivotIndex)).isEqualTo(pivot);
      }
    }
  }

  private void linsertPerformAndVerify2(String key, ListPosition pos, int pivotIndex,
      String valueBase,
      AtomicBoolean continueInserting) {
    int counter = 0;
    while (continueInserting.get()) {
      String value = valueBase + counter;
      long startLength = jedis.llen(key);
      String pivot = jedis.lindex(key, pivotIndex);
      assertThat(jedis.linsert(key, pos, pivot, value)).isEqualTo(startLength + 1);

      if (pos == BEFORE) {
        // Increment the pivot index as we just inserted a new value before it
        pivotIndex++;
        assertThat(jedis.lindex(key, pivotIndex - 1)).isEqualTo(insertedValue);
        assertThat(jedis.lindex(key, pivotIndex)).isEqualTo(pivot);
      } else {
        assertThat(jedis.lindex(key, pivotIndex + 1)).isEqualTo(insertedValue);
        assertThat(jedis.lindex(key, pivotIndex)).isEqualTo(pivot);
      }
    }
  }

  private void lpushPerformAndVerify(String key, List<String> elementList) {
    jedis.lpush(key, elementList.toArray(new String[] {}));

    Long listLength = jedis.llen(key);
    assertThat(listLength).as("Initial list lengths not equal for key %s'", key)
        .isEqualTo(elementList.size());
  }

  private String makeListKeyWithHashtag(int index, String hashtag) {
    return "{" + hashtag + "}-key-" + index;
  }

  private List<String> makeElementList(String key, int listSize) {
    List<String> elementList = new ArrayList<>();
    for (int i = 0; i < listSize; i++) {
      elementList.add("-" + key + "-" + i + "-");
    }
    return elementList;
  }
}
