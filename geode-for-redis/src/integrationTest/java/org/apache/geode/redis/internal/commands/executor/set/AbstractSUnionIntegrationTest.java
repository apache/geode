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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_DIFFERENT_SLOTS;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSUnionIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String setKey1 = "{user1}setKey1";
  private static final String[] setMembers1 = {"one", "two", "three", "four", "five"};
  private static final String nonExistentSetKey = "{user1}nonExistentSet";
  private static final String setKey2 = "{user1}setKey2";

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void sunionErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SUNION, 1);
  }

  @Test
  public void sunion_returnsAllValuesInSet_setNotModified() {
    jedis.sadd(setKey1, setMembers1);
    assertThat(jedis.sunion(setKey1)).containsExactlyInAnyOrder(setMembers1);
    assertThat(jedis.smembers(setKey1)).containsExactlyInAnyOrder(setMembers1);
  }

  @Test
  public void sunionWithNonExistentSet_returnsEmptySet_nonExistentKeyDoesNotExist() {
    assertThat(jedis.sunion(nonExistentSetKey)).isEmpty();
    assertThat(jedis.exists(nonExistentSetKey)).isFalse();
  }

  @Test
  public void sunionWithNonExistentSetAndSet_returnsAllValuesInSet() {
    jedis.sadd(setKey1, setMembers1);
    assertThat(jedis.sunion(nonExistentSetKey, setKey1)).containsExactlyInAnyOrder(setMembers1);
  }

  @Test
  public void sunionWithSetAndNonExistentSet_returnsAllValuesInSet() {
    jedis.sadd(setKey1, setMembers1);
    assertThat(jedis.sunion(setKey1, nonExistentSetKey))
        .containsExactlyInAnyOrder(setMembers1);
  }

  @Test
  public void sunionWithMultipleNonExistentSets_returnsEmptySet() {
    assertThat(jedis.sunion("{user1}nonExistentSet1", "{user1}nonExistentSet2")).isEmpty();
  }

  @Test
  public void sunionWithNonOverlappingSets_returnsUnionOfSets() {
    String[] secondSetMembers = new String[] {"apple", "microsoft", "linux", "peach"};
    jedis.sadd(setKey1, setMembers1);
    jedis.sadd(setKey2, secondSetMembers);

    String[] result =
        {"one", "two", "three", "four", "five", "apple", "microsoft", "linux", "peach"};
    assertThat(jedis.sunion(setKey1, setKey2)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sunionWithSetsWithSomeSharedValues_returnsUnionOfSets() {
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(setKey1, setMembers1);
    jedis.sadd(setKey2, secondSetMembers);

    String[] result = {"one", "two", "three", "four", "five", "linux", "peach"};
    assertThat(jedis.sunion(setKey1, setKey2)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sunionWithSetsWithAllSharedValues_returnsUnionOfSets() {
    jedis.sadd(setKey1, setMembers1);
    jedis.sadd(setKey2, setMembers1);

    assertThat(jedis.sunion(setKey1, setKey2)).containsExactlyInAnyOrder(setMembers1);
  }

  @Test
  public void sunionWithMultipleSets_returnsUnionOfSets() {
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    String thirdSetKey = "{user1}setKey3";
    String[] thirdSetMembers = new String[] {"fire", "dance", "floor", "five"};
    jedis.sadd(setKey1, setMembers1);
    jedis.sadd(setKey2, secondSetMembers);
    jedis.sadd(thirdSetKey, thirdSetMembers);


    String[] result =
        {"one", "two", "three", "four", "five", "linux", "peach", "fire", "dance", "floor"};
    assertThat(jedis.sunion(setKey1, setKey2, thirdSetKey))
        .containsExactlyInAnyOrder(result);
  }

  @Test
  public void sunionWithSetsFromDifferentSlots_returnsCrossSlotError() {
    String setKeyDifferentSlot = "{user2}setKey2";
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(setKey1, setMembers1);
    jedis.sadd(setKeyDifferentSlot, secondSetMembers);

    assertThatThrownBy(() -> jedis.sunion(setKey1, setKeyDifferentSlot))
        .hasMessageContaining(ERROR_DIFFERENT_SLOTS);
  }


  @Test
  public void sunionWithDifferentKeyType_returnsWrongTypeError() {
    jedis.set("ding", "dong");
    assertThatThrownBy(() -> jedis.sunion("ding")).hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(setKey1, setMembers1);
    jedis.sadd(setKey2, secondSetMembers);

    String[] unionMembers = {"one", "two", "three", "four", "five", "linux", "peach"};
    Set<String> unionList = new HashSet<>(Arrays.asList(unionMembers));
    Set<String> firstSetMembersList = new HashSet<>(Arrays.asList(setMembers1));


    final AtomicReference<Set<String>> sunionResultReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem(setKey2, secondSetMembers),
        i -> sunionResultReference.set(jedis.sunion(setKey1, setKey2)))
            .runWithAction(() -> {
              assertThat(sunionResultReference).satisfiesAnyOf(
                  sunionResult -> assertThat(sunionResult.get())
                      .containsExactlyInAnyOrderElementsOf(firstSetMembersList),
                  sunionResult -> assertThat(sunionResult.get())
                      .containsExactlyInAnyOrderElementsOf(unionList));
              jedis.sadd(setKey2, unionMembers);
            });
  }

  @Test
  public void testSUnionStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("{user1}set1", firstSet);
    jedis.sadd("{user1}set2", secondSet);
    jedis.sadd("{user1}set3", thirdSet);

    Long resultSize = jedis.sunionstore("{user1}result", "{user1}set1", "{user1}set2");
    assertThat(resultSize).isEqualTo(7);

    Set<String> resultSet = jedis.smembers("{user1}result");
    String[] expected =
        new String[] {"pear", "apple", "plum", "orange", "peach", "microsoft", "linux"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long notEmptyResultSize =
        jedis.sunionstore("{user1}notempty", "{user1}nonexistent", "{user1}set1");
    Set<String> notEmptyResultSet = jedis.smembers("{user1}notempty");
    assertThat(notEmptyResultSize).isEqualTo(firstSet.length);
    assertThat(notEmptyResultSet).containsExactlyInAnyOrder(firstSet);

    jedis.sadd("{user1}newEmpty", "born2die");
    jedis.srem("{user1}newEmpty", "born2die");
    Long newNotEmptySize =
        jedis.sunionstore("{user1}newNotEmpty", "{user1}set2", "{user1}newEmpty");
    Set<String> newNotEmptySet = jedis.smembers("{user1}newNotEmpty");
    assertThat(newNotEmptySize).isEqualTo(secondSet.length);
    assertThat(newNotEmptySet).containsExactlyInAnyOrder(secondSet);
  }

  @Test
  public void testSUnionStore_withNonExistentKeys() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{user1}set1", firstSet);

    Long resultSize =
        jedis.sunionstore("{user1}set1", "{user1}nonExistent1", "{user1}nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("{user1}set1")).isFalse();
  }

  @Test
  public void testSUnionStore_withNonExistentKeys_andNonSetTarget() {
    jedis.set("{user1}string1", "stringValue");

    Long resultSize =
        jedis.sunionstore("{user1}string1", "{user1}nonExistent1", "{user1}nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("{user1}set1")).isFalse();
  }

  @Test
  public void testSUnionStore_withNonSetKey() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{user1}set1", firstSet);
    jedis.set("{user1}string1", "value1");

    assertThatThrownBy(() -> jedis.sunionstore("{user1}set1", "{user1}string1"))
        .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
    assertThat(jedis.exists("{user1}set1")).isTrue();
  }

  @Test
  public void testConcurrentSUnionStore() throws InterruptedException {
    int ENTRIES = 100;
    int SUBSET_SIZE = 100;

    Set<String> masterSet = new HashSet<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    List<Set<String>> otherSets = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      Set<String> oneSet = new HashSet<>();
      for (int j = 0; j < SUBSET_SIZE; j++) {
        oneSet.add("set-" + i + "-" + j);
      }
      otherSets.add(oneSet);
    }

    jedis.sadd("{user1}master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("{user1}set-" + i, otherSets.get(i).toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sunionstore("{user1}master", "{user1}master", "{user1}set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sunionstore("{user1}master", "{user1}master", "{user1}set-" + i);
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    otherSets.forEach(masterSet::addAll);

    assertThat(jedis.smembers("{user1}master").toArray())
        .containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void doesNotThrowExceptions_whenConcurrentSaddAndSunionExecute() {
    final int ENTRIES = 1000;
    Set<String> set1 = new HashSet<>();
    Set<String> set2 = new HashSet<>();
    for (int i = 0; i < ENTRIES; i++) {
      set1.add("value-1-" + i);
      set2.add("value-2-" + i);
    }

    jedis.sadd("{player1}key1", set1.toArray(new String[] {}));
    jedis.sadd("{player1}key2", set2.toArray(new String[] {}));

    new ConcurrentLoopingThreads(ENTRIES,
        i -> jedis.sunion("{player1}key1", "{player1}key2"),
        i -> jedis.sadd("{player1}key1", "newValue-1-" + i),
        i -> jedis.sadd("{player1}key2", "newValue-2-" + i))
            .runInLockstep();
  }

}
