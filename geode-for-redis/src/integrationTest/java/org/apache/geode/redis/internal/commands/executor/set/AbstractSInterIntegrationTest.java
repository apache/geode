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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
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
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractSInterIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final String SET1 = "{tag1}set1";
  private static final String SET2 = "{tag1}set2";
  private static final String SET3 = "{tag1}set3";

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void sinterErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SINTER, 1);
  }

  @Test
  public void sinterstoreErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SINTERSTORE, 2);
  }

  @Test
  public void testSInter_givenIntersection_returnsIntersectedMembers() {
    String[] firstSet = new String[] {"peach"};
    String[] secondSet = new String[] {"linux", "apple", "peach"};
    String[] thirdSet = new String[] {"luigi", "apple", "bowser", "peach", "mario"};
    jedis.sadd(SET1, firstSet);
    jedis.sadd(SET2, secondSet);
    jedis.sadd(SET3, thirdSet);

    Set<String> resultSet = jedis.sinter(SET1, SET2, SET3);

    String[] expected = new String[] {"peach"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);
  }

  @Test
  public void testSInter_givenNonSet_returnsErrorWrongType() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String nonSet = "apple";
    jedis.sadd(SET1, firstSet);
    jedis.set("{tag1}nonSet", nonSet);

    assertThatThrownBy(() -> jedis.sinter(SET1, "{tag1}nonSet")).hasMessageContaining(
        ERROR_WRONG_TYPE);
  }

  @Test
  public void testSInter_givenNoIntersection_returnsEmptySet() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"ubuntu", "microsoft", "linux", "solaris"};
    jedis.sadd(SET1, firstSet);
    jedis.sadd(SET2, secondSet);

    Set<String> emptyResultSet = jedis.sinter(SET1, SET2);
    assertThat(emptyResultSet).isEmpty();
  }

  @Test
  public void testSInter_givenSingleSet_returnsAllMembers() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd(SET1, firstSet);

    Set<String> resultSet = jedis.sinter(SET1);
    assertThat(resultSet).containsExactlyInAnyOrder(firstSet);
  }

  @Test
  public void testSInter_givenFullyMatchingSet_returnsAllMembers() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "pear", "plum", "peach", "orange",};
    jedis.sadd(SET1, firstSet);
    jedis.sadd(SET2, secondSet);

    Set<String> resultSet = jedis.sinter(SET1, SET2);
    assertThat(resultSet).containsExactlyInAnyOrder(firstSet);
  }

  @Test
  public void testSInter_givenNonExistentSingleSet_returnsEmptySet() {
    Set<String> emptyResultSet = jedis.sinter(SET1);
    assertThat(emptyResultSet).isEmpty();
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String[] values = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] newValues = new String[] {"ubuntu", "orange", "peach", "linux"};
    jedis.sadd(SET1, values);
    jedis.sadd(SET2, values);

    final AtomicReference<Set<String>> sinterResultReference = new AtomicReference<>();
    String[] result = new String[] {"orange", "peach"};
    new ConcurrentLoopingThreads(1000,
        i -> jedis.sadd(SET3, newValues),
        i -> sinterResultReference.set(
            jedis.sinter(SET1, SET2, SET3)))
                .runWithAction(() -> {
                  assertThat(sinterResultReference).satisfiesAnyOf(
                      sInterResult -> assertThat(sInterResult.get()).isEmpty(),
                      sInterResult -> assertThat(sInterResult.get())
                          .containsExactlyInAnyOrder(result));
                  jedis.srem(SET3, newValues);
                });
  }

  @Test
  public void testSInterStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("{tag1}set1", firstSet);
    jedis.sadd("{tag1}set2", secondSet);
    jedis.sadd("{tag1}set3", thirdSet);

    Long resultSize =
        jedis.sinterstore("{tag1}result", "{tag1}set1", "{tag1}set2", "{tag1}set3");
    Set<String> resultSet = jedis.smembers("{tag1}result");

    String[] expected = new String[] {"peach"};
    assertThat(resultSize).isEqualTo(expected.length);
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long otherResultSize = jedis.sinterstore("{tag1}set1", "{tag1}set1", "{tag1}set2");
    Set<String> otherResultSet = jedis.smembers("{tag1}set1");
    String[] otherExpected = new String[] {"apple", "peach"};
    assertThat(otherResultSize).isEqualTo(otherExpected.length);
    assertThat(otherResultSet).containsExactlyInAnyOrder(otherExpected);

    Long emptySetSize =
        jedis.sinterstore("{tag1}newEmpty", "{tag1}nonexistent", "{tag1}set2", "{tag1}set3");
    Set<String> emptyResultSet = jedis.smembers("{tag1}newEmpty");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    emptySetSize =
        jedis.sinterstore("{tag1}set1", "{tag1}nonexistent", "{tag1}set2", "{tag1}set3");
    emptyResultSet = jedis.smembers("{tag1}set1");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    Long copySetSize = jedis.sinterstore("{tag1}copySet", "{tag1}set2", "{tag1}newEmpty");
    Set<String> copyResultSet = jedis.smembers("{tag1}copySet");
    assertThat(copySetSize).isEqualTo(0);
    assertThat(copyResultSet).isEmpty();
  }

  @Test
  public void testSInterStore_withNonExistentKeys() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{tag1}set1", firstSet);

    Long resultSize =
        jedis.sinterstore("{tag1}set1", "{tag1}nonExistent1", "{tag1}nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("{tag1}set1")).isFalse();
  }

  @Test
  public void testSInterStore_withNonExistentKeys_andNonSetTarget() {
    jedis.set("string1", "stringValue");

    Long resultSize =
        jedis.sinterstore("{tag1}string1", "{tag1}nonExistent1", "{tag1}nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("{tag1}set1")).isFalse();
  }

  @Test
  public void testSInterStore_withNonSetKey() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{tag1}set1", firstSet);
    jedis.set("{tag1}string1", "value1");

    assertThatThrownBy(() -> jedis.sinterstore("{tag1}set1", "{tag1}string1"))
        .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
    assertThat(jedis.exists("{tag1}set1")).isTrue();
  }

  @Test
  public void testConcurrentSInterStore() throws InterruptedException {
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

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("set-" + i, otherSets.get(i).toArray(new String[] {}));
      jedis.sadd("set-" + i, masterSet.toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sinterstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sinterstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }
}
