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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractSUnionIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

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
  public void sunionErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SUNION, 1);
  }

  @Test
  public void sunionstroreErrors_givenTooFewArgumentst() {
    assertAtLeastNArgs(jedis, Protocol.Command.SUNIONSTORE, 2);
  }

  @Test
  public void testSUnion() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("{tag1}set1", firstSet);
    jedis.sadd("{tag1}set2", secondSet);
    jedis.sadd("{tag1}set3", thirdSet);

    Set<String> resultSet = jedis.sunion("{tag1}set1", "{tag1}set2");
    String[] expected =
        new String[] {"pear", "apple", "plum", "orange", "peach", "microsoft", "linux"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Set<String> otherResultSet = jedis.sunion("{tag1}nonexistent", "{tag1}set1");
    assertThat(otherResultSet).containsExactlyInAnyOrder(firstSet);

    jedis.sadd("{tag1}newEmpty", "born2die");
    jedis.srem("{tag1}newEmpty", "born2die");
    Set<String> yetAnotherResultSet = jedis.sunion("{tag1}set2", "{tag1}newEmpty");
    assertThat(yetAnotherResultSet).containsExactlyInAnyOrder(secondSet);
  }

  @Test
  public void testSUnionStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("{tag1}set1", firstSet);
    jedis.sadd("{tag1}set2", secondSet);
    jedis.sadd("{tag1}set3", thirdSet);

    Long resultSize = jedis.sunionstore("{tag1}result", "{tag1}set1", "{tag1}set2");
    assertThat(resultSize).isEqualTo(7);

    Set<String> resultSet = jedis.smembers("{tag1}result");
    String[] expected =
        new String[] {"pear", "apple", "plum", "orange", "peach", "microsoft", "linux"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long notEmptyResultSize =
        jedis.sunionstore("{tag1}notempty", "{tag1}nonexistent", "{tag1}set1");
    Set<String> notEmptyResultSet = jedis.smembers("{tag1}notempty");
    assertThat(notEmptyResultSize).isEqualTo(firstSet.length);
    assertThat(notEmptyResultSet).containsExactlyInAnyOrder(firstSet);

    jedis.sadd("{tag1}newEmpty", "born2die");
    jedis.srem("{tag1}newEmpty", "born2die");
    Long newNotEmptySize =
        jedis.sunionstore("{tag1}newNotEmpty", "{tag1}set2", "{tag1}newEmpty");
    Set<String> newNotEmptySet = jedis.smembers("{tag1}newNotEmpty");
    assertThat(newNotEmptySize).isEqualTo(secondSet.length);
    assertThat(newNotEmptySet).containsExactlyInAnyOrder(secondSet);
  }

  @Test
  public void testSUnionStore_withNonExistentKeys() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{tag1}set1", firstSet);

    Long resultSize =
        jedis.sunionstore("{tag1}set1", "{tag1}nonExistent1", "{tag1}nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("{tag1}set1")).isFalse();
  }

  @Test
  public void testSUnionStore_withNonExistentKeys_andNonSetTarget() {
    jedis.set("{tag1}string1", "stringValue");

    Long resultSize =
        jedis.sunionstore("{tag1}string1", "{tag1}nonExistent1", "{tag1}nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("{tag1}set1")).isFalse();
  }

  @Test
  public void testSUnionStore_withNonSetKey() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{tag1}set1", firstSet);
    jedis.set("{tag1}string1", "value1");

    assertThatThrownBy(() -> jedis.sunionstore("{tag1}set1", "{tag1}string1"))
        .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
    assertThat(jedis.exists("{tag1}set1")).isTrue();
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

    jedis.sadd("{tag1}master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("{tag1}set-" + i, otherSets.get(i).toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sunionstore("{tag1}master", "{tag1}master", "{tag1}set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sunionstore("{tag1}master", "{tag1}master", "{tag1}set-" + i);
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

    assertThat(jedis.smembers("{tag1}master").toArray())
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
