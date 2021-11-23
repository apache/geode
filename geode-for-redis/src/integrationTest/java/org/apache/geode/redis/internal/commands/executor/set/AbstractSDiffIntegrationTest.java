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

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractSDiffIntegrationTest implements RedisIntegrationTest {
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
  public void sdiffErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SDIFF, 1);
  }

  @Test
  public void sdiffstoreErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SDIFFSTORE, 2);
  }

  @Test
  public void testSDiff() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("{user1}set1", firstSet);
    jedis.sadd("{user1}set2", secondSet);
    jedis.sadd("{user1}set3", thirdSet);

    Set<String> result =
        jedis.sdiff("{user1}set1", "{user1}set2", "{user1}set3", "{user1}doesNotExist");
    String[] expected = new String[] {"pear", "plum", "orange"};
    assertThat(result).containsExactlyInAnyOrder(expected);

    Set<String> shouldNotChange = jedis.smembers("{user1}set1");
    assertThat(shouldNotChange).containsExactlyInAnyOrder(firstSet);

    Set<String> shouldBeEmpty =
        jedis.sdiff("{user1}doesNotExist", "{user1}set1", "{user1}set2", "{user1}set3");
    assertThat(shouldBeEmpty).isEmpty();

    Set<String> copySet = jedis.sdiff("{user1}set1");
    assertThat(copySet).containsExactlyInAnyOrder(firstSet);
  }

  @Test
  public void testSDiffStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("{user1}set1", firstSet);
    jedis.sadd("{user1}set2", secondSet);
    jedis.sadd("{user1}set3", thirdSet);

    Long resultSize =
        jedis.sdiffstore("{user1}result", "{user1}set1", "{user1}set2", "{user1}set3");
    Set<String> resultSet = jedis.smembers("{user1}result");

    String[] expected = new String[] {"pear", "plum", "orange"};
    assertThat(resultSize).isEqualTo(expected.length);
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long otherResultSize = jedis.sdiffstore("{user1}set1", "{user1}set1", "{user1}result");
    Set<String> otherResultSet = jedis.smembers("{user1}set1");
    String[] otherExpected = new String[] {"apple", "peach"};
    assertThat(otherResultSize).isEqualTo(otherExpected.length);
    assertThat(otherResultSet).containsExactlyInAnyOrder(otherExpected);

    Long emptySetSize =
        jedis.sdiffstore("{user1}newEmpty", "{user1}nonexistent", "{user1}set2", "{user1}set3");
    Set<String> emptyResultSet = jedis.smembers("{user1}newEmpty");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    emptySetSize =
        jedis.sdiffstore("{user1}set1", "{user1}nonexistent", "{user1}set2", "{user1}set3");
    emptyResultSet = jedis.smembers("{user1}set1");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    Long copySetSize = jedis.sdiffstore("{user1}copySet", "{user1}set2");
    Set<String> copyResultSet = jedis.smembers("{user1}copySet");
    assertThat(copySetSize).isEqualTo(secondSet.length);
    assertThat(copyResultSet.toArray()).containsExactlyInAnyOrder((Object[]) secondSet);
  }

  @Test
  public void testSDiffStore_withNonExistentKeys() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{user1}set1", firstSet);

    Long resultSize = jedis.sdiffstore("{user1}set1", "{user1}nonExistent1", "{user1}nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("{user1}set1")).isFalse();
  }

  @Test
  public void testSDiffStore_withNonExistentKeys_andNonSetTarget() {
    jedis.set("{user1}string1", "stringValue");

    Long resultSize =
        jedis.sdiffstore("{user1}string1", "{user1}nonExistent1", "{user1}nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("{user1}set1")).isFalse();
  }

  @Test
  public void testSDiffStore_withNonSetKey() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{user1}set1", firstSet);
    jedis.set("{user1}string1", "value1");

    assertThatThrownBy(() -> jedis.sdiffstore("{user1}set1", "{user1}string1"))
        .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
    assertThat(jedis.exists("{user1}set1")).isTrue();
  }

  @Test
  public void testConcurrentSDiffStore() throws InterruptedException {
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
      jedis.sadd("{user1}master", otherSets.get(i).toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sdiffstore("{user1}master", "{user1}master", "{user1}set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sdiffstore("{user1}master", "{user1}master", "{user1}set-" + i);
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("{user1}master").toArray())
        .containsExactlyInAnyOrder(masterSet.toArray());
  }
}
