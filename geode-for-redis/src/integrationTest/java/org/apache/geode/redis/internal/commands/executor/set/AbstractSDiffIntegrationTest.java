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

public abstract class AbstractSDiffIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String setKey = "{user1}setkey";
  private static final String nonExistentSetKey = "{user1}nonExistentSet";

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
  public void sdiffErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SDIFF, 1);
  }

  @Test
  public void sdiff_returnsAllValuesInSet() {
    String[] values = createKeyValuesSet();
    assertThat(jedis.sdiff(setKey)).containsExactlyInAnyOrder(values);
  }

  @Test
  public void sdiffWithNonExistentSet_returnsEmptySet() {
    assertThat(jedis.sdiff(nonExistentSetKey)).isEmpty();
  }

  @Test
  public void sdiffWithMultipleNonExistentSet_returnsEmptySet() {
    assertThat(jedis.sdiff("{user1}nonExistentSet1", "{user1}nonExistentSet2")).isEmpty();
  }

  @Test
  public void sdiffWithNonExistentSetAndSet_returnsAllValuesInSet() {
    String[] values = createKeyValuesSet();
    assertThat(jedis.sdiff(nonExistentSetKey, setKey)).isEmpty();
    assertThat(jedis.smembers(nonExistentSetKey)).isEmpty();
    assertThat(jedis.smembers(setKey)).containsExactlyInAnyOrder(values);
  }

  @Test
  public void sdiffWithSetAndNonExistentSet_returnsAllValuesInSet() {
    String[] values = createKeyValuesSet();
    assertThat(jedis.sdiff(setKey, nonExistentSetKey))
        .containsExactlyInAnyOrder(values);
    assertThat(jedis.smembers(setKey)).containsExactlyInAnyOrder(values);
    assertThat(jedis.smembers(nonExistentSetKey)).isEmpty();
  }

  @Test
  public void sdiffWithSetsWithDifferentValues_returnsFirstSetValues() {
    String[] firstValues = createKeyValuesSet();
    String[] secondValues = new String[] {"windows", "microsoft", "linux"};
    jedis.sadd("{user1}setkey2", secondValues);

    assertThat(jedis.sdiff(setKey, "{user1}setkey2")).containsExactlyInAnyOrder(firstValues);
  }

  @Test
  public void sdiffWithSetsWithSomeSharedValues_returnsDiffOfSets() {
    createKeyValuesSet();
    String[] secondValues = new String[] {"apple", "bottoms", "boots", "fur", "peach"};
    jedis.sadd("{user1}setkey2", secondValues);

    Set<String> result =
        jedis.sdiff(setKey, "{user1}setkey2");
    String[] expected = new String[] {"orange", "plum", "pear"};
    assertThat(result).containsExactlyInAnyOrder(expected);
  }

  @Test
  public void sdiffWithSetsWithAllSharedValues_returnsEmptySet() {
    String[] values = createKeyValuesSet();
    jedis.sadd("{user1}setkey2", values);

    assertThat(jedis.sdiff(setKey, "{user1}setkey2")).isEmpty();
  }

  @Test
  public void sdiffWithMultipleSets_returnsDiffOfSets() {
    String[] values = createKeyValuesSet();
    String[] secondValues = new String[] {"apple", "bottoms", "boots", "fur", "peach"};
    String[] thirdValues = new String[] {"queen", "opera", "boho", "orange"};

    jedis.sadd("{user1}setkey2", secondValues);
    jedis.sadd("{user1}setkey3", thirdValues);

    String[] expected = new String[] {"pear", "plum"};
    assertThat(jedis.sdiff(setKey, "{user1}setkey2", "{user1}setkey3"))
        .containsExactlyInAnyOrder(expected);
  }

  @Test
  public void sdiffWithDifferentyKeyType_returnsWrongTypeError() {
    jedis.set("ding", "dong");
    assertThatThrownBy(() -> jedis.sdiff("ding")).hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String[] values = new String[] {"pear", "apple", "plum", "orange", "peach"};
    Set<String> valuesList = new HashSet<>(Arrays.asList(values));

    jedis.sadd("{user1}firstset", values);
    jedis.sadd("{user1}secondset", values);

    final AtomicReference<Set<String>> sdiffResultReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem("{user1}secondset", values),
        i -> sdiffResultReference.set(jedis.sdiff("{user1}firstset", "{user1}secondset")))
            .runWithAction(() -> {
              assertThat(sdiffResultReference).satisfiesAnyOf(
                  sdiffResult -> assertThat(sdiffResult.get()).isEmpty(),
                  sdiffResult -> assertThat(sdiffResult.get())
                      .containsExactlyInAnyOrderElementsOf(valuesList));
              jedis.sadd("{user1}secondset", values);
            });
  }


  @Test
  public void sdiffstoreErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SDIFFSTORE, 2);
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

  private String[] createKeyValuesSet() {
    String[] values = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{user1}setkey", values);
    return values;
  }
}
