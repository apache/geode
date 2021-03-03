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
package org.apache.geode.redis.internal.executor.set;

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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractSInterIntegrationTest implements RedisPortSupplier {
  private Jedis jedis;
  private Jedis jedis2;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
    jedis2.close();
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
  public void testSInter() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Set<String> resultSet = jedis.sinter("set1", "set2", "set3");

    String[] expected = new String[] {"peach"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Set<String> emptyResultSet = jedis.sinter("nonexistent", "set2", "set3");
    assertThat(emptyResultSet).isEmpty();

    jedis.sadd("newEmpty", "born2die");
    jedis.srem("newEmpty", "born2die");
    Set<String> otherEmptyResultSet = jedis.sinter("set2", "newEmpty");
    assertThat(otherEmptyResultSet).isEmpty();
  }

  @Test
  public void testSInterStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Long resultSize = jedis.sinterstore("result", "set1", "set2", "set3");
    Set<String> resultSet = jedis.smembers("result");

    String[] expected = new String[] {"peach"};
    assertThat(resultSize).isEqualTo(expected.length);
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long otherResultSize = jedis.sinterstore("set1", "set1", "set2");
    Set<String> otherResultSet = jedis.smembers("set1");
    String[] otherExpected = new String[] {"apple", "peach"};
    assertThat(otherResultSize).isEqualTo(otherExpected.length);
    assertThat(otherResultSet).containsExactlyInAnyOrder(otherExpected);

    Long emptySetSize = jedis.sinterstore("newEmpty", "nonexistent", "set2", "set3");
    Set<String> emptyResultSet = jedis.smembers("newEmpty");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    emptySetSize = jedis.sinterstore("set1", "nonexistent", "set2", "set3");
    emptyResultSet = jedis.smembers("set1");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    Long copySetSize = jedis.sinterstore("copySet", "set2", "newEmpty");
    Set<String> copyResultSet = jedis.smembers("copySet");
    assertThat(copySetSize).isEqualTo(0);
    assertThat(copyResultSet).isEmpty();
  }

  @Test
  public void testSInterStore_withNonExistentKeys() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("set1", firstSet);

    Long resultSize = jedis.sinterstore("set1", "nonExistent1", "nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("set1")).isFalse();
  }

  @Test
  public void testSInterStore_withNonExistentKeys_andNonSetTarget() {
    jedis.set("string1", "stringValue");

    Long resultSize = jedis.sinterstore("string1", "nonExistent1", "nonExistent2");
    assertThat(resultSize).isEqualTo(0);
    assertThat(jedis.exists("set1")).isFalse();
  }

  @Test
  public void testSInterStore_withNonSetKey() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("set1", firstSet);
    jedis.set("string1", "value1");

    assertThatThrownBy(() -> jedis.sinterstore("set1", "string1"))
        .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
    assertThat(jedis.exists("set1")).isTrue();
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
        jedis2.sinterstore("master", "master", "set-" + i);
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
