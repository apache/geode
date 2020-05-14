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
package org.apache.geode.redis.sets;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class SUnionIntegrationTest {
  static Jedis jedis;
  static Jedis jedis2;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), 10000000);
    jedis2 = new Jedis("localhost", server.getPort(), 10000000);
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
  }

  @After
  public void cleanup() {
    jedis.flushAll();
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testSUnion() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Set<String> resultSet = jedis.sunion("set1", "set2");
    String[] expected =
        new String[] {"pear", "apple", "plum", "orange", "peach", "microsoft", "linux"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Set<String> otherResultSet = jedis.sunion("nonexistent", "set1");
    assertThat(otherResultSet).containsExactlyInAnyOrder(firstSet);

    jedis.sadd("newEmpty", "born2die");
    jedis.srem("newEmpty", "born2die");
    Set<String> yetAnotherResultSet = jedis.sunion("set2", "newEmpty");
    assertThat(yetAnotherResultSet).containsExactlyInAnyOrder(secondSet);
  }

  @Test
  public void testSUnionStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Long resultSize = jedis.sunionstore("result", "set1", "set2");
    assertThat(resultSize).isEqualTo(7);

    Set<String> resultSet = jedis.smembers("result");
    String[] expected =
        new String[] {"pear", "apple", "plum", "orange", "peach", "microsoft", "linux"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long notEmptyResultSize = jedis.sunionstore("notempty", "nonexistent", "set1");
    Set<String> notEmptyResultSet = jedis.smembers("notempty");
    assertThat(notEmptyResultSize).isEqualTo(firstSet.length);
    assertThat(notEmptyResultSet).containsExactlyInAnyOrder(firstSet);

    jedis.sadd("newEmpty", "born2die");
    jedis.srem("newEmpty", "born2die");
    Long newNotEmptySize = jedis.sunionstore("newNotEmpty", "set2", "newEmpty");
    Set<String> newNotEmptySet = jedis.smembers("newNotEmpty");
    assertThat(newNotEmptySize).isEqualTo(secondSet.length);
    assertThat(newNotEmptySet).containsExactlyInAnyOrder(secondSet);
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

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("set-" + i, otherSets.get(i).toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sunionstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis2.sunionstore("master", "master", "set-" + i);
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

    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }
}
