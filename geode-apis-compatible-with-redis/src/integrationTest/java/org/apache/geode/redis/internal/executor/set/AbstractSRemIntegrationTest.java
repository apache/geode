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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractSRemIntegrationTest implements RedisPortSupplier {
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
  public void errors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SREM, 2);
  }

  @Test
  public void testSRem_should_ReturnTheNumberOfElementsRemoved() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    Long removedElementsCount = jedis.srem(key, field1, field2);

    assertThat(removedElementsCount).isEqualTo(2);
  }

  @Test
  public void testSRem_should_RemoveSpecifiedElementsFromSet() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    jedis.srem(key, field2);

    Set<String> membersInSet = jedis.smembers(key);
    assertThat(membersInSet).doesNotContain(field2);
  }

  @Test
  public void testSRem_should_ThrowError_givenOnlyKey() {
    assertThatThrownBy(() -> jedis.srem("key")).hasMessageContaining("wrong number of arguments");
  }

  @Test
  public void testSRem_should_notRemoveMembersOfSetNotSpecified() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    jedis.srem(key, field1);

    Set<String> membersInSet = jedis.smembers(key);
    assertThat(membersInSet).containsExactly(field2);
  }

  @Test
  public void testSRem_should_ignoreMembersNotInSpecifiedSet() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    String unkownField = "random-guy";
    jedis.sadd(key, field1, field2);

    long result = jedis.srem(key, unkownField);

    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testSRem_should_throwError_givenKeyThatIsNotASet() {
    String key = "key";
    String value = "value";
    jedis.set(key, value);

    assertThatThrownBy(() -> jedis.srem(key, value))
        .hasMessageContaining("WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  @Test
  public void testSRem_shouldReturnZero_givenNonExistingKey() {
    String key = "key";
    String field = "field";

    long result = jedis.srem(key, field);

    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testSRemDeletesKeyWhenSetIsEmpty() {
    jedis.sadd("farm", "chicken");

    jedis.srem("farm", "chicken");

    assertThat(jedis.exists("farm")).isFalse();
  }

  @Test
  public void testConcurrentSRems() throws InterruptedException {
    int ENTRIES = 1000;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    AtomicLong sremmed1 = new AtomicLong(0);
    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        sremmed1.addAndGet(jedis.srem("master", masterSet.get(i)));
        Thread.yield();
      }
    };

    AtomicLong sremmed2 = new AtomicLong(0);
    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        sremmed2.addAndGet(jedis2.srem("master", masterSet.get(i)));
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master")).isEmpty();
    assertThat(sremmed1.get() + sremmed2.get()).isEqualTo(ENTRIES);
  }
}
