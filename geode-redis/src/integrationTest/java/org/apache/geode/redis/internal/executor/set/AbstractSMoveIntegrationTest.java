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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractSMoveIntegrationTest implements RedisPortSupplier {
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
  public void errors_GivenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.SMOVE, 3);
  }

  @Test
  public void testSmove_returnsWrongType_whenWrongSourceIsUsed() {
    jedis.set("a-string", "value");
    assertThatThrownBy(() -> jedis.smove("a-string", "some-set", "foo"))
        .hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);

    jedis.hset("a-hash", "field", "value");
    assertThatThrownBy(() -> jedis.smove("a-hash", "some-set", "foo"))
        .hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void testSmove_returnsWrongType_whenWrongDestinationIsUsed() {
    jedis.sadd("a-set", "foobaz");

    jedis.set("a-string", "value");
    assertThatThrownBy(() -> jedis.smove("a-set", "a-string", "foo"))
        .hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);

    jedis.hset("a-hash", "field", "value");
    assertThatThrownBy(() -> jedis.smove("a-set", "a-hash", "foo"))
        .hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void testSMove() {
    String source = "source";
    String dest = "dest";
    String test = "test";
    int elements = 10;
    String[] strings = generateStrings(elements, "value-");
    jedis.sadd(source, strings);

    long i = 1;
    for (String entry : strings) {
      long results = jedis.smove(source, dest, entry);
      assertThat(results).isEqualTo(1);
      assertThat(jedis.sismember(dest, entry)).isTrue();

      results = jedis.scard(source);
      assertThat(results).isEqualTo(strings.length - i);
      assertThat(jedis.scard(dest)).isEqualTo(i);
      i++;
    }

    assertThat(jedis.smove(test, dest, "unknown-value")).isEqualTo(0);
  }

  @Test
  public void testSMoveNegativeCases() {
    String source = "source";
    String dest = "dest";
    jedis.sadd(source, "sourceField");
    jedis.sadd(dest, "destField");
    String nonexistentField = "nonexistentField";

    assertThat(jedis.smove(source, dest, nonexistentField)).isEqualTo(0);
    assertThat(jedis.sismember(dest, nonexistentField)).isFalse();
    assertThat(jedis.smove(source, "nonexistentDest", nonexistentField)).isEqualTo(0);
    assertThat(jedis.smove("nonExistentSource", dest, nonexistentField)).isEqualTo(0);
  }

  @Test
  public void testConcurrentSMove() {
    String source = "source";
    String dest = "dest";
    int elements = 10000;
    String[] strings = generateStrings(elements, "value-");
    jedis.sadd(source, strings);

    AtomicLong counter = new AtomicLong(0);
    new ConcurrentLoopingThreads(elements,
        (i) -> counter.getAndAdd(jedis.smove(source, dest, strings[i])),
        (i) -> counter.getAndAdd(jedis2.smove(source, dest, strings[i]))).run();

    assertThat(counter.get()).isEqualTo(new Long(strings.length));
    assertThat(jedis.smembers(dest)).containsExactlyInAnyOrder(strings);
    assertThat(jedis.scard(source)).isEqualTo(0L);
  }

  @Test
  public void testConcurrentSMove_withDifferentDestination() {
    String source = "source";
    String dest1 = "dest1";
    String dest2 = "dest2";
    int elements = 10000;
    String[] strings = generateStrings(elements, "value-");
    jedis.sadd(source, strings);

    AtomicLong counter = new AtomicLong(0);
    new ConcurrentLoopingThreads(elements,
        (i) -> counter.getAndAdd(jedis.smove(source, dest1, strings[i])),
        (i) -> counter.getAndAdd(jedis2.smove(source, dest2, strings[i]))).run();

    List<String> result = new ArrayList<>();
    result.addAll(jedis.smembers(dest1));
    result.addAll(jedis.smembers(dest2));

    assertThat(counter.get()).isEqualTo(new Long(strings.length));
    assertThat(result).containsExactlyInAnyOrder(strings);
    assertThat(jedis.scard(source)).isEqualTo(0L);
  }


  private long moveSetElements(String source, String dest, String[] strings,
      Jedis jedis) {
    long results = 0;
    for (String entry : strings) {
      results += jedis.smove(source, dest, entry);
      Thread.yield();
    }
    return results;
  }

  private String[] generateStrings(int elements, String prefix) {
    Set<String> strings = new HashSet<>();
    for (int i = 0; i < elements; i++) {
      strings.add(prefix + i);
    }
    return strings.toArray(new String[strings.size()]);
  }
}
