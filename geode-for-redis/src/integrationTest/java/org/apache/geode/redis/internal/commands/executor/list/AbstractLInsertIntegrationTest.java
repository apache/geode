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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.args.ListPosition.AFTER;
import static redis.clients.jedis.args.ListPosition.BEFORE;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractLInsertIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final String initialValue = "initialValue";
  public static final String insertedValue = "insertedValue";
  private JedisCluster jedis;

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
  public void linsertErrors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.LINSERT, 4);
  }

  @Test
  public void linsert_onKeyThatDoesNotExist_doesNotCreateKey() {
    assertThat(jedis.linsert(KEY, BEFORE, "not in here", insertedValue)).isZero();
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void linsert_onKeyThatDoesNotExist_withInvalidBefore_errorsBecauseOfWrongNumOfArgs() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.LINSERT, KEY, "LINSERT",
        "notBefore", "doesn't matter", insertedValue))
            .isInstanceOf(JedisDataException.class)
            .hasMessage(
                String.format(WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND, "linsert"));
  }

  @Test
  public void linsert_onKeyThatIsNotAList_Errors() {
    jedis.sadd(KEY, initialValue);

    assertThatThrownBy(() -> jedis.linsert(KEY, BEFORE, initialValue, insertedValue))
        .isInstanceOf(JedisDataException.class)
        .hasMessage(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void linsert_withNonexistentPivot_returnsNegativeOne() {
    jedis.lpush(KEY, initialValue);

    assertThat(jedis.linsert(KEY, BEFORE, "nope", insertedValue)).isEqualTo(-1);

    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue);
    assertThat(jedis.lpop(KEY)).isNull();
  }

  @Test
  public void linsert_withInvalidBEFORE_errors() {
    jedis.lpush(KEY, initialValue);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.LINSERT, KEY, "LINSERT",
        "notBefore", initialValue, insertedValue))
            .isInstanceOf(JedisDataException.class)
            .hasMessage(
                String.format(WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND, "linsert"));
  }

  @Test
  public void linsert_BEFORE_onKeyWithMultipleValues_withValidPivot_insertsValue() {
    jedis.lpush(KEY, initialValue + "4");
    jedis.lpush(KEY, initialValue + "3");
    jedis.lpush(KEY, initialValue + "2");
    jedis.lpush(KEY, initialValue + "1");
    jedis.lpush(KEY, initialValue + "0");

    assertThat(jedis.linsert(KEY, BEFORE, initialValue + "2", insertedValue)).isEqualTo(6L);

    assertThat(jedis.llen(KEY)).isEqualTo(6L);

    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "0");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "1");
    assertThat(jedis.lpop(KEY)).isEqualTo(insertedValue);
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "2");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "3");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "4");
  }

  @Test
  public void linsert_BEFORE_onKeyWithMultipleDuplicateValues_withValidPivot_insertsValue() {
    jedis.lpush(KEY, initialValue + "3");
    jedis.lpush(KEY, initialValue + "2");
    jedis.lpush(KEY, initialValue + "1");
    jedis.lpush(KEY, initialValue + "3");
    jedis.lpush(KEY, initialValue + "0");

    assertThat(jedis.linsert(KEY, BEFORE, initialValue + "3", insertedValue)).isEqualTo(6L);

    assertThat(jedis.llen(KEY)).isEqualTo(6L);

    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "0");
    assertThat(jedis.lpop(KEY)).isEqualTo(insertedValue);
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "3");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "1");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "2");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "3");
  }

  @Test
  public void linsert_AFTER_onKeyWithMultipleDuplicateValues_withValidPivot_insertsValue() {
    jedis.lpush(KEY, initialValue + "3");
    jedis.lpush(KEY, initialValue + "2");
    jedis.lpush(KEY, initialValue + "1");
    jedis.lpush(KEY, initialValue + "3");
    jedis.lpush(KEY, initialValue + "0");

    assertThat(jedis.linsert(KEY, AFTER, initialValue + "3", insertedValue)).isEqualTo(6L);

    assertThat(jedis.llen(KEY)).isEqualTo(6L);

    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "0");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "3");
    assertThat(jedis.lpop(KEY)).isEqualTo(insertedValue);
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "1");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "2");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "3");
  }

  @Test
  public void linsert_AFTER_onKeyWithMultipleValues_withValidPivot_insertsValue() {
    jedis.lpush(KEY, initialValue + "4");
    jedis.lpush(KEY, initialValue + "3");
    jedis.lpush(KEY, initialValue + "2");
    jedis.lpush(KEY, initialValue + "1");
    jedis.lpush(KEY, initialValue + "0");

    assertThat(jedis.linsert(KEY, AFTER, initialValue + "2", insertedValue)).isEqualTo(6L);

    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "0");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "1");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "2");
    assertThat(jedis.lpop(KEY)).isEqualTo(insertedValue);
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "3");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "4");
  }

  @Test
  public void linsert_BEFORE_firstElement() {
    jedis.lpush(KEY, initialValue + "4");
    jedis.lpush(KEY, initialValue + "3");
    jedis.lpush(KEY, initialValue + "2");
    jedis.lpush(KEY, initialValue + "1");
    jedis.lpush(KEY, initialValue + "0");

    assertThat(jedis.linsert(KEY, BEFORE, initialValue + "0", insertedValue)).isEqualTo(6L);

    assertThat(jedis.lpop(KEY)).isEqualTo(insertedValue);
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "0");
  }

  @Test
  public void linsert_AFTER_lastElement() {
    jedis.lpush(KEY, initialValue + "4");
    jedis.lpush(KEY, initialValue + "3");
    jedis.lpush(KEY, initialValue + "2");
    jedis.lpush(KEY, initialValue + "1");
    jedis.lpush(KEY, initialValue + "0");

    assertThat(jedis.linsert(KEY, AFTER, initialValue + "4", insertedValue)).isEqualTo(6L);

    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "0");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "1");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "2");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "3");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "4");
    assertThat(jedis.lpop(KEY)).isEqualTo(insertedValue);
    assertThat(jedis.lpop(KEY)).isNull();
  }

  @Test
  public void testConcurrentLInserts() {
    // we should never see "snake", "inserted value", "snake", etc.
    String[] initialElements = {"lizard", "lizard", "lizard", "snake", "lizard", "lizard"};
    String[] elementsToPush = {"snake", "snake", "snake", "snake", "snake", "snake"};

    jedis.lpush(KEY, initialElements);

    new ConcurrentLoopingThreads(1000,
        i -> jedis.lpush(KEY, elementsToPush),
        i -> jedis.linsert(KEY, BEFORE, "snake", insertedValue)
    ).runWithAction(() -> {
      assertThat(jedis.llen(KEY)).isEqualTo(initialElements.length + elementsToPush.length + 1);

      assertThat(jedis).satisfiesAnyOf(
          //LINSERT happened first
          jedisClient -> {
            assertThat(jedisClient.lindex(KEY, 8))
                .as("failure 1")
                .isEqualTo(insertedValue);
            assertThat(jedisClient.lindex(KEY, 0)).isEqualTo("snake");
          },
      //LPUSH happened first
      jedisClient -> assertThat(jedisClient.lindex(KEY, 0))
          .as("failure 1").isEqualTo(insertedValue)
      );

      jedis.del(KEY);
      jedis.lpush(KEY, initialElements);
    });
  }
}
