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

package org.apache.geode.redis.internal.executor.sortedset;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.ThreadLocalRandom;

import com.google.common.util.concurrent.AtomicDouble;
import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;

public abstract class AbstractZIncrByIntegrationTest implements RedisIntegrationTest {
  JedisCluster jedis;
  final String STRING_KEY = "key";
  final String STRING_MEMBER = "member";
  final byte[] KEY = STRING_KEY.getBytes();
  final byte[] MEMBER = STRING_MEMBER.getBytes();

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  /************* errors *************/
  @Test
  public void shouldError_givenWrongKeyType() {
    jedis.set(STRING_KEY, "value");
    assertThatThrownBy(
        () -> jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY, "1", "member"))
            .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void shouldError_givenWrongNumberOfArguments() {
    jedis.zadd(KEY, 0.0, MEMBER);
    assertExactNumberOfArgs(jedis, Protocol.Command.ZINCRBY, 3);
  }

  @Test
  public void shouldError_givenNonFloatIncrement() {
    String nonFloatIncrement = "q";
    jedis.zadd(KEY, 1.0, MEMBER);

    assertThatThrownBy(() -> jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY,
        nonFloatIncrement, STRING_MEMBER))
            .hasMessageContaining(RedisConstants.ERROR_NOT_A_VALID_FLOAT);
  }

  @Test
  public void shouldError_givenNaNIncrement() {
    String nanIncrement = "NaN";
    jedis.zadd(STRING_KEY, 1.0, STRING_MEMBER);

    assertThatThrownBy(() -> jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY,
        nanIncrement, STRING_MEMBER))
            .hasMessageContaining(RedisConstants.ERROR_NOT_A_VALID_FLOAT);
  }

  /************* infinity *************/
  @Test
  public void shouldSetScoreToInfinity_ifIncrementWouldExceedMaxValue() {
    double increment = Double.MAX_VALUE / 2;

    jedis.zadd(STRING_KEY, Double.MAX_VALUE, STRING_MEMBER);
    jedis.zincrby(STRING_KEY, increment, STRING_MEMBER);
    assertThat(jedis.zscore(STRING_KEY, STRING_MEMBER)).isEqualTo(POSITIVE_INFINITY);

    jedis.zadd(STRING_KEY, -Double.MAX_VALUE, STRING_MEMBER);
    jedis.zincrby(STRING_KEY, -increment, STRING_MEMBER);
    assertThat(jedis.zscore(STRING_KEY, STRING_MEMBER)).isEqualTo(NEGATIVE_INFINITY);
  }

  @Test
  public void shouldSetScoreToInfinity_givenInfiniteIncrement() {
    jedis.zadd(STRING_KEY, 1.0, STRING_MEMBER);
    assertThat(jedis.zincrby(STRING_KEY, POSITIVE_INFINITY, STRING_MEMBER))
        .isEqualTo(POSITIVE_INFINITY);

    jedis.zadd(STRING_KEY, 1.0, STRING_MEMBER);
    assertThat(jedis.zincrby(STRING_KEY, NEGATIVE_INFINITY, STRING_MEMBER))
        .isEqualTo(NEGATIVE_INFINITY);

    jedis.zadd(STRING_KEY, 1.0, STRING_MEMBER);
    jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY, "inf", STRING_MEMBER);
    assertThat(jedis.zscore(STRING_KEY, STRING_MEMBER)).isEqualTo(POSITIVE_INFINITY);

    jedis.zadd(STRING_KEY, 1.0, STRING_MEMBER);
    jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY, "+inf", STRING_MEMBER);
    assertThat(jedis.zscore(STRING_KEY, STRING_MEMBER)).isEqualTo(POSITIVE_INFINITY);

    jedis.zadd(STRING_KEY, 1.0, STRING_MEMBER);
    jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY, "-inf", STRING_MEMBER);
    assertThat(jedis.zscore(STRING_KEY, STRING_MEMBER)).isEqualTo(NEGATIVE_INFINITY);

    jedis.zadd(STRING_KEY, 1.0, STRING_MEMBER);
    jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY, "Infinity", STRING_MEMBER);
    assertThat(jedis.zscore(STRING_KEY, STRING_MEMBER)).isEqualTo(POSITIVE_INFINITY);

    jedis.zadd(STRING_KEY, 1.0, STRING_MEMBER);
    jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY, "+Infinity", STRING_MEMBER);
    assertThat(jedis.zscore(STRING_KEY, STRING_MEMBER)).isEqualTo(POSITIVE_INFINITY);

    jedis.zadd(STRING_KEY, 1.0, STRING_MEMBER);
    jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY, "-Infinity", STRING_MEMBER);
    assertThat(jedis.zscore(STRING_KEY, STRING_MEMBER)).isEqualTo(NEGATIVE_INFINITY);
  }

  @Test
  public void memberShouldBeCreatedWhenItDoesNotExist_withIncrementOfPositiveInfinity() {
    jedis.zadd(KEY, 0.0, "something".getBytes()); // init the key but not the member

    assertKeyIsCreatedWithIncrementOf(POSITIVE_INFINITY);
  }

  @Test
  public void memberShouldBeCreatedWhenItDoesNotExist_withIncrementOfNegativeInfinity() {
    jedis.zadd(KEY, 0.0, "something".getBytes()); // init the key but not the member

    assertKeyIsCreatedWithIncrementOf(NEGATIVE_INFINITY);
  }

  @Test
  public void shouldCreateNonExistentKeyWithInfiniteScore_whenIncrementIsPositiveInfinity() {
    assertKeyIsCreatedWithIncrementOf(POSITIVE_INFINITY);
  }

  @Test
  public void shouldCreateNonExistentKeyWithInfiniteScore_whenIncrementIsNegativeInfinity() {
    assertKeyIsCreatedWithIncrementOf(NEGATIVE_INFINITY);
  }

  @Test
  public void scoreOfPositiveInfinityShouldRemainInfinite_whenIncrementingOrDecrementing() {
    jedis.zadd(KEY, POSITIVE_INFINITY, MEMBER);

    jedis.zincrby(KEY, 100.0, MEMBER);
    assertThat(jedis.zscore(KEY, MEMBER)).isEqualTo(POSITIVE_INFINITY);

    jedis.zincrby(KEY, -1000.0, MEMBER);
    assertThat(jedis.zscore(KEY, MEMBER)).isEqualTo(POSITIVE_INFINITY);
  }

  @Test
  public void scoreOfNegativeInfinityShouldRemainInfinite_whenIncrementingOrDecrementing() {
    jedis.zadd(KEY, NEGATIVE_INFINITY, MEMBER);

    jedis.zincrby(KEY, 100.0, MEMBER);
    assertThat(jedis.zscore(KEY, MEMBER)).isEqualTo(NEGATIVE_INFINITY);

    jedis.zincrby(KEY, -1000.0, MEMBER);
    assertThat(jedis.zscore(KEY, MEMBER)).isEqualTo(NEGATIVE_INFINITY);
  }

  @Test
  public void incrementingScoreOfInfinityByNegativeInfinity_throwsNaNError() {
    jedis.zadd(KEY, POSITIVE_INFINITY, MEMBER);

    assertThatThrownBy(() -> jedis.zincrby(KEY, NEGATIVE_INFINITY, MEMBER))
        .hasMessageContaining(RedisConstants.ERROR_OPERATION_PRODUCED_NAN);
  }

  @Test
  public void incrementingScoreOfNegativeInfinityByPositiveInfinity_throwsNaNError() {
    jedis.zadd(KEY, NEGATIVE_INFINITY, MEMBER);

    assertThatThrownBy(() -> jedis.zincrby(KEY, POSITIVE_INFINITY, MEMBER))
        .hasMessageContaining(RedisConstants.ERROR_OPERATION_PRODUCED_NAN);
  }

  @Test
  public void incrementingScoreOfNegativeInfinityByNegativeInfinity_IsAllowed() {
    jedis.zadd(KEY, NEGATIVE_INFINITY, MEMBER);

    assertThat(jedis.zincrby(KEY, NEGATIVE_INFINITY, MEMBER)).isEqualTo(NEGATIVE_INFINITY);
  }

  @Test
  public void incrementingScoreOfPositiveInfinityByPositiveiveInfinity_IsAllowed() {
    jedis.zadd(KEY, POSITIVE_INFINITY, MEMBER);

    assertThat(jedis.zincrby(KEY, POSITIVE_INFINITY, MEMBER)).isEqualTo(POSITIVE_INFINITY);
  }

  /************* key or member does not exist *************/
  @Test
  public void shouldCreateNewKey_whenIncrementedKeyDoesNotExist() {
    assertKeyIsCreatedWithIncrementOf(1.5);
  }

  @Test
  public void shouldCreateNewMember_whenIncrementedMemberDoesNotExist() {
    final double increment = 1.5;
    jedis.zadd(KEY, increment, "something".getBytes());

    assertThat(jedis.zincrby(KEY, increment, MEMBER)).isEqualTo(increment);
    assertThat(jedis.zscore(KEY, MEMBER)).isEqualTo(increment);
  }

  /************* happy path *************/
  @Test
  public void shouldIncrementScore_givenKeyAndMemberExist() {
    testZIncrByIncrements(KEY, MEMBER, 2.7, 1.5);
  }

  @Test
  public void shouldIncrementScore_givenNegativeIncrement() {
    testZIncrByIncrements(KEY, MEMBER, 2.7, -1.5);
  }

  @Test
  public void shouldIncrementScoreToANegativeNumber_givenNegativeIncrementGreaterThanScore() {
    testZIncrByIncrements(KEY, MEMBER, 2.7, -5.6);
  }

  @Test
  public void shouldIncrementScore_givenNegativeInitialScoreAndPositiveIncrement() {
    testZIncrByIncrements(KEY, MEMBER, -2.7, 1.7);
  }

  @Test
  public void shouldIncrementScore_givenIncrementInExponentialForm() {
    testZIncrByIncrements(KEY, MEMBER, -2.7, 2e5);
    testZIncrByIncrements(KEY, MEMBER, 2e5, -2.7);
  }

  /************* concurrency *************/
  @Test
  public void testConcurrentZIncrBy_performsAllZIncrBys() {
    AtomicDouble expectedValue = new AtomicDouble(0.0);

    jedis.zadd(STRING_KEY, expectedValue.get(), STRING_MEMBER);

    new ConcurrentLoopingThreads(1000,
        (i) -> {
          double increment = ThreadLocalRandom.current().nextDouble(-50, 50);
          expectedValue.addAndGet(increment);
          jedis.zincrby(STRING_KEY, increment, STRING_MEMBER);
        },
        (i) -> {
          double increment = ThreadLocalRandom.current().nextDouble(-50, 50);
          expectedValue.addAndGet(increment);
          jedis.zincrby(STRING_KEY, increment, STRING_MEMBER);
        }).run();

    Offset<Double> offset = Offset.offset(0.00000001);
    assertThat(jedis.zscore(STRING_KEY, STRING_MEMBER)).isCloseTo(expectedValue.get(), offset);
  }

  /************* helper methods *************/
  private void testZIncrByIncrements(final byte[] key, final byte[] member,
      final double initialScore, final double increment) {
    jedis.zadd(key, initialScore, member);
    assertThat(jedis.zincrby(key, increment, member)).isEqualTo(initialScore + increment);
    assertThat(jedis.zscore(key, member)).isEqualTo(initialScore + increment);
  }

  private void assertKeyIsCreatedWithIncrementOf(double increment) {
    assertThat(jedis.zincrby(KEY, increment, MEMBER)).isEqualTo(increment);
    assertThat(jedis.zscore(KEY, MEMBER)).isEqualTo(increment);
  }
}
