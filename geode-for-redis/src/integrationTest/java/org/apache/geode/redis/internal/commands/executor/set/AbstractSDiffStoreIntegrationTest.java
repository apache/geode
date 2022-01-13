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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSDiffStoreIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;

  private static final String DESTINATION_KEY = "{tag1}destinationKey";
  private static final String[] DESTINATION_MEMBERS =
      {"six", "seven", "eight", "nine", "ten", "one", "two"};
  private static final String SET_KEY = "{tag1}setKey";
  private static final String[] SET_MEMBERS = {"one", "two", "three", "four", "five"};
  private static final String NON_EXISTENT_SET = "{tag1}nonExistentSet";

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
  public void sdiffstoreTooFewArguments_returnsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.SDIFFSTORE, 2);
  }

  @Test
  public void sdiffstore_withNonSetKeyAsFirstKey_returnsWrongTypeError() {
    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    String secondSetKey = "{tag1}secondKey";
    jedis.sadd(SET_KEY, SET_MEMBERS);
    jedis.sadd(secondSetKey, SET_MEMBERS);
    assertThatThrownBy(() -> jedis.sdiffstore(DESTINATION_KEY, stringKey, SET_KEY, secondSetKey))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void sdiffstore_withNonSetKeyAsThirdKey_returnsWrongTypeError() {
    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    String secondSetKey = "{tag1}secondKey";
    jedis.sadd(SET_KEY, SET_MEMBERS);
    jedis.sadd(secondSetKey, SET_MEMBERS);
    assertThatThrownBy(() -> jedis.sdiffstore(DESTINATION_KEY, SET_KEY, secondSetKey, stringKey))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void sdiffstore_withNonSetKeyAsThirdKeyAndNonExistentSetAsFirstKey_returnsWrongTypeError() {
    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    jedis.sadd(SET_KEY, SET_MEMBERS);
    assertThatThrownBy(
        () -> jedis.sdiffstore(DESTINATION_KEY, NON_EXISTENT_SET, SET_KEY, stringKey))
            .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withOneExistentSet_returnsSDiffSizeAndStoresSDiff() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, SET_KEY)).isEqualTo(SET_MEMBERS.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withNonExistentSet_returnsZeroAndDestKeyDoesNotExist() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, NON_EXISTENT_SET)).isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withOneExistentAndOneNonExistentSet_returnsSDiffSizeAndStoresSDiff() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, SET_KEY, NON_EXISTENT_SET))
        .isEqualTo(SET_MEMBERS.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withOneNonExistentAndOneExistentSet_returnsZeroAndDestKeyDoesNotExist() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, NON_EXISTENT_SET, SET_KEY)).isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withOverlappingSets_returnsSDiffSizeAndStoresSDiff() {
    String key = "{tag1}key";
    String[] result = {"three", "four", "five"};
    jedis.sadd(SET_KEY, SET_MEMBERS);
    jedis.sadd(key, DESTINATION_MEMBERS);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, SET_KEY, key)).isEqualTo(3);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withNonOverlappingSets_returnsSDiffSizeAndStoresSDiff() {
    String key = "{tag1}key";
    String[] members = {"nine", "twelve"};
    jedis.sadd(SET_KEY, SET_MEMBERS);
    jedis.sadd(key, members);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, SET_KEY, key)).isEqualTo(SET_MEMBERS.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withIdenticalSets_returnsZeroAndDestKeyDoesNotExist() {
    String key = "{tag1}key";
    jedis.sadd(SET_KEY, SET_MEMBERS);
    jedis.sadd(key, SET_MEMBERS);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, SET_KEY, key)).isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withNonexistentSets_returnsZeroAndDestKeyDoesNotExist() {
    assertThat(jedis.sdiffstore(DESTINATION_KEY, NON_EXISTENT_SET, "{tag1}nonExistentKey2"))
        .isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  // Destination Key has members
  @Test
  public void sdiffstoreWithExistentDest_withOverlappingSets_returnsSDiffSizeAndStoresSDiff() {
    String key = "{tag1}key";
    String[] result = {"three", "four", "five"};
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);
    jedis.sadd(SET_KEY, SET_MEMBERS);
    jedis.sadd(key, DESTINATION_MEMBERS);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, SET_KEY, key)).isEqualTo(3);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sdiffstoreWithExistentDest_withIdenticalSets_returnsZeroAndDestKeyDoesNotExist() {
    String key = "{tag1}key";
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);
    jedis.sadd(SET_KEY, SET_MEMBERS);
    jedis.sadd(key, SET_MEMBERS);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, SET_KEY, key)).isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void sdiffstoreWithDifferentDestKeyType_withExistentSet_returnsSDiffSizeAndStoresSDiff() {
    jedis.set(DESTINATION_KEY, "{tag1}destMember");
    jedis.sadd(SET_KEY, SET_MEMBERS);
    assertThat(jedis.sdiffstore(DESTINATION_KEY, SET_KEY)).isEqualTo(SET_MEMBERS.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS);
  }

  @Test
  public void sdiffstoreWithDifferentDestKeyType_withNonExistentSet_returnsZeroAndDestKeyDoesNotExist() {
    jedis.set(DESTINATION_KEY, "{tag1}destMember");
    assertThat(jedis.sdiffstore(DESTINATION_KEY, NON_EXISTENT_SET)).isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String firstKey = "{tag1}firstset";
    String secondKey = "{tag1}secondset";
    jedis.sadd(firstKey, SET_MEMBERS);
    jedis.sadd(secondKey, SET_MEMBERS);

    final AtomicLong sdiffSize = new AtomicLong(0);
    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem(secondKey, SET_MEMBERS),
        i -> sdiffSize.set(jedis.sdiffstore(DESTINATION_KEY, firstKey, secondKey)))
            .runWithAction(() -> {
              // Check sdiffstore return size of diff
              assertThat(sdiffSize).satisfiesAnyOf(
                  sdiffstoreSize -> assertThat(sdiffstoreSize.get()).isEqualTo(0),
                  sdiffstoreSize -> assertThat(sdiffstoreSize.get()).isEqualTo(SET_MEMBERS.length));
              // Checks if values were stored in destination key
              assertThat(DESTINATION_KEY).satisfiesAnyOf(
                  key -> assertThat(jedis.exists(key)).isFalse(),
                  key -> assertThat(jedis.smembers(DESTINATION_KEY))
                      .containsExactlyInAnyOrder(SET_MEMBERS));
              jedis.sadd(secondKey, SET_MEMBERS);
              jedis.srem(DESTINATION_KEY, SET_MEMBERS);
            });
  }
}
