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

  private static final String destinationKey = "{tag1}destinationKey";
  private static final String[] destinationMembers =
      {"six", "seven", "eight", "nine", "ten", "one", "two"};
  private static final String setKey = "{tag1}setKey";
  private static final String[] setMembers = {"one", "two", "three", "four", "five"};
  private static final String nonExistentSetKey = "{tag1}nonExistentSet";

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
  public void sdiffstore_DifferentKeyType_returnsWrongTypeError() {
    jedis.set("{tag1}ding", "{tag1}dong");
    assertThatThrownBy(() -> jedis.sdiffstore(destinationKey, "{tag1}ding"))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void sdiffstore_withDifferentKeyTypeAndTwoSetKeys_returnsWrongTypeError() {
    String diffTypeKey = "{tag1}ding";
    jedis.set(diffTypeKey, "dong");

    String secondSetKey = "{tag1}secondKey";
    jedis.sadd(setKey, setMembers);
    jedis.sadd(secondSetKey, setMembers);
    assertThatThrownBy(() -> jedis.sdiffstore(destinationKey, diffTypeKey, setKey, secondSetKey))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void sdiffstore_withTwoSetKeysAndDifferentKeyType_returnsWrongTypeError() {
    String diffTypeKey = "{tag1}ding";
    jedis.set(diffTypeKey, "dong");

    String secondSetKey = "{tag1}secondKey";
    jedis.sadd(setKey, setMembers);
    jedis.sadd(secondSetKey, setMembers);
    assertThatThrownBy(() -> jedis.sdiffstore(destinationKey, setKey, secondSetKey, diffTypeKey))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withOneExistentSet_returnsSDiffSizeAndStoresSDiff() {
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey)).isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withNonExistentSet_returnsZeroAndDestKeyDoesNotExist() {
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey)).isEqualTo(0);
    assertThat(jedis.exists(destinationKey)).isFalse();
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withOneExistentAndOneNonExistentSet_returnsSDiffSizeAndStoresSDiff() {
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, nonExistentSetKey))
        .isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withOneNonExistentAndOneExistentSet_returnsZeroAndDestKeyDoesNotExist() {
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey, setKey)).isEqualTo(0);
    assertThat(jedis.exists(destinationKey)).isFalse();
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withOverlappingSets_returnsSDiffSizeAndStoresSDiff() {
    String key = "{tag1}key";
    String[] result = {"three", "four", "five"};
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, destinationMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(3);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withNonOverlappingSets_returnsSDiffSizeAndStoresSDiff() {
    String key = "{tag1}key";
    String[] members = {"nine", "twelve"};
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, members);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withIdenticalSets_returnsZeroAndDestKeyDoesNotExist() {
    String key = "{tag1}key";
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(0);
    assertThat(jedis.exists(destinationKey)).isFalse();
  }

  @Test
  public void sdiffstoreWithNonExistentDest_withNonexistentSets_returnsZeroAndDestKeyDoesNotExist() {
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey, "{tag1}nonExistentKey2"))
        .isEqualTo(0);
    assertThat(jedis.exists(destinationKey)).isFalse();
  }

  // Destination Key has members
  @Test
  public void sdiffstoreWithExistentDest_withOverlappingSets_returnsSDiffSizeAndStoresSDiff() {
    String key = "{tag1}key";
    String[] result = {"three", "four", "five"};
    jedis.sadd(destinationKey, destinationMembers);
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, destinationMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(3);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sdiffstoreWithExistentDest_withIdenticalSets_returnsZeroAndDestKeyDoesNotExist() {
    String key = "{tag1}key";
    jedis.sadd(destinationKey, destinationMembers);
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(0);
    assertThat(jedis.exists(destinationKey)).isFalse();
  }

  @Test
  public void sdiffstoreWithDifferentDestKeyType_withExistentSet_returnsSDiffSizeAndStoresSDiff() {
    jedis.set(destinationKey, "{tag1}destMember");
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey)).isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreWithDifferentDestKeyType_withNonExistentSet_returnsZeroAndDestKeyDoesNotExist() {
    jedis.set(destinationKey, "{tag1}destMember");
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey)).isEqualTo(0);
    assertThat(jedis.exists(destinationKey)).isFalse();
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String firstKey = "{tag1}firstset";
    String secondKey = "{tag1}secondset";
    jedis.sadd(firstKey, setMembers);
    jedis.sadd(secondKey, setMembers);

    final AtomicLong sdiffSize = new AtomicLong(0);
    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem(secondKey, setMembers),
        i -> sdiffSize.set(jedis.sdiffstore(destinationKey, firstKey, secondKey)))
            .runWithAction(() -> {
              // Check sdiffstore return size of diff
              assertThat(sdiffSize).satisfiesAnyOf(
                  sdiffstoreSize -> assertThat(sdiffstoreSize.get()).isEqualTo(0),
                  sdiffstoreSize -> assertThat(sdiffstoreSize.get()).isEqualTo(setMembers.length));
              // Checks if values were stored in destination key
              assertThat(destinationKey).satisfiesAnyOf(
                  key -> assertThat(jedis.exists(key)).isFalse(),
                  key -> assertThat(jedis.smembers(destinationKey))
                      .containsExactlyInAnyOrder(setMembers));
              jedis.sadd(secondKey, setMembers);
              jedis.srem(destinationKey, setMembers);
            });
  }
}
