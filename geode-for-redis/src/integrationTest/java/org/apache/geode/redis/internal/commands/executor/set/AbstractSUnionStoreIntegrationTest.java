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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_DIFFERENT_SLOTS;
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

public abstract class AbstractSUnionStoreIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String DESTINATION_KEY = "{tag1}destinationKey";
  private static final String[] DESTINATION_MEMBERS =
      {"six", "seven", "eight", "nine", "ten", "one", "two"};
  private static final String SET_KEY_1 = "{tag1}setKey1";
  private static final String[] SET_MEMBERS_1 = {"one", "two", "three", "four", "five"};
  private static final String NON_EXISTENT_SET = "{tag1}nonExistentSet";
  private static final String SET_KEY_2 = "{tag1}setKey2";

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
  public void sunionstore_givenTooFewArguments_returnsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.SUNION, 1);
  }

  @Test
  public void sunionstore_withNonExistentDest_withExistentSet_returnsUnionSize_storesUnion() {
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    assertThat(jedis.sunionstore(DESTINATION_KEY, SET_KEY_1)).isEqualTo(SET_MEMBERS_1.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunionstore_withNonExistentDest_withNonExistentSet_returnsZero_destKeyDoesNotExist() {
    assertThat(jedis.sunionstore(DESTINATION_KEY, NON_EXISTENT_SET)).isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void sunionstore_withNonExistentDest_withOneExistentAndOneNonExistentSet_returnsUnionSize_storesUnion() {
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    assertThat(jedis.sunionstore(DESTINATION_KEY, SET_KEY_1, NON_EXISTENT_SET))
        .isEqualTo(SET_MEMBERS_1.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunionstore_withNonExistentDest_withOneNonExistentAndOneExistentSet_returnsUnionSize_storesUnion() {
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    assertThat(jedis.sunionstore(DESTINATION_KEY, NON_EXISTENT_SET, SET_KEY_1))
        .isEqualTo(SET_MEMBERS_1.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunionstore_withNonExistentDest_withNonOverlappingSets_returnsUnionSize_storesUnion() {
    String[] secondSetMembers = new String[] {"apple", "microsoft", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, secondSetMembers);

    String[] result =
        {"one", "two", "three", "four", "five", "apple", "microsoft", "linux", "peach"};
    assertThat(jedis.sunionstore(DESTINATION_KEY, SET_KEY_1, SET_KEY_2)).isEqualTo(result.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(result);

  }

  @Test
  public void sunionstore_withNonExistentDest_withSomeSharedValues_returnsUnionSize_storesUnion() {
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, secondSetMembers);

    String[] result = {"one", "two", "three", "four", "five", "linux", "peach"};
    assertThat(jedis.sunionstore(DESTINATION_KEY, SET_KEY_1, SET_KEY_2)).isEqualTo(result.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sunionstore_withNonExistentDest_withAllSharedValues_returnsUnionSize_storesUnion() {
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, SET_MEMBERS_1);

    assertThat(jedis.sunionstore(DESTINATION_KEY, SET_KEY_1, SET_KEY_2))
        .isEqualTo(SET_MEMBERS_1.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunionstore_withNonExistentDest_withMultipleNonExistentSets_returnsZero_destKeyDoesNotExist() {
    assertThat(jedis.sunionstore(DESTINATION_KEY, NON_EXISTENT_SET, "{tag1}nonExistentSet2"))
        .isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void sunionstore_withExistentDest_withExistentSet_returnsUnionSize_storesUnion() {
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    assertThat(jedis.sunionstore(DESTINATION_KEY, SET_KEY_1)).isEqualTo(SET_MEMBERS_1.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunionstore_withExistentDest_withNonExistentSet_returnsZero_destKeyDoesNotExist() {
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);
    assertThat(jedis.sunionstore(DESTINATION_KEY, NON_EXISTENT_SET)).isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void sunionstore_withExistentDest_withNonOverlappingSets_returnsUnionSize_storesUnion() {
    String[] secondSetMembers = new String[] {"apple", "microsoft", "linux", "peach"};
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, secondSetMembers);

    String[] result =
        {"one", "two", "three", "four", "five", "apple", "microsoft", "linux", "peach"};
    assertThat(jedis.sunionstore(DESTINATION_KEY, SET_KEY_1, SET_KEY_2)).isEqualTo(result.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(result);

  }

  @Test
  public void sunionstore_withExistentDest_withAllSharedValues_returnsUnionSize_storesUnion() {
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, SET_MEMBERS_1);

    assertThat(jedis.sunionstore(DESTINATION_KEY, SET_KEY_1, SET_KEY_2))
        .isEqualTo(SET_MEMBERS_1.length);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunionstore_withNonSetDestKey_withExistentSet_returnsUnionSize_storesUnion() {
    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    assertThat(jedis.sunionstore(stringKey, SET_KEY_1)).isEqualTo(SET_MEMBERS_1.length);
    assertThat(jedis.smembers(stringKey)).containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunionstore_withNonSetDestKey_withNonExistentSet_returnsZero_destKeyDoesNotExist() {
    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    assertThat(jedis.sunionstore(stringKey, NON_EXISTENT_SET)).isEqualTo(0);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void sunionstore_withNonExistentDest_withNonSetKeyAsFirstKey_returnsWrongTypeError() {
    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, "doorbell");
    assertThatThrownBy(() -> jedis.sunionstore(DESTINATION_KEY, stringKey, SET_KEY_1, SET_KEY_2))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void sunionstore_withExistentDest_withNonSetKeyAsThirdKey_returnsWrongTypeError() {
    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, "doorbell");
    assertThatThrownBy(() -> jedis.sunionstore(DESTINATION_KEY, SET_KEY_1, SET_KEY_2, stringKey))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void sunionstore_withNonExistentDest_withNonSetKeyAsThirdKeyAndNonExistentSetAsFirstKey_returnsWrongTypeError() {
    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, "doorbell");
    assertThatThrownBy(
        () -> jedis.sunionstore(DESTINATION_KEY, NON_EXISTENT_SET, SET_KEY_1, stringKey))
            .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void sunionstore_withSetsFromDifferentSlots_returnsCrossSlotError() {
    String setKeyDifferentSlot = "{tag2}setKey2";
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(setKeyDifferentSlot, secondSetMembers);

    assertThatThrownBy(() -> jedis.sunionstore(DESTINATION_KEY, SET_KEY_1, setKeyDifferentSlot))
        .hasMessageContaining(ERROR_DIFFERENT_SLOTS);
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, secondSetMembers);

    String[] unionMembers = {"one", "two", "three", "four", "five", "linux", "peach"};
    final AtomicLong sunionstoreResultReference = new AtomicLong(0);
    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem(SET_KEY_2, secondSetMembers),
        i -> sunionstoreResultReference
            .set(jedis.sunionstore(DESTINATION_KEY, SET_KEY_1, SET_KEY_2)))
                .runWithAction(() -> {
                  // Check sunionstore return size of union
                  assertThat(sunionstoreResultReference).satisfiesAnyOf(
                      sunionstoreSize -> assertThat(sunionstoreSize.get())
                          .isEqualTo(SET_MEMBERS_1.length),
                      sunionstoreSize -> assertThat(sunionstoreSize.get())
                          .isEqualTo(unionMembers.length));
                  // Checks if values were stored in destination key
                  assertThat(DESTINATION_KEY).satisfiesAnyOf(
                      key -> assertThat(jedis.smembers(DESTINATION_KEY))
                          .containsExactlyInAnyOrder(SET_MEMBERS_1),
                      key -> assertThat(jedis.smembers(DESTINATION_KEY))
                          .containsExactlyInAnyOrder(unionMembers));
                  jedis.sadd(SET_KEY_2, secondSetMembers);
                  jedis.del(DESTINATION_KEY);
                });
  }
}
