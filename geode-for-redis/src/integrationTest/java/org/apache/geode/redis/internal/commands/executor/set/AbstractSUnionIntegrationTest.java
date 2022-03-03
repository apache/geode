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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.Command.SUNION;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSUnionIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
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
  public void sunionErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, SUNION, 1);
  }

  @Test
  public void sunion_returnsAllValuesInSet_setNotModified() {
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    assertThat(jedis.sunion(SET_KEY_1)).containsExactlyInAnyOrder(SET_MEMBERS_1);
    assertThat(jedis.smembers(SET_KEY_1)).containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunion_withNonExistentSet_returnsEmptySet_nonExistentKeyDoesNotExist() {
    assertThat(jedis.sunion(NON_EXISTENT_SET)).isEmpty();
    assertThat(jedis.exists(NON_EXISTENT_SET)).isFalse();
  }

  @Test
  public void sunion_withNonExistentSetAndSet_returnsAllValuesInSet() {
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    assertThat(jedis.sunion(NON_EXISTENT_SET, SET_KEY_1))
        .containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunion_withSetAndNonExistentSet_returnsAllValuesInSet() {
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    assertThat(jedis.sunion(SET_KEY_1, NON_EXISTENT_SET))
        .containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunion_withMultipleNonExistentSets_returnsEmptySet() {
    assertThat(jedis.sunion(NON_EXISTENT_SET, "{tag1}nonExistentSet2")).isEmpty();
  }

  @Test
  public void sunion_withNonOverlappingSets_returnsUnionOfSets() {
    String[] secondSetMembers = new String[] {"apple", "microsoft", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, secondSetMembers);

    String[] result =
        {"one", "two", "three", "four", "five", "apple", "microsoft", "linux", "peach"};
    assertThat(jedis.sunion(SET_KEY_1, SET_KEY_2)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sunion_withSetsWithSomeSharedValues_returnsUnionOfSets() {
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, secondSetMembers);

    String[] result = {"one", "two", "three", "four", "five", "linux", "peach"};
    assertThat(jedis.sunion(SET_KEY_1, SET_KEY_2)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sunion_withSetsWithAllSharedValues_returnsUnionOfSets() {
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, SET_MEMBERS_1);

    assertThat(jedis.sunion(SET_KEY_1, SET_KEY_2)).containsExactlyInAnyOrder(SET_MEMBERS_1);
  }

  @Test
  public void sunion_withSetsFromDifferentSlots_returnsCrossSlotError() {
    String setKeyDifferentSlot = "{tag2}setKey2";
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(setKeyDifferentSlot, secondSetMembers);

    assertThatThrownBy(() -> jedis.sendCommand(SET_KEY_1, SUNION, SET_KEY_1, setKeyDifferentSlot))
        .hasMessage(ERROR_WRONG_SLOT);
  }


  @Test
  public void sunion_withDifferentKeyTypeAndTwoSetKeys_returnsWrongTypeError() {
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, secondSetMembers);

    String diffKey = "{tag1}diffKey";
    jedis.set(diffKey, "dong");
    assertThatThrownBy(() -> jedis.sunion(diffKey, SET_KEY_1, SET_KEY_2))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void sunion_withTwoSetKeysAndDifferentKeyType_returnsWrongTypeError() {
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, secondSetMembers);

    String diffKey = "{tag1}diffKey";
    jedis.set(diffKey, "dong");
    assertThatThrownBy(() -> jedis.sunion(SET_KEY_1, SET_KEY_2, diffKey))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void sunion_withNonSetKeyAsThirdKeyAndNonExistentSetAsFirstKey_returnsWrongTypeError() {
    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    assertThatThrownBy(() -> jedis.sunion(NON_EXISTENT_SET, SET_KEY_1, stringKey))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String[] secondSetMembers = new String[] {"one", "two", "linux", "peach"};
    jedis.sadd(SET_KEY_1, SET_MEMBERS_1);
    jedis.sadd(SET_KEY_2, secondSetMembers);

    String[] unionMembers = {"one", "two", "three", "four", "five", "linux", "peach"};

    final AtomicReference<Set<String>> sunionResultReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem(SET_KEY_2, secondSetMembers),
        i -> sunionResultReference.set(jedis.sunion(SET_KEY_1, SET_KEY_2)))
            .runWithAction(() -> {
              assertThat(sunionResultReference).satisfiesAnyOf(
                  sunionResult -> assertThat(sunionResult.get())
                      .containsExactlyInAnyOrder(SET_MEMBERS_1),
                  sunionResult -> assertThat(sunionResult.get())
                      .containsExactlyInAnyOrder(unionMembers));
              jedis.sadd(SET_KEY_2, unionMembers);
            });
  }
}
