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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.Command.SMOVE;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSMoveIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String NON_EXISTENT_SET_KEY = "{tag1}nonExistentSet";
  private static final String SOURCE_KEY = "{tag1}sourceKey";
  private static final String[] SOURCE_MEMBERS = {"one", "two", "three", "four", "five"};
  private static final String DESTINATION_KEY = "{tag1}destKey";
  private static final String[] DESTINATION_MEMBERS = {"a", "b", "c"};
  private static final String MOVED_MEMBER = "one";

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
  public void smove_givenWrongNumberOfArguments_returnsError() {
    assertExactNumberOfArgs(jedis, SMOVE, 3);
  }

  @Test
  public void smove_withWrongTypeSource_returnsWrongTypeError() {
    jedis.set(SOURCE_KEY, "value");
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);

    assertThatThrownBy(() -> jedis.smove(SOURCE_KEY, DESTINATION_KEY, MOVED_MEMBER))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void smove_withWrongTypeDest_returnsWrongTypeError() {
    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);
    jedis.set(DESTINATION_KEY, "value");

    assertThatThrownBy(() -> jedis.smove(SOURCE_KEY, DESTINATION_KEY, MOVED_MEMBER))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void smove_withWrongTypeSourceAndDest_returnsWrongTypeError() {
    jedis.set(SOURCE_KEY, "sourceMember");
    jedis.set(DESTINATION_KEY, "destMember");

    assertThatThrownBy(() -> jedis.smove(SOURCE_KEY, DESTINATION_KEY, MOVED_MEMBER))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void smove_withNonExistentSourceAndWrongTypeDestination_returnsZero() {
    jedis.set(DESTINATION_KEY, "not a RedisSet");

    assertThat(jedis.smove(NON_EXISTENT_SET_KEY, DESTINATION_KEY, MOVED_MEMBER))
        .isEqualTo(0);
  }

  @Test
  public void smove_withNonExistentMemberInSourceAndDestinationNotASet_returnsWrongTypeError() {
    String nonExistentMember = "foo";
    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);
    jedis.set(DESTINATION_KEY, "not a set");

    assertThat(jedis.smove(NON_EXISTENT_SET_KEY, DESTINATION_KEY, nonExistentMember))
        .isEqualTo(0);
    assertThatThrownBy(() -> jedis.sismember(DESTINATION_KEY, nonExistentMember))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void smove_withNonExistentSource_returnsZero_sourceKeyDoesNotExist() {
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);

    assertThat(jedis.smove(NON_EXISTENT_SET_KEY, DESTINATION_KEY, MOVED_MEMBER))
        .isEqualTo(0);
    assertThat(jedis.exists(NON_EXISTENT_SET_KEY)).isFalse();
  }

  @Test
  public void smove_withNonExistentMemberInSource_returnsZero_memberNotAddedToDest() {
    String nonExistentMember = "foo";
    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);

    assertThat(jedis.smove(NON_EXISTENT_SET_KEY, DESTINATION_KEY, nonExistentMember))
        .isEqualTo(0);
    assertThat(jedis.sismember(DESTINATION_KEY, nonExistentMember)).isFalse();
  }

  @Test
  public void smove_withExistentSourceAndNonExistentDest_returnsOne_memberMovedFromSourceToCreatedDest() {
    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);

    String[] sourceResult = ArrayUtils.remove(SOURCE_MEMBERS, 0);
    String[] destResult = new String[] {MOVED_MEMBER};

    assertThat(jedis.smove(SOURCE_KEY, DESTINATION_KEY, MOVED_MEMBER))
        .isEqualTo(1);

    assertThat(jedis.smembers(SOURCE_KEY)).containsExactlyInAnyOrder(sourceResult);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(destResult);
  }

  @Test
  public void smove_withExistentSourceAndDest_returnsOne_memberMovedFromSourceToDest() {
    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);

    String[] sourceResult = ArrayUtils.remove(SOURCE_MEMBERS, 0);
    String[] destResult = ArrayUtils.add(DESTINATION_MEMBERS, MOVED_MEMBER);

    assertThat(jedis.smove(SOURCE_KEY, DESTINATION_KEY, MOVED_MEMBER))
        .isEqualTo(1);

    assertThat(jedis.smembers(SOURCE_KEY)).containsExactlyInAnyOrder(sourceResult);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(destResult);
  }

  @Test
  public void smove_withSameSourceAndDest_withMemberInDest_returnsOne_setNotModified() {
    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);
    assertThat(jedis.smove(SOURCE_KEY, SOURCE_KEY, MOVED_MEMBER))
        .isEqualTo(1);

    assertThat(jedis.smembers(SOURCE_KEY)).containsExactlyInAnyOrder(SOURCE_MEMBERS);
  }

  @Test
  public void smove_withExistentSourceAndDest_withMemberInDest_returnsOne_memberRemovedFromSource() {
    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);
    String[] newDestMembers = ArrayUtils.add(DESTINATION_MEMBERS, MOVED_MEMBER);
    jedis.sadd(DESTINATION_KEY, newDestMembers);

    String[] sourceResult = ArrayUtils.remove(SOURCE_MEMBERS, 0);

    assertThat(jedis.smove(SOURCE_KEY, DESTINATION_KEY, MOVED_MEMBER))
        .isEqualTo(1);

    assertThat(jedis.smembers(SOURCE_KEY)).containsExactlyInAnyOrder(sourceResult);
    assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(newDestMembers);
  }

  @Test
  public void smoveWithSetsFromDifferentSlots_returnsCrossSlotError() {
    String setKeyDifferentSlot = "{tag2}setKey2";
    jedis.sadd(SOURCE_KEY, setKeyDifferentSlot);
    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);
    jedis.sadd(setKeyDifferentSlot, DESTINATION_MEMBERS);

    assertThatThrownBy(
        () -> jedis.sendCommand(SOURCE_KEY, SMOVE, SOURCE_KEY, setKeyDifferentSlot, MOVED_MEMBER))
            .hasMessage(ERROR_WRONG_SLOT);
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently_withSRemAndSMove() {
    String[] sourceMemberRemoved = ArrayUtils.remove(SOURCE_MEMBERS, 0);
    String[] destMemberAdded = ArrayUtils.add(DESTINATION_MEMBERS, MOVED_MEMBER);

    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);

    final AtomicLong moved = new AtomicLong(0);
    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem(SOURCE_KEY, MOVED_MEMBER),
        i -> moved.set(jedis.smove(SOURCE_KEY, DESTINATION_KEY, MOVED_MEMBER)))
            .runWithAction(() -> {
              if (moved.get() == 1) {
                assertThat(jedis.smembers(SOURCE_KEY))
                    .containsExactlyInAnyOrder(sourceMemberRemoved);
                assertThat(jedis.smembers(DESTINATION_KEY))
                    .containsExactlyInAnyOrder(destMemberAdded);
              } else {
                assertThat(jedis.smembers(SOURCE_KEY))
                    .containsExactlyInAnyOrder(sourceMemberRemoved);
                assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(
                    DESTINATION_MEMBERS);
              }
              jedis.sadd(SOURCE_KEY, MOVED_MEMBER);
              jedis.srem(DESTINATION_KEY, MOVED_MEMBER);
            });
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently_withSMovesFromSameSourceAndDifferentDestination() {
    String[] sourceMemberRemoved = ArrayUtils.remove(SOURCE_MEMBERS, 0);
    String[] destMemberAdded = ArrayUtils.add(DESTINATION_MEMBERS, MOVED_MEMBER);
    String[] nonExisistentMemberAdded = {MOVED_MEMBER};

    jedis.sadd(SOURCE_KEY, SOURCE_MEMBERS);
    jedis.sadd(DESTINATION_KEY, DESTINATION_MEMBERS);

    final AtomicLong movedToNonExistent = new AtomicLong(0);
    final AtomicLong movedToDest = new AtomicLong(0);
    new ConcurrentLoopingThreads(1000,
        i -> movedToNonExistent.set(jedis.smove(SOURCE_KEY, NON_EXISTENT_SET_KEY, MOVED_MEMBER)),
        i -> movedToDest.set(jedis.smove(SOURCE_KEY, DESTINATION_KEY, MOVED_MEMBER)))
            .runWithAction(() -> {
              // Asserts that only one smove was preformed
              assertThat(movedToNonExistent.get() ^ movedToDest.get()).isEqualTo(1);
              assertThat(jedis.smembers(SOURCE_KEY)).containsExactlyInAnyOrder(sourceMemberRemoved);

              if (movedToNonExistent.get() == 1) {
                assertThat(jedis.smembers(NON_EXISTENT_SET_KEY))
                    .containsExactlyInAnyOrder(nonExisistentMemberAdded);
              } else {
                assertThat(jedis.exists(NON_EXISTENT_SET_KEY)).isFalse();
              }

              if (movedToDest.get() == 1) {
                assertThat(jedis.smembers(DESTINATION_KEY))
                    .containsExactlyInAnyOrder(destMemberAdded);
              } else {
                assertThat(jedis.smembers(DESTINATION_KEY)).containsExactlyInAnyOrder(
                    DESTINATION_MEMBERS);
              }

              jedis.sadd(SOURCE_KEY, MOVED_MEMBER);
              jedis.srem(DESTINATION_KEY, MOVED_MEMBER);
              jedis.srem(NON_EXISTENT_SET_KEY, MOVED_MEMBER);
            });
  }
}
