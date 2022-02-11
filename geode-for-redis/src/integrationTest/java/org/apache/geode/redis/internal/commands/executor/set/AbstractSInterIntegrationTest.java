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
import static redis.clients.jedis.Protocol.Command.SINTER;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSInterIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String SET1 = "{tag1}set1";
  private static final String SET2 = "{tag1}set2";
  private static final String SET3 = "{tag1}set3";

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
  public void sinterErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, SINTER, 1);
  }

  @Test
  public void sinter_withSetsFromDifferentSlots_returnsCrossSlotError() {
    String setKeyDifferentSlot = "{tag2}set2";
    jedis.sadd(SET1, "member1");
    jedis.sadd(setKeyDifferentSlot, "member2");

    assertThatThrownBy(() -> jedis.sendCommand(SET1, SINTER, SET1, setKeyDifferentSlot))
        .hasMessage("CROSSSLOT " + ERROR_WRONG_SLOT);
  }

  @Test
  public void testSInter_givenIntersection_returnsIntersectedMembers() {
    String[] firstSet = new String[] {"peach"};
    String[] secondSet = new String[] {"linux", "apple", "peach"};
    String[] thirdSet = new String[] {"luigi", "apple", "bowser", "peach", "mario"};
    jedis.sadd(SET1, firstSet);
    jedis.sadd(SET2, secondSet);
    jedis.sadd(SET3, thirdSet);

    Set<String> resultSet = jedis.sinter(SET1, SET2, SET3);

    String[] expected = new String[] {"peach"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);
  }

  @Test
  public void testSInter_givenNonSet_returnsErrorWrongType() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String nonSet = "apple";
    jedis.sadd(SET1, firstSet);
    jedis.set("{tag1}nonSet", nonSet);

    assertThatThrownBy(() -> jedis.sinter(SET1, "{tag1}nonSet"))
        .hasMessage("WRONGTYPE " + ERROR_WRONG_TYPE);
  }

  @Test
  public void testSInter_givenNonSetKeyAsFirstKey_returnsWrongTypeError() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "pear", "plum", "peach", "orange",};
    jedis.sadd(SET1, firstSet);
    jedis.sadd(SET2, secondSet);

    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    assertThatThrownBy(() -> jedis.sinter(stringKey, SET1, SET2))
        .hasMessage("WRONGTYPE " + ERROR_WRONG_TYPE);
  }

  @Test
  public void testSInter_givenNonSetKeyAsThirdKey_returnsWrongTypeError() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "pear", "plum", "peach", "orange",};
    jedis.sadd(SET1, firstSet);
    jedis.sadd(SET2, secondSet);

    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    assertThatThrownBy(() -> jedis.sinter(SET1, SET2, stringKey))
        .hasMessage("WRONGTYPE " + ERROR_WRONG_TYPE);
  }

  @Test
  public void testSInter_givenNonSetKeyAsThirdKeyAndNonExistentSetAsFirstKey_returnsWrongTypeError() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd(SET1, firstSet);

    String stringKey = "{tag1}ding";
    jedis.set(stringKey, "dong");

    assertThatThrownBy(() -> jedis.sinter("{tag1}nonExistent", SET1, stringKey))
        .hasMessage("WRONGTYPE " + ERROR_WRONG_TYPE);
  }

  @Test
  public void testSInter_givenNoIntersection_returnsEmptySet() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"ubuntu", "microsoft", "linux", "solaris"};
    jedis.sadd(SET1, firstSet);
    jedis.sadd(SET2, secondSet);

    Set<String> emptyResultSet = jedis.sinter(SET1, SET2);
    assertThat(emptyResultSet).isEmpty();
  }

  @Test
  public void testSInter_givenSingleSet_returnsAllMembers() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd(SET1, firstSet);

    Set<String> resultSet = jedis.sinter(SET1);
    assertThat(resultSet).containsExactlyInAnyOrder(firstSet);
  }

  @Test
  public void testSInter_givenFullyMatchingSet_returnsAllMembers() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "pear", "plum", "peach", "orange",};
    jedis.sadd(SET1, firstSet);
    jedis.sadd(SET2, secondSet);

    Set<String> resultSet = jedis.sinter(SET1, SET2);
    assertThat(resultSet).containsExactlyInAnyOrder(firstSet);
  }

  @Test
  public void testSInter_givenNonExistentSingleSet_returnsEmptySet() {
    Set<String> emptyResultSet = jedis.sinter(SET1);
    assertThat(emptyResultSet).isEmpty();
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String[] values = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] newValues = new String[] {"ubuntu", "orange", "peach", "linux"};
    jedis.sadd(SET1, values);
    jedis.sadd(SET2, values);

    final AtomicReference<Set<String>> sinterResultReference = new AtomicReference<>();
    String[] result = new String[] {"orange", "peach"};
    new ConcurrentLoopingThreads(1000,
        i -> jedis.sadd(SET3, newValues),
        i -> sinterResultReference.set(
            jedis.sinter(SET1, SET2, SET3)))
                .runWithAction(() -> {
                  assertThat(sinterResultReference).satisfiesAnyOf(
                      sInterResult -> assertThat(sInterResult.get()).isEmpty(),
                      sInterResult -> assertThat(sInterResult.get())
                          .containsExactlyInAnyOrder(result));
                  jedis.srem(SET3, newValues);
                });
  }
}
