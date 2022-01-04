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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSDiffIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String setKey = "{user1}setkey";
  private static final String nonExistentSetKey = "{user1}nonExistentSet";

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
  public void sdiffErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SDIFF, 1);
  }

  @Test
  public void sdiff_returnsAllValuesInSet() {
    String[] values = createKeyValuesSet();
    assertThat(jedis.sdiff(setKey)).containsExactlyInAnyOrder(values);
  }

  @Test
  public void sdiffWithNonExistentSet_returnsEmptySet() {
    assertThat(jedis.sdiff(nonExistentSetKey)).isEmpty();
  }

  @Test
  public void sdiffWithMultipleNonExistentSet_returnsEmptySet() {
    assertThat(jedis.sdiff("{user1}nonExistentSet1", "{user1}nonExistentSet2")).isEmpty();
  }

  @Test
  public void sdiffWithNonExistentSetAndSet_returnsAllValuesInSet() {
    createKeyValuesSet();
    assertThat(jedis.sdiff(nonExistentSetKey, setKey)).isEmpty();
  }

  @Test
  public void sdiffWithSetAndNonExistentSet_returnsAllValuesInSet() {
    String[] values = createKeyValuesSet();
    assertThat(jedis.sdiff(setKey, nonExistentSetKey))
        .containsExactlyInAnyOrder(values);
  }

  @Test
  public void sdiffWithSetsWithDifferentValues_returnsFirstSetValues() {
    String[] firstValues = createKeyValuesSet();
    String[] secondValues = new String[] {"windows", "microsoft", "linux"};
    jedis.sadd("{user1}setkey2", secondValues);

    assertThat(jedis.sdiff(setKey, "{user1}setkey2")).containsExactlyInAnyOrder(firstValues);
  }

  @Test
  public void sdiffWithSetsWithSomeSharedValues_returnsDiffOfSets() {
    createKeyValuesSet();
    String[] secondValues = new String[] {"apple", "bottoms", "boots", "fur", "peach"};
    jedis.sadd("{user1}setkey2", secondValues);

    Set<String> result =
        jedis.sdiff(setKey, "{user1}setkey2");
    String[] expected = new String[] {"orange", "plum", "pear"};
    assertThat(result).containsExactlyInAnyOrder(expected);
  }

  @Test
  public void sdiffWithSetsWithAllSharedValues_returnsEmptySet() {
    String[] values = createKeyValuesSet();
    jedis.sadd("{user1}setkey2", values);
    assertThat(jedis.sdiff(setKey, "{user1}setkey2")).isEmpty();
  }

  @Test
  public void sdiffWithMultipleSets_returnsDiffOfSets() {
    String[] values = createKeyValuesSet();
    String[] secondValues = new String[] {"apple", "bottoms", "boots", "fur", "peach"};
    String[] thirdValues = new String[] {"queen", "opera", "boho", "orange"};

    jedis.sadd("{user1}setkey2", secondValues);
    jedis.sadd("{user1}setkey3", thirdValues);

    String[] expected = new String[] {"pear", "plum"};
    assertThat(jedis.sdiff(setKey, "{user1}setkey2", "{user1}setkey3"))
        .containsExactlyInAnyOrder(expected);
  }

  @Test
  public void sdiffSetsNotModified_returnSetValues() {
    String[] firstValues = createKeyValuesSet();
    String[] secondValues = new String[] {"apple", "bottoms", "boots", "fur", "peach"};
    jedis.sadd("{user1}setkey2", secondValues);
    jedis.sdiff(setKey, "{user1}setkey2");
    assertThat(jedis.smembers(setKey)).containsExactlyInAnyOrder(firstValues);
    assertThat(jedis.smembers("{user1}setkey2")).containsExactlyInAnyOrder(secondValues);
  }

  @Test
  public void sdiffNonExistentSetsNotModified_returnEmptySet() {
    jedis.sdiff(nonExistentSetKey, "{user1}nonExisistent2");
    assertThat(jedis.smembers(nonExistentSetKey)).isEmpty();
    assertThat(jedis.smembers("{user1}nonExisistent2")).isEmpty();
  }

  @Test
  public void sdiffNonExistentSetAndSetNotModified_returnEmptySetAndSetValues() {
    String[] firstValues = createKeyValuesSet();
    jedis.sdiff(nonExistentSetKey, setKey);
    assertThat(jedis.smembers(nonExistentSetKey).isEmpty());
    assertThat(jedis.smembers(setKey)).containsExactlyInAnyOrder(firstValues);
  }

  @Test
  public void sdiffSetAndNonExistentSetNotModified_returnSetValueAndEmptySet() {
    String[] firstValues = createKeyValuesSet();
    jedis.sdiff(setKey, nonExistentSetKey);
    assertThat(jedis.smembers(setKey)).containsExactlyInAnyOrder(firstValues);
    assertThat(jedis.smembers(nonExistentSetKey).isEmpty());
  }

  @Test
  public void sdiffWithDifferentyKeyType_returnsWrongTypeError() {
    jedis.set("ding", "dong");
    assertThatThrownBy(() -> jedis.sdiff("ding")).hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String[] values = new String[] {"pear", "apple", "plum", "orange", "peach"};
    Set<String> valuesList = new HashSet<>(Arrays.asList(values));

    jedis.sadd("{user1}firstset", values);
    jedis.sadd("{user1}secondset", values);

    final AtomicReference<Set<String>> sdiffResultReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem("{user1}secondset", values),
        i -> sdiffResultReference.set(jedis.sdiff("{user1}firstset", "{user1}secondset")))
            .runWithAction(() -> {
              assertThat(sdiffResultReference).satisfiesAnyOf(
                  sdiffResult -> assertThat(sdiffResult.get()).isEmpty(),
                  sdiffResult -> assertThat(sdiffResult.get())
                      .containsExactlyInAnyOrderElementsOf(valuesList));
              jedis.sadd("{user1}secondset", values);
            });
  }

  private String[] createKeyValuesSet() {
    String[] values = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd("{user1}setkey", values);
    return values;
  }
}
