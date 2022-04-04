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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public abstract class AbstractLTrimIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
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
  public void givenWrongNumOfArgs_returnsError() {
    assertExactNumberOfArgs(jedis, Protocol.Command.LTRIM, 3);
  }

  @Test
  public void withNonListKey_Fails() {
    jedis.set("string", "preexistingValue");
    assertThatThrownBy(() -> jedis.ltrim("string", 0, -1))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void withNonExistentKey_returnsOK() {
    assertThat(jedis.ltrim("nonexistent", 0, -1)).isEqualTo("OK");
  }

  @Test
  public void withNonIntegerRangeSpecifier_Fails() {
    jedis.lpush(KEY, "e1", "e2", "e3", "e4");

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.LTRIM, KEY,
        "0", "not-an-integer"))
            .hasMessage(ERROR_NOT_INTEGER);
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.LTRIM, KEY,
        "not-an-integer", "-1"))
            .hasMessage(ERROR_NOT_INTEGER);
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.LTRIM, KEY,
        "not-an-integer", "not-an-integer"))
            .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  @Parameters(method = "getValidRanges")
  @TestCaseName("{method}: start:{0}, end:{1}, expected:{2}")
  public void trimsToSpecifiedRange_givenValidRange(long start, long end, String[] expected) {
    initializeTestList();

    jedis.ltrim(KEY, start, end);
    assertThat(jedis.lrange(KEY, 0, -1)).containsExactly(expected);
  }

  @SuppressWarnings("unused")
  private Object[] getValidRanges() {
    // Values are start, end, expected result
    // For initial list of {e4, e3, e2, e1}
    return new Object[] {
        new Object[] {0L, 0L, new String[] {"e4"}},
        new Object[] {0L, 1L, new String[] {"e4", "e3"}},
        new Object[] {0L, 2L, new String[] {"e4", "e3", "e2"}},
        new Object[] {1L, 2L, new String[] {"e3", "e2"}},
        new Object[] {1L, -1L, new String[] {"e3", "e2", "e1"}},
        new Object[] {1L, -2L, new String[] {"e3", "e2"}},
        new Object[] {-2L, -1L, new String[] {"e2", "e1"}},
        new Object[] {-1L, -1L, new String[] {"e1"}},
        new Object[] {0L, 3L, new String[] {"e4", "e3", "e2", "e1"}},
        new Object[] {2L, 3L, new String[] {"e2", "e1"}},
        new Object[] {3L, 4L, new String[] {"e1"}},
        new Object[] {0L, 4L, new String[] {"e4", "e3", "e2", "e1"}},
        new Object[] {0L, 10L, new String[] {"e4", "e3", "e2", "e1"}},
        new Object[] {-5L, -1L, new String[] {"e4", "e3", "e2", "e1"}},
        new Object[] {-10L, 10L, new String[] {"e4", "e3", "e2", "e1"}}
    };
  }

  private void initializeTestList() {
    jedis.lpush(KEY, "e1", "e2", "e3", "e4");
  }

  @Test
  @Parameters(method = "getInvalidRanges")
  @TestCaseName("{method}: start:{0}, end:{1}")
  public void removesKey_whenRangeIsEmpty(long start, long end) {
    initializeTestList();

    jedis.ltrim(KEY, start, end);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @SuppressWarnings("unused")
  private Object[] getInvalidRanges() {
    // Values are start, end
    // For initial list of {e4, e3, e2, e1}
    return new Object[] {
        new Object[] {0L, -5L},
        new Object[] {5L, 10L},
        new Object[] {-5L, -10L}
    };
  }

  @Test
  @Parameters(method = "getRangesForOneElementList")
  @TestCaseName("{method}: start:{0}, end:{1}, expected:{2}")
  public void trimsToSpecifiedRange_givenListWithOneElement(long start, long end,
      String[] expected) {
    jedis.lpush(KEY, "e1");

    jedis.ltrim(KEY, start, end);
    assertThat(jedis.lrange(KEY, 0, -1)).containsExactly(expected);
  }

  @SuppressWarnings("unused")
  private Object[] getRangesForOneElementList() {
    // Values are start, end, expected
    return new Object[] {
        new Object[] {0L, 0L, new String[] {"e1"}},
        new Object[] {0L, 1L, new String[] {"e1"}},
        new Object[] {0L, -1L, new String[] {"e1"}},
        new Object[] {-1L, 0L, new String[] {"e1"}},
        new Object[] {-1L, -1L, new String[] {"e1"}},
        new Object[] {1L, 1L, new String[] {}}
    };
  }

  @Test
  public void withConcurrentLPush_returnsCorrectValue() {
    String[] valuesInitial = new String[] {"un", "deux", "trois"};
    String[] valuesToAdd = new String[] {"plum", "peach", "orange"};
    jedis.lpush(KEY, valuesInitial);
    List<String> valuesInitialReversed = new ArrayList<>(Arrays.asList("trois", "deux", "un"));
    List<String> valuesToAddReversed = new ArrayList<>(Arrays.asList("orange", "peach", "plum"));
    final AtomicReference<String> lpopReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.lpush(KEY, valuesToAdd),
        i -> jedis.ltrim(KEY, 0, 2))
            .runWithAction(() -> {
              List<String> result = jedis.lrange(KEY, 0, 2);
              assertThat(result).satisfiesAnyOf(
                  lrangeResult -> assertThat(result).isEqualTo(valuesInitialReversed),
                  lrangeResult -> assertThat(result).isEqualTo(valuesToAddReversed));
              jedis.del(KEY);
              jedis.lpush(KEY, valuesInitial);
            });
  }
}
