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
package org.apache.geode.redis.internal.executor.string;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractGetRangeIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.GETRANGE, 3);
  }

  @Test
  public void givenStartIndexIsNotAnInteger_returnsNotIntegerError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.GETRANGE, "key", "NaN", "5"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenEndIndexIsNotAnInteger_returnsNotIntegerError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.GETRANGE, "key", "0", "NaN"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void testGetRange_whenWholeRangeSpecified_returnsEntireValue() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String everything = jedis.getrange(key, 0, -1);
    assertThat(everything).isEqualTo(valueWith19Characters);

    String alsoEverything = jedis.getrange(key, 0, 18);
    assertThat(alsoEverything).isEqualTo(valueWith19Characters);

  }

  @Test
  public void testGetRange_whenMoreThanWholeRangeSpecified_returnsEntireValue() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String fromStartToWayPastEnd = jedis.getrange(key, 0, 5000);
    assertThat(fromStartToWayPastEnd).isEqualTo(valueWith19Characters);

    String wayBeforeStartAndJustToEnd = jedis.getrange(key, -50000, -1);
    assertThat(wayBeforeStartAndJustToEnd).isEqualTo(valueWith19Characters);

    String wayBeforeStartAndWayAfterEnd = jedis.getrange(key, -50000, 5000);
    assertThat(wayBeforeStartAndWayAfterEnd).isEqualTo(valueWith19Characters);
  }

  @Test
  public void testGetRange_whenValidSubrangeSpecified_returnsAppropriateSubstring() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String fromStartToBeforeEnd = jedis.getrange(key, 0, 16);
    assertThat(fromStartToBeforeEnd).isEqualTo("abc123babyyouknow");

    String fromStartByNegativeOffsetToBeforeEnd = jedis.getrange(key, -19, 16);
    assertThat(fromStartByNegativeOffsetToBeforeEnd).isEqualTo("abc123babyyouknow");

    String fromStartToBeforeEndByNegativeOffset = jedis.getrange(key, 0, -3);
    assertThat(fromStartToBeforeEndByNegativeOffset).isEqualTo("abc123babyyouknow");

    String fromAfterStartToBeforeEnd = jedis.getrange(key, 2, 16);
    assertThat(fromAfterStartToBeforeEnd).isEqualTo("c123babyyouknow");

    String fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset = jedis.getrange(key, -16, -2);
    assertThat(fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset)
        .isEqualTo("123babyyouknowm");

    String fromAfterStartToEnd = jedis.getrange(key, 2, 18);
    assertThat(fromAfterStartToEnd).isEqualTo("c123babyyouknowme");

    String fromAfterStartToEndByNegativeOffset = jedis.getrange(key, 2, -1);
    assertThat(fromAfterStartToEndByNegativeOffset).isEqualTo("c123babyyouknowme");
  }

  @Test
  public void testGetRange_whenValidSubrangeSpecified_binaryData_returnsAppropriateSubstring() {
    byte[] keyWith13Chars =
        new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n', 'g', '1'};

    jedis.set(keyWith13Chars, keyWith13Chars);

    byte[] fromStartToBeforeEnd = jedis.getrange(keyWith13Chars, 0, 10);
    assertThat(fromStartToBeforeEnd)
        .isEqualTo(new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n'});

    byte[] fromStartByNegativeOffsetToBeforeEnd = jedis.getrange(keyWith13Chars, -19, 10);
    assertThat(fromStartByNegativeOffsetToBeforeEnd)
        .isEqualTo(new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n'});

    byte[] fromStartToBeforeEndByNegativeOffset = jedis.getrange(keyWith13Chars, 0, -3);
    assertThat(fromStartToBeforeEndByNegativeOffset)
        .isEqualTo(new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n'});

    byte[] fromAfterStartToBeforeEnd = jedis.getrange(keyWith13Chars, 2, 10);
    assertThat(fromAfterStartToBeforeEnd)
        .isEqualTo(new byte[] {0, 4, 0, 5, 's', 't', 'r', 'i', 'n'});

    byte[] fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset =
        jedis.getrange(keyWith13Chars, -10, -2);
    assertThat(fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset)
        .isEqualTo(new byte[] {4, 0, 5, 's', 't', 'r', 'i', 'n', 'g'});

    byte[] fromAfterStartToEnd = jedis.getrange(keyWith13Chars, 2, 13);
    assertThat(fromAfterStartToEnd)
        .isEqualTo(new byte[] {0, 4, 0, 5, 's', 't', 'r', 'i', 'n', 'g', '1'});

    byte[] fromAfterStartToEndByNegativeOffset = jedis.getrange(keyWith13Chars, 2, -1);
    assertThat(fromAfterStartToEndByNegativeOffset)
        .isEqualTo(new byte[] {0, 4, 0, 5, 's', 't', 'r', 'i', 'n', 'g', '1'});
  }

  @Test
  public void testGetRange_whenValidSubrangeSpecified_utf16Data_returnsAppropriateSubstring() {
    String utf16string = "最𐐷𤭢";
    byte[] key = utf16string.getBytes(StandardCharsets.UTF_16);

    jedis.set(key, key);

    byte[] fromStartToBeforeEnd = jedis.getrange(key, 0, 4);
    assertThat(fromStartToBeforeEnd).isEqualTo(new byte[] {-2, -1, 103, 0, -40});

    byte[] fromStartByNegativeOffsetToBeforeEnd = jedis.getrange(key, -19, 4);
    assertThat(fromStartByNegativeOffsetToBeforeEnd).isEqualTo(new byte[] {-2, -1, 103, 0, -40});

    byte[] fromStartToBeforeEndByNegativeOffset = jedis.getrange(key, 0, -2);
    assertThat(fromStartToBeforeEndByNegativeOffset)
        .isEqualTo(new byte[] {-2, -1, 103, 0, -40, 1, -36, 55, -40, 82, -33});

    byte[] fromAfterStartToBeforeEnd = jedis.getrange(key, 2, 4);
    assertThat(fromAfterStartToBeforeEnd).isEqualTo(new byte[] {103, 0, -40});

    byte[] fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset = jedis.getrange(key, -10, -2);
    assertThat(fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset)
        .isEqualTo(new byte[] {103, 0, -40, 1, -36, 55, -40, 82, -33});

    byte[] fromAfterStartToEnd = jedis.getrange(key, 2, 10);
    assertThat(fromAfterStartToEnd).isEqualTo(new byte[] {103, 0, -40, 1, -36, 55, -40, 82, -33});

    byte[] fromAfterStartToEndByNegativeOffset = jedis.getrange(key, 2, -1);
    assertThat(fromAfterStartToEndByNegativeOffset)
        .isEqualTo(new byte[] {103, 0, -40, 1, -36, 55, -40, 82, -33, 98});
  }

  @Test
  public void testGetRange_rangeIsInvalid_returnsEmptyString() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String range1 = jedis.getrange(key, -2, -16);
    assertThat(range1).isEqualTo("");

    String range2 = jedis.getrange(key, 2, 0);
    assertThat(range2).isEqualTo("");
  }

  @Test
  public void testGetRange_nonexistentKey_returnsEmptyString() {
    String key = "nonexistent";

    String range = jedis.getrange(key, 0, -1);
    assertThat(range).isEqualTo("");
  }

  @Test
  public void testGetRange_rangePastEndOfValue_returnsEmptyString() {
    String key = "key";
    String value = "value";

    jedis.set(key, value);

    String range = jedis.getrange(key, 7, 14);
    assertThat(range).isEqualTo("");
  }
}
