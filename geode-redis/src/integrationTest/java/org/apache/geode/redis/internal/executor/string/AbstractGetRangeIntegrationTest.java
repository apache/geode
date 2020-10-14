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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
  public void givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.GETRANGE))
        .hasMessageContaining("ERR wrong number of arguments for 'getrange' command");
  }

  @Test
  public void givenStartNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.GETRANGE, "key"))
        .hasMessageContaining("ERR wrong number of arguments for 'getrange' command");
  }

  @Test
  public void givenEndNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.GETRANGE, "key", "1"))
        .hasMessageContaining("ERR wrong number of arguments for 'getrange' command");
  }

  @Test
  public void givenMoreThanFourArgumentsProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.GETRANGE, "key", "1", "5", "extraArg"))
            .hasMessageContaining("ERR wrong number of arguments for 'getrange' command");
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
