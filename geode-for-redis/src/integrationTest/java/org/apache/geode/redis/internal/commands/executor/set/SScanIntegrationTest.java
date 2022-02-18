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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.redis.GeodeRedisServerRule;

public class SScanIntegrationTest extends AbstractSScanIntegrationTest {

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Override
  public int getPort() {
    return server.getPort();
  }

  @Test
  public void givenCursorGreaterThanSignedLongMaxValue_returnsCursorError() {
    assertThatThrownBy(
        () -> jedis.sscan(KEY, SIGNED_LONG_MAX.add(BigInteger.ONE).toString()))
            .hasMessage(ERROR_CURSOR);
  }

  @Test
  public void givenNegativeCursorLessThanSignedLongMinValue_returnsCursorError() {
    assertThatThrownBy(
        () -> jedis.sscan(KEY, SIGNED_LONG_MIN.subtract(BigInteger.ONE).toString()))
            .hasMessage(ERROR_CURSOR);
  }

  @Test
  public void givenCursorEqualToSignedLongMinValue_doesNotError() {
    jedis.sadd(KEY, "1");
    assertThatNoException()
        .isThrownBy(() -> jedis.sscan(KEY, SIGNED_LONG_MAX.toString()));
  }

  @Test
  public void givenNegativeCursorEqualToSignedLongMinValue_doesNotError() {
    jedis.sadd(KEY, "1");
    assertThatNoException()
        .isThrownBy(() -> jedis.sscan(KEY, SIGNED_LONG_MIN.toString()));
  }
}
