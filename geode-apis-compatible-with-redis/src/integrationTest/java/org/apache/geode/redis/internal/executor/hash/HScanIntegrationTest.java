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
package org.apache.geode.redis.internal.executor.hash;



import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import org.apache.geode.redis.GeodeRedisServerRule;

public class HScanIntegrationTest extends AbstractHScanIntegrationTest {

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Override
  public int getPort() {
    return server.getPort();
  }


  // Note: these tests will not pass native redis, so included here in concrete test class
  @Test
  public void givenCursorGreaterThanIntMaxValue_returnsCursorError() {
    int largestCursorValue = Integer.MAX_VALUE;

    BigInteger tooBigCursor =
        new BigInteger(String.valueOf(largestCursorValue)).add(BigInteger.valueOf(1));

    assertThatThrownBy(() -> jedis.hscan("a", tooBigCursor.toString()))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenCursorLessThanIntMinValue_returnsCursorError() {
    int smallestCursorValue = Integer.MIN_VALUE;

    BigInteger tooSmallCursor =
        new BigInteger(String.valueOf(smallestCursorValue)).subtract(BigInteger.valueOf(1));

    assertThatThrownBy(() -> jedis.hscan("a", tooSmallCursor.toString()))
        .hasMessageContaining(ERROR_CURSOR);
  }


  @Test
  public void givenCount_shouldReturnExpectedNumberOfEntries() {
    Map<byte[], byte[]> entryMap = new HashMap<>();
    entryMap.put("1".getBytes(), "yellow".getBytes());
    entryMap.put("2".getBytes(), "green".getBytes());
    entryMap.put("3".getBytes(), "orange".getBytes());
    jedis.hmset("colors".getBytes(), entryMap);

    int COUNT_PARAM = 2;

    ScanParams scanParams = new ScanParams();
    scanParams.count(COUNT_PARAM);
    ScanResult<Map.Entry<byte[], byte[]>> result;

    String cursor = "0";

    result = jedis.hscan("colors".getBytes(), cursor.getBytes(), scanParams);

    assertThat(result.getResult().size()).isEqualTo(COUNT_PARAM);
  }

}
