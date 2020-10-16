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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.HashSet;
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

  @Test
  public void givenDifferentCursorThanSpecifiedByPreviousHscan_returnsAllEntries() {
    Map<String, String> entryMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      entryMap.put(String.valueOf(i), String.valueOf(i));
    }
    jedis.hmset("a", entryMap);

    ScanParams scanParams = new ScanParams();
    scanParams.count(5);
    ScanResult<Map.Entry<String, String>> result = jedis.hscan("a", "0", scanParams);
    assertThat(result.isCompleteIteration()).isFalse();

    result = jedis.hscan("a", "100");

    assertThat(result.getResult()).hasSize(10);
    assertThat(new HashSet<>(result.getResult())).isEqualTo(entryMap.entrySet());
  }
}
