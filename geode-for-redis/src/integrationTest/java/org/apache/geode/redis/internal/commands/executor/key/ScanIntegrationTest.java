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

package org.apache.geode.redis.internal.commands.executor.key;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import org.apache.geode.redis.GeodeRedisServerRule;

public class ScanIntegrationTest extends AbstractScanIntegrationTest {

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Override
  public int getPort() {
    return server.getPort();
  }

  @Test
  public void givenDifferentCursorThanSpecifiedByPreviousScan_returnsAllKeys() {
    List<String> keyList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String key = "{tag1}" + i;
      jedis.set(key, "a");
      keyList.add(key);
    }

    ScanResult<String> result = jedis.scan("0", new ScanParams().count(5).match("{tag1}*"));
    assertThat(result.isCompleteIteration()).isFalse();

    result = jedis.scan("100", new ScanParams().match("{tag1}*"));

    assertThat(result.getResult()).containsExactlyInAnyOrder(keyList.toArray(new String[0]));
  }
}
