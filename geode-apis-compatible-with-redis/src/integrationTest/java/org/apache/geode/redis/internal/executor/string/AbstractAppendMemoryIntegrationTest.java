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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractAppendMemoryIntegrationTest implements RedisIntegrationTest {
  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void testAppend_actuallyIncreasesBucketSize() {
    int listSize = 1000;
    String key = "key";

    Map<String, String> info = getInfo(jedis);
    Long previousMemValue = Long.valueOf(info.get("used_memory"));

    jedis.set(key, "initial");
    for (int i = 0; i < listSize; i++) {
      jedis.append(key, "morestuff");
    }

    info = getInfo(jedis);
    Long finalMemValue = Long.valueOf(info.get("used_memory"));

    assertThat(finalMemValue).isGreaterThan(previousMemValue);
  }

  /**
   * Convert the values returned by the INFO command into a basic param:value map.
   */
  static Map<String, String> getInfo(Jedis jedis) {
    Map<String, String> results = new HashMap<>();
    String rawInfo = jedis.info();

    for (String line : rawInfo.split("\r\n")) {
      int colonIndex = line.indexOf(":");
      if (colonIndex > 0) {
        String key = line.substring(0, colonIndex);
        String value = line.substring(colonIndex + 1);
        results.put(key, value);
      }
    }

    return results;
  }
}
