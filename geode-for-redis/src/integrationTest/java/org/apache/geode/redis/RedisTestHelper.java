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
package org.apache.geode.redis;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class RedisTestHelper {
  /**
   * Convert the values returned by the INFO command into a basic param:value map.
   */
  public static Map<String, String> getInfo(Jedis jedis) {
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
