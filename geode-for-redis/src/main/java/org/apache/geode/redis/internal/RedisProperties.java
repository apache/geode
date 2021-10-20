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

package org.apache.geode.redis.internal;

import org.apache.commons.lang3.StringUtils;

public class RedisProperties {
  /** System Property Names **/
  public static final String REDIS_REGION_NAME_PROPERTY = "geode-for-redis-region-name";
  public static final String WRITE_TIMEOUT_SECONDS = "geode-for-redis-write-timeout-seconds";
  public static final String EXPIRATION_INTERVAL_SECONDS =
      "geode-for-redis-expiration-interval-seconds";

  public static String getStringSystemProperty(String propName, String defaultValue) {
    String geodeValue = System.getProperty("geode." + propName, defaultValue);
    String gemfireValue = System.getProperty("gemfire." + propName, defaultValue);

    if (StringUtils.isNotBlank(geodeValue) && !geodeValue.equals(defaultValue)) {
      return geodeValue;
    } else if (StringUtils.isNotBlank(geodeValue) && !gemfireValue.equals(defaultValue)) {
      return gemfireValue;
    } else {
      return defaultValue;
    }
  }

  /** assumes that default is greater than or equal to minValue **/
  public static int getIntegerSystemProperty(String propName, int defaultValue, int minValue) {
    int geodeValue = Integer.getInteger("geode." + propName, defaultValue);
    int gemfireValue = Integer.getInteger("gemfire." + propName, defaultValue);

    if (geodeValue != defaultValue && geodeValue >= minValue) {
      return geodeValue;
    } else if (gemfireValue != defaultValue && gemfireValue >= minValue) {
      return gemfireValue;
    } else {
      return defaultValue;
    }
  }
}
