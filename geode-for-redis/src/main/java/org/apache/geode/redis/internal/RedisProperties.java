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
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

public class RedisProperties {
  private static final Logger logger = LogService.getLogger();

  private static final String PREFIX = "geode.geode-for-redis-";
  public static final String REDIS_REGION_NAME_PROPERTY = PREFIX + "region-name";
  public static final String WRITE_TIMEOUT_SECONDS = PREFIX + "write-timeout-seconds";
  public static final String EXPIRATION_INTERVAL_SECONDS = PREFIX + "expiration-interval-seconds";
  public static final String REGION_BUCKETS = PREFIX + "region-buckets";
  public static final String UNAUTHENTICATED_MAX_ARRAY_SIZE =
      PREFIX + "unauthenticated-max-array-size";
  public static final String UNAUTHENTICATED_MAX_BULK_STRING_LENGTH =
      PREFIX + "unauthenticated-max-bulk-string-length";


  private static String convertToGemfire(String geodeName) {
    return "gemfire." + geodeName.substring("geode.".length());
  }

  private static void validatePropertyName(String geodeName) {
    if (!geodeName.startsWith(PREFIX)) {
      throw new IllegalStateException("Property names must start with \"" + PREFIX + "\"");
    }
  }

  public static String getStringSystemProperty(String propName, String defaultValue) {
    validatePropertyName(propName);

    String geodeValue = System.getProperty(propName);
    if (StringUtils.isNotEmpty(geodeValue)) {
      return geodeValue;
    }

    String gemfireValue = System.getProperty(convertToGemfire(propName));
    if (StringUtils.isNotEmpty(gemfireValue)) {
      return gemfireValue;
    }

    return defaultValue;
  }

  public static int getIntegerSystemProperty(String propName, int defaultValue, int minValue) {
    return getIntegerSystemProperty(propName, defaultValue, minValue, Integer.MAX_VALUE);
  }

  public static int getIntegerSystemProperty(String propName, int defaultValue, int minValue,
      int maxValue) {
    validatePropertyName(propName);

    int geodeValue = getIntegerFromProperty(propName, defaultValue, minValue, maxValue);
    if (geodeValue != defaultValue) {
      return geodeValue;
    }

    return getIntegerFromProperty(convertToGemfire(propName), defaultValue, minValue, maxValue);
  }

  private static int getIntegerFromProperty(String propName, int defaultValue, int minValue,
      int maxValue) {
    String stringValue = System.getProperty(propName);
    if (stringValue == null) {
      return defaultValue;
    }
    int intValue;
    try {
      intValue = Integer.decode(stringValue);
    } catch (NumberFormatException e) {
      logger.warn("Ignoring system property \"" + propName + "\" because it's value of \""
          + stringValue + "\" is not a valid integer. Defaulting to " + defaultValue);
      return defaultValue;
    }
    if (intValue < minValue) {
      logger.warn("Ignoring system property \"" + propName + "\" because it's value of \""
          + intValue + "\" is less than \"" + minValue + "\". Defaulting to " + defaultValue);
      return defaultValue;
    }
    if (intValue > maxValue) {
      logger.warn("Ignoring system property \"" + propName + "\" because it's value of \""
          + intValue + "\" is greater than \"" + maxValue + "\". Defaulting to " + defaultValue);
      return defaultValue;
    }
    return intValue;
  }
}
