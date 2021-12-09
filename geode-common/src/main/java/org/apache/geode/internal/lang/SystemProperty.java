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
package org.apache.geode.internal.lang;

import java.util.Optional;

public class SystemProperty {

  public static final String GEODE_PREFIX = "geode.";
  public static final String GEMFIRE_PREFIX = "gemfire.";
  public static final String DEFAULT_PREFIX = GEODE_PREFIX;

  /**
   * This method will try to look up "geode." and "gemfire." versions of the system property. It
   * will check and prefer "geode." setting first, then try to check "gemfire." setting.
   *
   * @param name system property name set in Geode
   * @return an Optional containing the Boolean value of the system property
   */
  public static Optional<Boolean> getProductBooleanProperty(String name) {
    String property = getProperty(name);
    return property != null ? Optional.of(Boolean.parseBoolean(property)) : Optional.empty();
  }

  /**
   * This method will try to look up "geode." and "gemfire." versions of the system property. It
   * will check and prefer "geode." setting first, then try to check "gemfire." setting.
   *
   * @param name system property name set in Geode
   * @return an Optional containing the Integer value of the system property
   */
  public static Optional<Integer> getProductIntegerProperty(String name) {
    Integer propertyValue = Integer.getInteger(DEFAULT_PREFIX + name);
    if (propertyValue == null) {
      propertyValue = Integer.getInteger(GEMFIRE_PREFIX + name);
    }

    if (propertyValue != null) {
      return Optional.of(propertyValue);
    }
    return Optional.empty();
  }

  /**
   * This method will try to look up "geode." and "gemfire." versions of the system property. It
   * will check and prefer "geode." setting first, then try to check "gemfire." setting.
   *
   * @param name system property name set in Geode
   * @return the integer value of the system property if exits or the default value
   */
  public static Integer getProductIntegerProperty(String name, int defaultValue) {
    return getProductIntegerProperty(name).orElse(defaultValue);
  }

  /**
   * This method will try to look up "geode." and "gemfire." versions of the system property. It
   * will check and prefer "geode." setting first, then try to check "gemfire." setting.
   *
   * @param name system property name set in Geode
   * @return an Optional containing the Long value of the system property
   */
  public static Optional<Long> getProductLongProperty(String name) {
    Long propertyValue = Long.getLong(DEFAULT_PREFIX + name);
    if (propertyValue == null) {
      propertyValue = Long.getLong(GEMFIRE_PREFIX + name);
    }

    if (propertyValue != null) {
      return Optional.of(propertyValue);
    }
    return Optional.empty();
  }

  public static Long getProductLongProperty(String name, long defaultValue) {
    return getProductLongProperty(name).orElse(defaultValue);
  }

  /**
   * This method will try to look up "geode." and "gemfire." versions of the system property. It
   * will check and prefer "geode." setting first, then try to check "gemfire." setting.
   *
   * @param name system property name set in Geode
   * @return an Optional containing the String value of the system property
   */
  public static Optional<String> getProductStringProperty(String name) {
    String property = getProperty(name);
    return property != null ? Optional.of(property) : Optional.empty();
  }

  private static String getProperty(String name) {
    String property = getGeodeProperty(name);
    return property != null ? property : getGemfireProperty(name);
  }

  private static String getGeodeProperty(String name) {
    return System.getProperty(DEFAULT_PREFIX + name);
  }

  private static String getGemfireProperty(String name) {
    return System.getProperty(GEMFIRE_PREFIX + name);
  }

  private SystemProperty() {
    // do not instantiate
  }
}
