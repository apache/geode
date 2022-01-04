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
package org.apache.geode.internal.serialization.filter;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Implementation of {@code FilterConfiguration} for configuring the JVM's JMX serialization filter.
 *
 * <p>
 * Configure the “jmx.remote.rmi.server.serial.filter.pattern” system property if Java version is
 * Java 9 or greater. The serial pattern will be configured to accept only standard JMX open-types.
 * If the system property already has a non-null value, then leave it as is.
 *
 * <p>
 * Configure the {@code jdk.serialFilter} system property if Java version is Java 8. The serial
 * pattern will be configured to accept only geode sanctioned serializables and standard JMX
 * open-types. If the system property already has a non-null value, then leave it as is.
 */
class JmxSerialFilterConfiguration implements FilterConfiguration {

  private static final Logger LOGGER = LogService.getLogger();

  private final String key;
  private final String value;
  private final Consumer<String> logger;

  /**
   * Constructs instance for the specified system property and filter pattern.
   */
  JmxSerialFilterConfiguration(String property, String pattern) {
    this(property, pattern, LOGGER::info);
  }

  @VisibleForTesting
  JmxSerialFilterConfiguration(String property, String pattern, Consumer<String> logger) {
    key = property;
    value = pattern;
    this.logger = logger;
  }

  @Override
  public boolean configure() {
    if (isBlank(System.getProperty(key))) {
      System.setProperty(key, value);
      logger.accept("System property '" + key + "' is now configured with '" + value + "'.");
      return true;
    }

    logger.accept("System property '" + key + "' is already configured.");
    return false;
  }
}
