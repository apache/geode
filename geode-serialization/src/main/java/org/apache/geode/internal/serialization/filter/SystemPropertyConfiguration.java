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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Configure the “jmx.remote.rmi.server.serial.filter.pattern” system property if Java version is
 * Java 9 or greater. The serial pattern will be configured to accept only standard JMX open-types.
 * If the system property already has a non-null value, then leave it as is.
 *
 * <p>
 * Configure the {@code jdk.serialFilter} system property if Java version is Java 8. The serial
 * pattern will be configured to accept only geode sanctioned serializables and standard JMX
 * open-types. If the system property already has a non-null value, then leave it as is.
 */
class SystemPropertyConfiguration implements FilterConfiguration {

  private static final Logger logger = LogService.getLogger();

  private final String propertyName;
  private final String pattern;
  private final Consumer<String> loggerConsumer;

  SystemPropertyConfiguration(String propertyName, String pattern) {
    this(propertyName, pattern, logger::info);
  }

  @VisibleForTesting
  SystemPropertyConfiguration(String propertyName, String pattern,
      Consumer<String> loggerConsumer) {
    this.propertyName = propertyName;
    this.pattern = pattern;
    this.loggerConsumer = loggerConsumer;
  }

  @Override
  public boolean configure() {
    return new SetSystemProperty(propertyName, pattern, loggerConsumer).execute();
  }

  private static class SetSystemProperty {

    private final String propertyName;
    private final String pattern;
    private final Consumer<String> loggerConsumer;

    private SetSystemProperty(String propertyName, String pattern,
        Consumer<String> loggerConsumer) {
      this.propertyName = propertyName;
      this.pattern = pattern;
      this.loggerConsumer = loggerConsumer;
    }

    public boolean execute() {
      if (isNotBlank(System.getProperty(propertyName))) {
        loggerConsumer.accept("System property " + propertyName + " is already configured.");
        return false;
      }
      System.setProperty(propertyName, pattern);
      loggerConsumer.accept("System property " + propertyName + " is now configured with '"
          + pattern + "'.");
      return true;
    }
  }
}
