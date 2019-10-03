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
package org.apache.geode.logging.internal.spi;


import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.ConfigurationProperties;

/**
 * Provides custom configuration of the logging backend for Geode Logging.
 */
public interface LoggingProvider {

  /**
   * The root name of all Geode loggers.
   */
  String GEODE_LOGGER_PREFIX = "org.apache.geode";
  /**
   * The name of the security Geode logger returned by {@link Cache#getSecurityLogger()}.
   */
  String SECURITY_LOGGER_NAME = GEODE_LOGGER_PREFIX + ".security";
  /**
   * The name of the main Geode logger returned by {@link Cache#getLogger()}.
   */
  String MAIN_LOGGER_NAME = GEODE_LOGGER_PREFIX;

  /**
   * Updates the logging backend with any custom configuration. Invoked by Geode during
   * {@code Cache} creation and anytime Geode configuration of logging changes, such as when
   * {@link ConfigurationProperties#LOG_LEVEL} is adjusted.
   */
  void configure(final LogConfig logConfig, final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope);

  /**
   * Removes any custom configuration from the logging backend. Invoked by Geode after closing
   * the {@code Cache}.
   */
  void cleanup();

  /**
   * Returns configuration info to be logged as part of the Geode Logging {@code Banner}. Default
   * implementation returns the class name. Geode out-of-box returns the path to the log4j2.xml
   * configuration file.
   */
  default String getConfigurationInfo() {
    return getClass().getName();
  }

  /**
   * Optional: Invoked by Geode during {@code Cache} creation if
   * {@link ConfigurationProperties#LOG_FILE} is specified.
   *
   * <p>
   * Geode out-of-box disables logging to stdout when {@code Cache} creation starts logging to a
   * file.
   */
  default void disableLoggingToStandardOutput() {
    // override to disable logging to stdout
  }

  /**
   * Optional: Invoked by Geode when closing a {@code Cache} that was logging to a file.
   *
   * <p>
   * Geode out-of-box re-enables logging to stdout after closing a {@code Cache} that was logging
   * to a file.
   */
  default void enableLoggingToStandardOutput() {
    // override to enable logging to stdout
  }

  /**
   * If multiple {@code LoggingProvider}s are loadable then the instance with the highest priority
   * or the first iterable element will be used.
   */
  int getPriority();
}
