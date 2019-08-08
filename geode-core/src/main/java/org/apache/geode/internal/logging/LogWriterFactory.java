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
package org.apache.geode.internal.logging;

import static org.apache.geode.logging.spi.LoggingProvider.MAIN_LOGGER_NAME;
import static org.apache.geode.logging.spi.LoggingProvider.SECURITY_LOGGER_NAME;

import org.apache.geode.logging.internal.log4j.LogWriterLogger;
import org.apache.geode.logging.spi.LogConfig;

/**
 * Factory for creating {@link LogWriterLogger}s.
 */
public class LogWriterFactory {

  /**
   * Creates the log writer for a distributed system based on the system's parsed configuration. The
   * initial banner and messages are also entered into the log by this method.
   *
   * @param logConfig geode configuration for the logger
   * @param secure indicates if the logger is for security related messages
   */
  public static InternalLogWriter createLogWriterLogger(final LogConfig logConfig,
      final boolean secure) {
    String name = secure ? SECURITY_LOGGER_NAME : MAIN_LOGGER_NAME;
    return createLogWriterLogger(name, logConfig.getName(), secure);
  }

  /**
   * Wraps the {@code logWriter} within a {@link SecurityLogWriter}.
   */
  public static InternalLogWriter toSecurityLogWriter(final InternalLogWriter logWriter) {
    return new SecurityLogWriter(logWriter.getLogWriterLevel(), logWriter);
  }

  /**
   * Returns a LogWriterLogger that is decorated with the LogWriter and LogWriterI18n methods.
   *
   * <p>
   * This is the bridge to LogWriter and LogWriterI18n that we need to eventually stop using in
   * phase 1. We will switch over from a shared LogWriterLogger instance to having every GemFire
   * class own its own private static GemFireLogger
   *
   * @return The LogWriterLogger for the calling class.
   */
  public static LogWriterLogger createLogWriterLogger(final String name,
      final String connectionName, final boolean isSecure) {
    return LogWriterLogger.create(name, connectionName, isSecure);
  }
}
