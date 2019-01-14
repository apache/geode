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

import static org.apache.geode.internal.logging.Configuration.STARTUP_CONFIGURATION;
import static org.apache.geode.internal.logging.SessionContext.State.CREATED;
import static org.apache.geode.internal.logging.SessionContext.State.STARTED;
import static org.apache.geode.internal.logging.SessionContext.State.STOPPED;

import java.util.Optional;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * Configures the logging {@code Configuration} and provides lifecycle to Geode logging.
 */
public class LoggingSession implements SessionContext {

  private static final Logger logger = LogService.getLogger();

  private final Configuration configuration;
  private final LoggingSessionListeners loggingSessionListeners;

  private volatile boolean logBanner;
  private volatile boolean logConfiguration;

  private State state = STOPPED;

  public static LoggingSession create() {
    return create(Configuration.create(), LoggingSessionListeners.get());
  }

  @VisibleForTesting
  static LoggingSession create(final Configuration configuration,
      final LoggingSessionListeners loggingSessionListeners) {
    return new LoggingSession(configuration, loggingSessionListeners);
  }

  LoggingSession(final Configuration configuration,
      final LoggingSessionListeners loggingSessionListeners) {
    this.configuration = configuration;
    this.loggingSessionListeners = loggingSessionListeners;
  }

  public synchronized void createSession(final LogConfigSupplier logConfigSupplier) {
    createSession(logConfigSupplier, true, true);
  }

  public synchronized void createSession(final LogConfigSupplier logConfigSupplier,
      final boolean logBanner, final boolean logConfiguration) {
    configuration.initialize(logConfigSupplier);
    state = state.changeTo(CREATED);
    loggingSessionListeners.createSession(this);

    this.logBanner = logBanner;
    this.logConfiguration = logConfiguration;
  }

  /**
   * Note: nothing checks Boolean.getBoolean(InternalLocator.INHIBIT_DM_BANNER anymore.
   */
  public synchronized void startSession() {
    state = state.changeTo(STARTED);
    loggingSessionListeners.startSession();
    configuration.disableLoggingToStandardOutputIfLoggingToFile();

    if (logBanner) {
      logger.info(new Banner(configuration.getConfigurationInfo()).getString());
    }
    if (logConfiguration) {
      String configInfo = configuration.getLogConfigSupplier().getLogConfig().toLoggerString();
      logger.info(STARTUP_CONFIGURATION + configInfo);
    }
  }

  public synchronized void stopSession() {
    configuration.enableLoggingToStandardOutput();
    state = state.changeTo(STOPPED);
    loggingSessionListeners.stopSession();
  }

  public synchronized void shutdown() {
    configuration.shutdown();
  }

  public Optional<LogFile> getLogFile() {
    return loggingSessionListeners.getLogFile();
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public LogConfigSupplier getLogConfigSupplier() {
    return configuration.getLogConfigSupplier();
  }

  @VisibleForTesting
  LoggingSessionListeners getLoggingSessionListeners() {
    return loggingSessionListeners;
  }
}
