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
package org.apache.geode.logging.internal;

import static org.apache.geode.logging.internal.Configuration.STARTUP_CONFIGURATION;
import static org.apache.geode.logging.internal.InternalSessionContext.State.CREATED;
import static org.apache.geode.logging.internal.InternalSessionContext.State.STARTED;
import static org.apache.geode.logging.internal.InternalSessionContext.State.STOPPED;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.util.Optional;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.logging.Banner;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.LogFile;

/**
 * Configures the logging {@code Configuration} and provides lifecycle to Geode logging.
 */
public class LoggingSession implements InternalSessionContext {

  private static final boolean STANDARD_OUTPUT_ALWAYS_ON =
      Boolean.getBoolean(GEMFIRE_PREFIX + "standard-output-always-on");

  private static final Logger logger = LogService.getLogger();

  private final Configuration configuration;
  private final LoggingSessionNotifier loggingSessionNotifier;

  private volatile boolean logBanner;
  private volatile boolean logConfiguration;
  private volatile boolean standardOutputAlwaysOn;

  private State state = STOPPED;

  public static LoggingSession create() {
    return create(Configuration.create(), LoggingSessionRegistryProvider.get());
  }

  @VisibleForTesting
  static LoggingSession create(final Configuration configuration,
      final LoggingSessionNotifier loggingSessionNotifier) {
    return new LoggingSession(configuration, loggingSessionNotifier);
  }

  LoggingSession(final Configuration configuration,
      final LoggingSessionNotifier loggingSessionNotifier) {
    this.configuration = configuration;
    this.loggingSessionNotifier = loggingSessionNotifier;
  }

  public synchronized void createSession(final LogConfigSupplier logConfigSupplier) {
    createSession(logConfigSupplier, true, true);
  }

  public synchronized void createSession(final LogConfigSupplier logConfigSupplier,
      final boolean logBanner, final boolean logConfiguration) {
    createSession(logConfigSupplier, logBanner, logConfiguration, STANDARD_OUTPUT_ALWAYS_ON);
  }

  public synchronized void createSession(final LogConfigSupplier logConfigSupplier,
      final boolean logBanner, final boolean logConfiguration,
      final boolean standardOutputAlwaysOn) {
    configuration.initialize(logConfigSupplier);
    state = state.changeTo(CREATED);
    loggingSessionNotifier.createSession(this);

    this.logBanner = logBanner;
    this.logConfiguration = logConfiguration;
    this.standardOutputAlwaysOn = standardOutputAlwaysOn;
  }

  /**
   * Note: nothing checks Boolean.getBoolean(InternalLocator.INHIBIT_DM_BANNER anymore.
   */
  public synchronized void startSession() {
    state = state.changeTo(STARTED);
    loggingSessionNotifier.startSession();
    if (!standardOutputAlwaysOn) {
      configuration.disableLoggingToStandardOutputIfLoggingToFile();
    }

    if (logBanner) {
      logger.info(new Banner(configuration.getConfigurationInfo()).getString());
    }
    if (logConfiguration) {
      String configInfo = configuration.getLogConfigSupplier().getLogConfig().toLoggerString();
      logger.info(STARTUP_CONFIGURATION + System.lineSeparator() + configInfo);
    }
  }

  public synchronized void stopSession() {
    configuration.enableLoggingToStandardOutput();
    state = state.changeTo(STOPPED);
    loggingSessionNotifier.stopSession();
  }

  public synchronized void shutdown() {
    configuration.shutdown();
  }

  public Optional<LogFile> getLogFile() {
    return loggingSessionNotifier.getLogFile();
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
  LoggingSessionNotifier getLoggingSessionNotifier() {
    return loggingSessionNotifier;
  }
}
