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

import java.util.Optional;

import org.apache.geode.annotations.TestingOnly;

/**
 * Configures the logging {@code Configuration} and provides lifecycle to Geode logging.
 */
public class LoggingSession {

  private final Configuration configuration;
  private final LoggingSessionListeners loggingSessionListeners;
  private State state = State.STOPPED;

  public static LoggingSession create() {
    return create(Configuration.create(), LoggingSessionListeners.get());
  }

  @TestingOnly
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
    configuration.initialize(logConfigSupplier);
    changeStateTo(State.CREATED);
    loggingSessionListeners.createSession(logConfigSupplier);
  }

  public synchronized void startSession() {
    changeStateTo(State.STARTED);
    loggingSessionListeners.startSession();
  }

  public synchronized void stopSession() {
    changeStateTo(State.STOPPED);
    loggingSessionListeners.stopSession();
    // TODO:KIRK: should this invoke shutdown()?
  }

  public synchronized void shutdown() {
    configuration.shutdown();
  }

  public Optional<LogFile> getLogFile() {
    return loggingSessionListeners.getLogFile();
  }

  @TestingOnly
  LoggingSessionListeners getLoggingSessionListeners() {
    return loggingSessionListeners;
  }

  @TestingOnly
  synchronized State getState() {
    return state;
  }

  void changeStateTo(final State newState) {
    state = state.changeTo(newState);
  }

  enum State {
    CREATED,
    STARTED,
    STOPPED;

    State changeTo(final State newState) {
      switch (newState) {
        case CREATED:
          if (this != STOPPED) {
            throw new IllegalStateException("Session must not exist before creating");
          }
          return CREATED;
        case STARTED:
          if (this != CREATED) {
            throw new IllegalStateException("Session must be created before starting");
          }
          return STARTED;
        case STOPPED:
          if (this != STARTED) {
            throw new IllegalStateException("Session must be started before stopping");
          }
      }
      return STOPPED;
    }
  }
}
